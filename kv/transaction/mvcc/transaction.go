package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
// 作用：向当前mvcctxn的修改列表中添加一条write CF写入。其中ts为commit_ts
// CF:write  Key:encodekey(userkey,ts)  Value:write.ToBytes()
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, ts),
			Cf:    engine_util.CfWrite,
			Value: write.ToBytes(),
		},
	}
	txn.writes = append(txn.writes, modify)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
// 作用：查询某个用户键是否存在锁，只负责读锁，不负责判断这把锁是否和当前事务冲突
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	//反序列化lock结构体
	lock, err := ParseLock(value)
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
// 作用：向当前修改列表加入一条lock CF写入
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key:   key,
			Cf:    engine_util.CfLock,
			Value: lock.ToBytes(),
		},
	}
	txn.writes = append(txn.writes, modify)
}

// DeleteLock adds a delete lock to this transaction.
// 作用：向修改列表加入一条删除 lock CF 数据的操作
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Delete{
			Cf:  engine_util.CfLock,
			Key: key,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
// 作用：读取用户键在当前事务 StartTS 时刻可见的值，实现 snapshot isolation 的快照读取
// —— 当前事务能看到的最新数据
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	//1.遍历 Write 通过 iter.Seek() 查找遍历 Write，找到 commitTs <= ts 最新 Write
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	//寻找到的总是最新版本的key
	iter.Seek(EncodeKey(key, txn.StartTS))
	if !iter.Valid() {
		return nil, nil
	}
	// 2. 判断找到的 key 是不是自己需要的 key
	userKey := DecodeUserKey(iter.Item().KeyCopy(nil))
	if !bytes.Equal(userKey, key) {
		return nil, nil
	}
	//3. 判断 Write 的 Kind 是不是 WriteKindPut
	value, err := iter.Item().ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	write, err := ParseWrite(value)
	if err != nil {
		return nil, err
	}
	// 4. 从 Default 中获取值
	if write.Kind == WriteKindPut {
		return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
// 作用：把当前事务预写的真实用户值加入 default CF 修改列表
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, txn.StartTS),
			Cf:    engine_util.CfDefault,
			Value: value,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// DeleteValue removes a key/value pair in this transaction.
// 作用：删除当前事务在default CF中预写的值
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Delete{
			Cf:  engine_util.CfDefault,
			Key: EncodeKey(key, txn.StartTS),
		},
	}
	txn.writes = append(txn.writes, modify)
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// 作用：查找这个 key 是否已经存在属于当前事务的 write 记录
// —— 用于评估这个 key 有没有被当前这个事务提交/回滚过
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	//1.通过 iter.Seek(EncodeKey(key, math.MaxUint64)) 查询该 key 的最新 Write
	for iter.Seek(EncodeKey(key, ^uint64(0))); iter.Valid(); iter.Next() {
		item := iter.Item()
		gotKey := item.KeyCopy(nil)
		userKey := DecodeUserKey(gotKey)
		if !bytes.Equal(userKey, key) {
			return nil, 0, nil
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}
		//2. 如果 write.StartTS > txn.StartTS，继续遍历，直到找到 write.StartTS == txn.StartTS 的 Write
		if write.StartTS == txn.StartTS {
			//3. 返回这个 Write 和 commitTs；
			return write, decodeTimestamp(gotKey), nil
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// 作用：查找某个用户键最新的一条 write 记录，并返回它的 commitTS
// ——获取该 key 最新的 write，用于冲突检查
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	// 1. 通过 iter.Seek(EncodeKey(key, math.MaxUint64)) 查找；
	iter.Seek(EncodeKey(key, ^uint64(0)))
	if !iter.Valid() {
		return nil, 0, nil
	}
	userKey := DecodeUserKey(iter.Item().KeyCopy(nil))
	if !bytes.Equal(userKey, key) {
		return nil, 0, nil
	}
	value, err := iter.Item().ValueCopy(nil)
	if err != nil {
		return nil, 0, err
	}
	//2. 判断目标 Write 的 key 是不是我们需要的，不是返回空；即只要读到了就存在
	write, err := ParseWrite(value)
	if err != nil {
		return nil, 0, err
	}
	// 是直接返回该 Write；
	return write, decodeTimestamp(iter.Item().KeyCopy(nil)), nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts) //”^ts“：这里把 uint64 时间戳按位取反，再用大端序 binary.BigEndian，整数大小关系就和字节的字典序一致
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
