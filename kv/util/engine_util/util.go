package engine_util

import (
	"bytes"

	"github.com/Connor1996/badger"
	"github.com/golang/protobuf/proto"
)

// 把列族和key组合成一个新的key，格式为"cf_key"，其中cf是列族的名称，key是原始的key
func KeyWithCF(cf string, key []byte) []byte {
	return append([]byte(cf+"_"), key...)
}

// 从指定的CF中获取key对应的value
func GetCF(db *badger.DB, cf string, key []byte) (val []byte, err error) {
	err = db.View(func(txn *badger.Txn) error {
		val, err = GetCFFromTxn(txn, cf, key)
		return err
	})
	return
}

// 在已有的事务中从指定的CF中获取key对应的value
func GetCFFromTxn(txn *badger.Txn, cf string, key []byte) (val []byte, err error) {
	item, err := txn.Get(KeyWithCF(cf, key))
	if err != nil {
		return nil, err
	}
	val, err = item.ValueCopy(val)
	return
}

// 在指定的CF中设置key对应的value
func PutCF(engine *badger.DB, cf string, key []byte, val []byte) error {
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Set(KeyWithCF(cf, key), val)
	})
}

// 从指定的key中获取元数据，并将其反序列化到msg中，msg必须是一个实现了proto.Message接口的结构体
func GetMeta(engine *badger.DB, key []byte, msg proto.Message) error {
	var val []byte
	err := engine.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.Value()
		return err
	})
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

// 在已有的事务中从指定的key中获取元数据，并将其反序列化到msg中，msg必须是一个实现了proto.Message接口的结构体
// 如果需要在一个事务里获取多个元数据，可以调用这个函数多次，避免重复创建事务，提高效率
func GetMetaFromTxn(txn *badger.Txn, key []byte, msg proto.Message) error {
	item, err := txn.Get(key)
	if err != nil {
		return err
	}
	val, err := item.Value()
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

// 把一个 protobuf 消息序列化后写入到 Badger，key 不带 CF 前缀
func PutMeta(engine *badger.DB, key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

// 删除指定 CF 中的 key 对应的 KV 对
func DeleteCF(engine *badger.DB, cf string, key []byte) error {
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Delete(KeyWithCF(cf, key))
	})
}

// 在所有CF中删除一个范围内的KEY
func DeleteRange(db *badger.DB, startKey, endKey []byte) error {
	batch := new(WriteBatch)
	txn := db.NewTransaction(false)
	defer txn.Discard()
	for _, cf := range CFs {
		deleteRangeCF(txn, batch, cf, startKey, endKey)
	}

	return batch.WriteToDB(db)
}

// 只删除一个CF中一个范围内的KEY
func deleteRangeCF(txn *badger.Txn, batch *WriteBatch, cf string, startKey, endKey []byte) {
	it := NewCFIterator(cf, txn)
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if ExceedEndKey(key, endKey) {
			break
		}
		batch.DeleteCF(cf, key)
	}
	defer it.Close()
}

// 判断当前key是否超过了endKey，如果endKey为空，则永远不会超过
func ExceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}
