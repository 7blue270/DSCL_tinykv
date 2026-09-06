package mvcc

import (
	"bytes"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner 用于从存储层连续读取多个键值对，并把多条物理 MVCC 记录
// 折叠成用户可见的逻辑记录。
// 不变式：Scanner 要么已经结束，要么已经准备好返回下一个结果。
type Scanner struct {
	txn      *MvccTxn
	iter     engine_util.DBIterator
	nextKey  []byte
	finished bool
}

// NewScanner 创建一个基于 txn 快照的扫描器，并从 startKey 开始扫描。
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	return &Scanner{
		nextKey:  startKey,
		txn:      txn,
		iter:     txn.Reader.IterCF(engine_util.CfWrite),
		finished: false,
	}
}

// Close 关闭 Scanner 持有的底层迭代器。
func (scan *Scanner) Close() {
	scan.iter.Close()
}

// advancePastKey 跳过 userKey 的所有剩余版本，并记录下一个逻辑用户键。
// 如果迭代器已经耗尽，则将 Scanner 标记为扫描结束。
// 理由：假设已经为 a 找到正确值并返回，但返回前没有跳过 a 的其他版本，下一次调用 Next() 时就可能再次返回 a，导致同一个用户键出现多次
// 1.合并多个版本
func (scan *Scanner) advancePastKey(userKey []byte) {
	for scan.iter.Valid() {
		key := DecodeUserKey(scan.iter.Item().KeyCopy(nil))
		if !bytes.Equal(key, userKey) {
			scan.nextKey = key
			return
		}
		scan.iter.Next()
	}
	scan.finished = true
	scan.nextKey = nil
}

// Next 返回下一个逻辑用户键值对，而不是下一条物理 write 记录。
// Scanner 耗尽时返回 nil, nil, nil。
func (scan *Scanner) Next() ([]byte, []byte, error) {
	if scan.finished {
		return nil, nil, nil
	}

	for !scan.finished {
		// 定位 nextKey 在当前事务 StartTS 时可见的最新版本。
		key := append([]byte(nil), scan.nextKey...) //做了一份独立拷贝

		//2.只选择当前快照可见的版本。
		scan.iter.Seek(EncodeKey(key, scan.txn.StartTS))
		if !scan.iter.Valid() { //已经走到最后的位置了
			scan.finished = true
			scan.nextKey = nil
			return nil, nil, nil
		}

		gotKey := scan.iter.Item().KeyCopy(nil)
		userKey := DecodeUserKey(gotKey)
		if !bytes.Equal(userKey, key) {
			// Seek 可能落到下一个用户键的最新版本，而该版本不一定对当前事务可见，因此需要用新的用户键和 StartTS 再次定位。
			//a 只有一个版本 a@120，但它晚于快照 100，不可见，可能会直接跳到b@150，所以先记录nextkey
			scan.nextKey = userKey
			continue
		}

		// 3.检查lock。创建时间不晚于当前快照的锁会阻塞该逻辑键。
		// 同时保留 Lock.IsLockedFor 对 TsMax 的特殊处理语义。
		lock, err := scan.txn.GetLock(key)
		if err != nil {
			return nil, nil, err
		}
		locked := lock != nil && lock.Ts <= scan.txn.StartTS
		if locked && scan.txn.StartTS == TsMax && !bytes.Equal(key, lock.Primary) {
			locked = false
		}
		if locked {
			scan.advancePastKey(key)
			keyErr := &KeyError{}
			keyErr.Locked = lock.Info(key)
			return key, nil, keyErr
		}

		// 从第一个可见版本开始，跳过 Rollback，直到找到 Put 或 Delete。
		// 这里遍历的记录都属于同一个用户键。
		for scan.iter.Valid() {
			item := scan.iter.Item()
			encodedKey := item.KeyCopy(nil)
			itemUserKey := DecodeUserKey(encodedKey)
			//同样的处理（同上）
			if !bytes.Equal(itemUserKey, key) {
				scan.nextKey = itemUserKey
				break
			}

			encodedWrite, err := item.ValueCopy(nil) //key-value中取value
			if err != nil {
				return nil, nil, err
			}
			write, err := ParseWrite(encodedWrite)
			if err != nil {
				return nil, nil, err
			}

			//4.正确处理writekind
			switch write.Kind {
			case WriteKindPut:
				value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
				if err != nil {
					return nil, nil, err
				}
				scan.advancePastKey(key)
				return key, value, nil
			case WriteKindDelete:
				// 已删除的键不占用扫描结果名额。继续去处理下一个要处理的键next key
				scan.advancePastKey(key)
			case WriteKindRollback:
				// Rollback 不改变逻辑值，应该移动到下一条物理记录，继续检查该用户键的更早版本。
				scan.iter.Next()
				continue
			default:
				return nil, nil, fmt.Errorf("mvcc scanner: unknown write kind %d", write.Kind)
			}
			break
		}
	}
	return nil, nil, nil
}
