package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"sync"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.

// 【这段代码是单机版storage的实现】：不与其他节点通信，所有数据都存储在本地的单节点 TinyKV 实例的 `Storage` 实现。

type StandAloneStorage struct {
	// Your Data Here (1).
	conf    *config.Config // 配置对象，包含了存储引擎的路径等配置信息
	db      *badger.DB     // badger数据库实例，提供了键值存储的功能
	mu      sync.RWMutex   // 读写锁，保护对数据库的访问，确保线程安全【Go语言中的并发控制机制】
	started bool           // 标志位，表示存储是否已经启动，防止重复启动或停止
}

// 构造函数，接受一个配置对象，并返回一个新的 `StandAloneStorage` 实例。
// 这里只做对象初始化，真正的数据库连接在 Start 方法中建立
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.mu.Lock()
	defer s.mu.Unlock() //作用：在函数退出时释放锁，无论是正常退出还是由于错误导致的退出，都能确保锁被正确释放，避免死锁问题。

	if s.started {
		return nil // 已经启动，无需重复启动
	}

	s.db = engine_util.CreateDB(s.conf.DBPath, false) // 创建 badger 数据库实例，路径来自配置
	s.started = true                                  // 标记为已启动
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil // 已经停止，无需重复停止
	}

	var err error
	if s.db != nil {
		err = s.db.Close() // 关闭数据库连接
		s.db = nil         // 释放数据库实例
	}

	s.started = false
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started || s.db == nil {
		return nil, errors.New("storage is not started or already closed") // 存储未启动，无法创建 Reader，返回 nil
	}

	txn := s.db.NewTransaction(false)       // 创建一个只读事务，参数 false 表示这是一个只读事务，不会修改数据库
	return &standAloneReader{txn: txn}, nil // 返回一个新的 standAloneReader 实例，包含创建的事务
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started || s.db == nil {
		return errors.New("storage is not started or already closed") // 存储未启动，无法执行写操作，返回错误
	}

	wb := new(engine_util.WriteBatch) // 通过engine_util创建一个新的 WriteBatch 实例，用于批量写入操作
	for _, modify := range batch {
		switch data := modify.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value) // 将 Put 操作添加到 WriteBatch 中，指定列族、键和值
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key) // 将 Delete 操作添加到 WriteBatch 中，指定列族和键
		default:
			return errors.New("unsupported modify type")
		}
	}
	return wb.WriteToDB(s.db) // 将 WriteBatch 中的操作写入数据库，返回可能的错误
}

// ==========创建一个 standAloneReader 结构体，包含一个 badger.Txn 字段，用于执行数据库操作=============
type standAloneReader struct {
	txn *badger.Txn
}

func (r *standAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	// Your Code Here (1).
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil // 键不存在，返回 nil 值和 nil 错误
		}
		return nil, err
	}
	return val, nil
}

func (r *standAloneReader) IterCF(cf string) engine_util.DBIterator {
	// Your Code Here (1).
	return engine_util.NewCFIterator(cf, r.txn) // 创建并返回一个新的 CF 迭代器，使用当前事务和指定的列族
}

func (r *standAloneReader) Close() {
	// Your Code Here (1).
	if r.txn != nil {
		r.txn.Discard() // 释放事务资源，确保不再使用该事务
		r.txn = nil     // 将事务引用置空，防止后续误用
	}
}

//【注释】
//1.这个单机节点底层是badger，但其不提供CF功能，所以通过engine_util来添加前缀，具体需要根据操作类型来决定CF的值
//2.这个tinykv中的CF其实只有 default,write,lock三种
