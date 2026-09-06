package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	//作用：tinykv需要对多行数据进行处理，所以需要server.Latches对keys进行加锁管理
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
// 作用：快照读取
// 使用req.version，即读取快照时间。读取在req.version时能看见的值
// 逻辑：先创建reader和mvcc事务，然后getlock并检查Lock的可见性，然后读取快照值，最后组织一下响应
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	//1.创建reader和mvcc事务
	reader, err := server.storage.Reader(req.Context)
	//如果出现region错误，通过resp返回
	if regionErr, ok := err.(*raft_storage.RegionError); ok { //类型断言语法
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	//2.getlock并检查lock的可见性
	lock, err := txn.GetLock(req.Key)
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	if lock != nil && req.Version >= lock.Ts { //当前事务要读取的快照可能受到一个尚未提交事务的影响
		resp.Error = &kvrpcpb.KeyError{ //其他错误通过error返回
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
	}
	//3.读取快照值并组织响应
	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
	}
	resp.Value = value
	return resp, nil
}

// 作用：2PC第一阶段，检查冲突，写入value，加锁。其修改了default,lock的CF，不修改write的CF
// 逻辑：先对所有mutation key获取latch，创建mvcctxn（该txn收集整个prewrite命令产生的修改），对每个mutation检查write confilct，检查lock confilct，没有冲突时写入value和lock，最后原子写入
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	//1.对所有mutation key获取latch ———————— 【先获取latch再创建reader，否则reader可能是等待latch前的旧快照】
	keys := make([][]byte, 0, len(req.Mutations))
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys) //一次性获取latch【锁住mutation key】
	defer server.Latches.ReleaseLatches(keys)
	reader, err := server.storage.Reader(req.Context) //创建获得latch后的存储快照reader
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	//2.创建mvcctxn
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	//3.对每个mutation检查write confilct，检查lock confilct，没有冲突时写入value和lock
	for _, mutation := range req.Mutations {
		//3.1检查write confilct
		recentWrite, commitTS, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if recentWrite != nil && commitTS >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitTS,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			return resp, nil
		}
		//3.2检查lock confilct
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock != nil { // 任意一个 key 冲突，整个 Prewrite 都不写入。
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(mutation.Key),
			})
			return resp, nil
		}
		//3.3没有冲突时写入value和lock
		newLock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl, //这把lock最长可以存活多久，如果超时则回滚/清理
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		}
		txn.PutValue(mutation.Key, mutation.Value)
		txn.PutLock(mutation.Key, newLock)
	}
	//4.最后原子写入
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

// 作用：第二阶段，写入提交记录、解锁。即建立commitTS -> startTS,使得值成为已提交、可见的版本
// 逻辑：先对所有key获取latch，再创建mvccTxn，再调用currentwrite查找wr.ts==req.startversion的记录，查找lock判断lock.startts和txn的是否一致(不一致说明prewrite太长，TTL超时，被回滚了)，最后原子写入write
// commit不修改default(Prewrite 写入的值继续保留)，修改lock和write
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	//1.对所有key获取latch后获得reader【锁住req.keys】
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	//2.再创建mvccTxn
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	//3.再调用currentwrite查找wr.ts==req.startversion的记录，，查找lock判断lock.startts和txn的是否一致
	for _, key := range req.Keys {
		//3.1调用currentwrite查找对应的write
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		//3.2【幂等处理】对于当前ts的事务，如果找到了非rollback的write记录，则说明之前已经commit成功了，本次请求是重复请求
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				// 已经 rollback，不能再 commit
				resp.Error = &kvrpcpb.KeyError{
					Abort: "transaction already rolled back",
				}
				return resp, nil
			}
			// 已经成功 commit 过，重复 Commit
			continue
		}
		// if write.Kind != mvcc.WriteKindRollback && write.StartTS == req.StartVersion {
		// 	return resp, nil
		// }

		//3.3查找lock判断lock.startts和txn的是否一致
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		//不一致说明prewrite太长，TTL超时，被回滚了;不存在要么是已经提交了，要么是被别的事务清除了
		//【4b错误修改】lock为空和lock存在但ts不相等，这两种情况的处理方法不同
		if lock == nil {
			// 情况一：没有write，没有lock，则没有 prewrite，无需 commit。使用continue跳过这个key
			continue
		}
		if lock.Ts != req.StartVersion {
			// 情况二：被另一个事务锁住
			resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
			return resp, nil
		}
		//3.4第一次提交事务（写入新write并删掉lock）
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	//4.最后原子写入
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

// 作用：从 req.StartKey 开始，按照用户键升序，在 req.Version 快照中最多返回 req.Limit 个逻辑结果
// startkey - 起始用户键，version - 快照事件，limit - 最大返回数量
// 逻辑：创建reader和mvcctxn，创建scanner，循环调用scanner.next，组织结果
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	//1. 创建reader和mvcctxn
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer reader.Close()
	defer scanner.Close()
	var pairs []*kvrpcpb.KvPair // 存储扫描到的 {key, value}
	//2. 循环调用scanner.next
	for i := 0; i < int(req.Limit); i++ {
		key, value, err := scanner.Next()
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			//2.1 这里会返回 keyerror
			return nil, err
		}
		//2.2 key为空表示没数据了
		if key == nil {
			break
		}
		//2.3 value为空则说明被lock了，不为空则组织结果
		if value != nil {
			pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
		}
	}
	//3. 组织结果并返回
	resp.Pairs = pairs
	return resp, nil
}

// 作用：客户端通过事务的 primary key 和 startTS 查询事务当前状态，并在锁超时时回滚 primary key
// primarykey —— 事务primary key；lockTs —— 被查询事务的ts；currentTs —— 客户端提供的当前时间
// 逻辑：latch primary key，检查currentwrite（一旦找到currentwrite，就是事务最终状态），没有时则检查lock
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}

	// 1. 该命令可能回滚 primary key，因此必须先加 latch，再创建 Reader。
	keys := [][]byte{req.PrimaryKey}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	// 2. 检查CurrentWrite，查找是否存在当前事务的write。Write 是事务的最终状态，优先级高于可能残留的 Lock。
	write, commitTS, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if write != nil {
		//2.1 如果是put/delete，返回commitversion
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = commitTS
		}
		//2.2 rollback则version返回0
		return resp, nil
	}

	// 3. 没有最终状态时，检查 primary lock 是否属于目标事务。
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock == nil || lock.Ts != req.LockTs {
		// 没有lock，或者lock的ts和请求的ts不匹配时（锁是其他事务的），留下 Rollback 标记，阻止当前ts的指代的事务其迟到的 Commit。
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	} else if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		// TTL 已到期：删除预写锁和值，并记录 Rollback。
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	} else {
		// Lock 仍然有效，不修改存储，只把 TTL 返回给客户端。
		resp.LockTtl = lock.Ttl
		return resp, nil
	}

	// 4. 上述两个回滚分支统一原子写入。
	server.Latches.Validate(txn, keys)
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

// 作用：“回滚” 指定 startTS “事务”在一批 key 上的 Prewrite 状态
// req.startVersion 要回滚事务的startTs；req.keys 要回滚的key
// 逻辑：latch 所有key，对每个key先检查currentwrite，没有currentwrite则检查lock，写下rollback marker，最后一次性提交
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	//1. latch 所有key
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	//2. 创建reader和mvccTxn
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range req.Keys {
		//3. 对每个key先检查currentwrite
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				//return resp, nil
				continue
			}else{
			resp.Error = &kvrpcpb.KeyError{Abort: "transaction already committed",}
			return resp, nil
			}
		}
		//4. 没有currentwrite则检查lock，写下rollback marker
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		//4.1 如果lock不存在或者lock的ts不等于req.startversion，则说明该事务已经被其他事务清理了，直接写下rollback marker
		if lock == nil || lock.Ts != req.StartVersion {
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}
		//4.2 如果lock存在且ts等于req.startversion，则说明该事务还在进行中，删除lock和value，并写下rollback marker
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	//5. 最后一次性提交
	server.Latches.Validate(txn, req.Keys)
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

// 作用：解决锁冲突问题。当客户端已经确认某个事务最终应该 Commit 或 Rollback 后，要求 TinyKV 找出当前 Region 内属于该事务的所有锁，并统一处理
// commitversion >0表示该事务最终应该 Commit，否则应该 Rollback
// 逻辑：查找这个事务的全部 lock ， 提取所有用户key，根据commitversion处理。
// 辅助函数：kvcommit和kvbatchrollback
func (server *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	//1. 不加 latch，只扫描 lock CF，发现 keys
	reader,err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	locks,err := mvcc.AllLocksForTxn(txn)
	reader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	keys := make([][]byte, 0, len(locks))
	for _, pair := range locks {
	// 复制 key，避免它继续依赖底层迭代器的内存。
		key := append([]byte(nil), pair.Key...)
		keys = append(keys, key)
	}
	// 当前 Region 没有该事务的遗留锁，直接成功。
	if len(keys) == 0 {
		return resp, nil
	}
	//2.对发现的keys由kvcommit和kvbatchrollback持有latch，避免死锁
	if req.CommitVersion > 0 {
		commitResp, err := server.KvCommit(ctx, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			CommitVersion: req.CommitVersion,
			Keys:          keys,
		})
		if err != nil {
			return nil, err
		}

		// 3. 把 KvCommit 的响应转换成 ResolveLockResponse。
		resp.RegionError = commitResp.RegionError
		resp.Error = commitResp.Error
		return resp, nil
	}
	//2.2 commitversion ==0表示该事务最终应该 Rollback
	rollbackResp, err := server.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
		Context:      req.Context,
		StartVersion: req.StartVersion,
		Keys:         keys,
	})
	if err != nil {
		return nil, err
	}

	//3. 把 KvBatchRollback 的响应转换成 ResolveLockResponse。
	resp.RegionError = rollbackResp.RegionError
	resp.Error = rollbackResp.Error
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
