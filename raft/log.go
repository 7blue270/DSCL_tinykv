// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated

type RaftLog struct {
	// storage contains all stable entries since the last snapshot.  稳定存储的数据
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.                          已提交，即这条日志已经被大多数节点存储了
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed                              应用到状态机的最高日志位置
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64 //已持久化到本地磁盘中

	// all entries that have not yet compact.                       内存缓冲区， snapshot之后所有的数据都在entries里面，其包含了持久化和未持久化的数据
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
	//【commit和apply的关系】：commit是日志被大多数节点存储了，apply是日志被应用到状态机了，commit和apply之间的日志条目可能还没有被应用到状态机中

	// Your Data Here (2A).
}

// 使用entries[0]作为一个dummy entry，存放被压缩掉的最后一条日志
// 辅助函数：将逻辑日志 Index 转换为 entries 数组的下标 Offset
func (l *RaftLog) toEntryIndex(i uint64) int {
	if len(l.entries) == 0 {
		return 0
	}
	// 数组下标 = 目标Index - Dummy的Index
	return int(i - l.entries[0].Index)
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
// 初始化和元数据
// 【注意】：新建raftlog时，需要从上层storage 获取所有的未被持久化的 entries
// 【注意】：注意，entries中的第0个位置用于存放被压缩掉的最后一条日志，即dummy entry
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	dummyIndex := firstIndex - 1
	dummyTerm, err := storage.Term(dummyIndex)
	if err != nil {
		panic(err)
	}
	ents := make([]pb.Entry, 1, 1+len(entries))
	ents[0] = pb.Entry{
		Index: dummyIndex,
		Term:  dummyTerm,
	}
	ents = append(ents, entries...)
	// 初始化 RaftLog 的字段
	return &RaftLog{
		storage:   storage,
		committed: dummyIndex, // 初始化时，committed 和 applied 都指向第一个日志条目的前一个位置
		applied:   dummyIndex,
		stabled:   lastIndex, // 已经持久化的日志条目索引
		entries:   ents,      // 从 storage 获取的未被持久化的日志条目
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
// 【2C】当底层 Storage 发生日志压缩（compact）后，RaftLog 要同步“裁剪内存里的 entries 缓存”，避免读到已被 compact 的旧日志，并节省内存。
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	//1. 拿到compact之后的第一个日志条目的index，即底层持久化存储的第一个日志条目的index
	fi, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	//2.没有内存，直接返回
	if len(l.entries) == 0 {
		return
	}
	//3.当前dummy index
	dummyIndex := l.entries[0].Index
	//一版情况下，dummy index应该等于fi-1,如果满足，提前返回
	if fi <= dummyIndex+1 {
		return
	}
	//4.计算丢弃到哪里
	cut := l.toEntryIndex(fi - 1)
	// cut 越界意味着：storage 的 fi 已经超过了我们内存里最后一条日志
	// 这种情况通常发生在安装 snapshot 后，内存 entries 已经过期
	if cut >= len(l.entries) {
		// 重建一个新的 dummy entry
		term, err := l.storage.Term(fi - 1)
		if err != nil {
			panic(err)
		}
		l.entries = []pb.Entry{{Index: fi - 1, Term: term}}
		return
	}
	// 5. 裁剪 entries，使 entries[0] 变成新的 dummy（index=fi-1）
	// 裁剪后 entries[0] = 原来 entries[cut]
	l.entries = append([]pb.Entry(nil), l.entries[cut:]...)

	// 6.强制确保 entries[0] 的 index 正好是 fi-1（防御性）
	if l.entries[0].Index != fi-1 {
		term, err := l.storage.Term(fi - 1)
		if err != nil {
			panic(err)
		}
		l.entries[0] = pb.Entry{Index: fi - 1, Term: term}
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
// 返回当前内存中的所有日志条目，即snapshot之后的日志条目，包含了持久化和未持久化的数据
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	// 注意：返回的日志条目不包含dummy entry，即排除第 0 位
	if len(l.entries) > 1 {
		return l.entries[1:]
	}
	return nil
}

// unstableEntries return all the unstable entries
// 计算出未持久化的日志条目，即stabled之后的日志条目
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		unstableIndex := l.toEntryIndex(l.stabled + 1)
		if unstableIndex > 0 && unstableIndex < len(l.entries) {
			return l.entries[unstableIndex:]
		}
	}
	return []pb.Entry{}
}

// nextEnts returns all the committed but not applied entries
// 计算出已提交但未应用到数据库的日志条目，即applied之后committed之前的日志条目
// 返回可以发给状态机去执行的日志条目
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied < l.committed {
		appliedIndex := l.toEntryIndex(l.applied + 1)
		committedIndex := l.toEntryIndex(l.committed + 1)
		if appliedIndex > 0 && appliedIndex < len(l.entries) {
			// 注意：committedIndex可能会超过entries的长度，因此需要进行边界检查
			if committedIndex > len(l.entries) {
				committedIndex = len(l.entries)
			}
			return l.entries[appliedIndex:committedIndex]
		}
	}
	return nil
}

// LastIndex return the last index of the log entries
// 基础查询工具，获取当前节点看到的最新一条日志的index
// 【逻辑】：
// 1.如果正在处理snapshot，则返回snapshot的index
// 2.如果内存中有日志条目，则返回最后一条日志条目的index
// 3.否则，返回storage中最后一条日志条目的index
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	//如果内存中有日志，即除了dummy entry之外还有其他日志条目，则返回最后一条日志条目的index
	if len(l.entries) > 1 {
		return l.entries[len(l.entries)-1].Index
	}
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return lastIndex
}

// Term return the term of the entry in the given index
// 基础查询工具，查询特定index的日志条目的term
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	//1.正在处理snapshot
	if l.pendingSnapshot != nil && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}
	//2.如果i在内存中，则返回内存中的日志条目的term
	if len(l.entries) > 0 {
		entryIndex := l.toEntryIndex(i)
		if entryIndex > 0 && entryIndex < len(l.entries) {
			return l.entries[entryIndex].Term, nil
		}
	}
	//3.否则，从storage中查询
	term, err := l.storage.Term(i)
	if err != nil {
		return 0, err
	}
	return term, nil
}
