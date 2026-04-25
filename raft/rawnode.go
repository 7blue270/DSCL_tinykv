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

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

//【raft】：实现raft需要实现，leader选举，日志复制，持久化等功能

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.                     //当前节点的角色和当前leader的id
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.          //当前的任期term,投票vote，提交的进度commit
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.                                                     //需要持久化的日志条目
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.                                                                //待应用的日志条目，已经持久化了，但是还没有应用到状态机中
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.    //待发送的网络包
	Messages []pb.Message
}

// 【理解】：raftnode相当于是一个主板，raft相当于是一个cpu，ready相当于是一个总线，外部应用通过ready来和raft进行交互
// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).

	//上一次交付给应用层的状态，用于ready/hasready的去重
	prevSoftSt *SoftState
	prevHardSt pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
// 【注意】：config是用来初始化raft的
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}
	// 【说明】：validate方法在初始化的时候检查配置的合法性，防止因为错误配置导致的运行时错误
	if err := config.validate(); err != nil {
		return nil, err
	}

	raft := newRaft(config)

	rn := &RawNode{
		Raft: raft,
		prevSoftSt: &SoftState{
			Lead:      raft.Lead,
			RaftState: raft.State,
		},
		prevHardSt: pb.HardState{
			Term:   raft.Term,
			Vote:   raft.Vote,
			Commit: raft.RaftLog.committed,
		},
	}
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
// 【理解】：主动发起选举
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
// 【理解】：上层应用收到客户端请求，翻译为msgpropose信息，发送给raft
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// 【P3】 ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// 【P3】ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
// 【理解】：上层应用通过ready来获取raft的状态，ready中包含了raft需要上层应用处理的内容，比如需要持久化的日志条目，需要发送的消息等
// 【注意】：ready中只进行打包，更新操作等需要交给advance，从而实现生成和确认的分离，防止出现错误的状态更新
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	r := rn.Raft
	rd := Ready{}
	//软状态是否发生变化
	if rn.prevSoftSt == nil || r.Lead != rn.prevSoftSt.Lead || r.State != rn.prevSoftSt.RaftState {
		rd.SoftState = &SoftState{Lead: r.Lead, RaftState: r.State}
	}
	//硬状态是否发生变化
	if r.Term != rn.prevHardSt.Term || r.Vote != rn.prevHardSt.Vote || r.RaftLog.committed != rn.prevHardSt.Commit {
		rd.HardState = pb.HardState{Term: r.Term, Vote: r.Vote, Commit: r.RaftLog.committed}
	}
	//是否有未持久化的新日志
	rd.Entries = r.RaftLog.unstableEntries()
	//是否有未应用到状态机的新日志
	rd.CommittedEntries = r.RaftLog.nextEnts()
	//是否有待发送的网络消息
	if len(r.msgs) > 0 {
		//【注意】：这里需要进行深拷贝，防止上层应用修改了ready中的消息，导致raft的状态出现错误； =r.msgs实际是浅拷贝
		rd.Messages = append([]pb.Message(nil), r.msgs...)
	}

	return rd
}

// HasReady called when RawNode user need to check if any Ready pending.
// 【理解】：上层应用通过hasready来检查是否有ready需要处理，如果有就调用ready来获取ready的内容
// 需要检查的内容包括：
// 是否有未发送的网络消息；是否有未持久化的新日志；是否有未应用到状态机的新日志；硬状态/软状态是否有更新；是否有新的快照需要持久化
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	r := rn.Raft
	//软状态是否发生变化
	if rn.prevSoftSt == nil || r.Lead != rn.prevSoftSt.Lead || r.State != rn.prevSoftSt.RaftState {
		return true
	}
	//硬状态是否发生变化
	if r.Term != rn.prevHardSt.Term || r.Vote != rn.prevHardSt.Vote || r.RaftLog.committed != rn.prevHardSt.Commit {
		return true
	}
	//是否有未持久化的新日志
	if len(r.RaftLog.unstableEntries()) > 0 {
		return true
	}
	//是否有未应用到状态机的新日志
	if len(r.RaftLog.nextEnts()) > 0 {
		return true
	}
	//是否有待发送的网络消息
	if len(r.msgs) > 0 {
		return true
	}
	//【TODO】：(2C)是否有新的快照需要持久化

	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
// 【理解】：上层应用在处理完ready中的内容后，调用advance来通知raft，raft可以根据ready中的内容来更新自己的状态（stabled,applied,prs等）
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	r := rn.Raft

	//更新上一次交付的状态(软状态和硬状态)，用于下一次ready/hasready的去重
	if rd.SoftState != nil {
		rn.prevSoftSt = &SoftState{
			Lead:      rd.SoftState.Lead,
			RaftState: rd.SoftState.RaftState,
		}
	}
	if rd.HardState.Term != 0 || rd.HardState.Vote != 0 || rd.HardState.Commit != 0 {
		rn.prevHardSt = pb.HardState{
			Term:   rd.HardState.Term,
			Vote:   rd.HardState.Vote,
			Commit: rd.HardState.Commit,
		}
	}

	//推进stabled和applied
	if len(rd.Entries) > 0 {
		last := rd.Entries[len(rd.Entries)-1].Index //rd.entries中的现在已经全部都持久化了
		r.RaftLog.stabled = max(r.RaftLog.stabled, last)
	}
	if len(rd.CommittedEntries) > 0 {
		last := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index //rd.committedentries中的现在已经全部都应用到状态机了
		r.RaftLog.applied = max(r.RaftLog.applied, last)
	}

	//清空ready中已经处理的消息
	if len(rd.Messages) > 0 {
		//【注意】：这里的思想是，“用多少就截取多少”，这在实现分布式底层系统的时候，或者底层系统的时候是非常有必要的
		r.msgs = r.msgs[len(rd.Messages):]
	}
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
