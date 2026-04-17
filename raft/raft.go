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

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota //这里的iota是一个特殊的常量生成器，在const声明中使用时会自动递增。即从0开始递增
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.

	//【说明】：peers是一个包含了集群中所有节点ID的列表，包括当前节点自己。
	// 这个字段只在启动一个新的raft集群时设置，如果是从之前的配置重启raft，设置peers会导致panic。目前peers是私有的，仅用于测试。
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.

	//【说明】：Applied是最后一个被应用到状态机的日志条目的index。
	// 这个字段只在重启raft时设置，raft不会返回小于或等于Applied的日志条目给应用程序。如果在重启时没有设置Applied，raft可能会返回之前已经应用过的日志条目。
	Applied uint64
}

// 【说明】：validate方法在初始化的时候检查配置的合法性，防止因为错误配置导致的运行时错误
func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
// 【由leader维护】：match记录了leader已经复制到该follower的日志的最高index，next记录了leader下一次发送给该follower的日志条目的index
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64 //当前任期
	Vote uint64 //投票记录，记录了当前任期投票给了哪个节点

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress //记录所有集群节点的日志复制进度，key是节点id，value是Progress结构体，包含了match和next两个字段

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message // 需要发送的消息列表，Raft在处理完消息后会将需要发送的消息放入msgs中，等待上层Rawnode发送

	// the leader id
	Lead uint64

	//【说明】：基础设定的超时滴答数
	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.

	//【说明】：记录经过了多少个滴答tick了
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	Raft := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             None,
		RaftLog:          newLog(c.Storage),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		Prs:              make(map[uint64]*Progress),
	}
	//使用peers初始化Prs
	for _, peer := range c.peers {
		Raft.Prs[peer] = &Progress{
			Match: 0,
			Next:  1,
		}
	}
	return Raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

}

// tick advances the internal logical clock by a single tick.
// 【说明】：
// 每当tick被调用时，raft会检查当前状态，并根据状态进行相应的处理，如果是leader，则检查是否需要发送心跳；如果是follower或candidate，则检查是否需要发起选举
// 上层Rawnode会定期调用tick来驱动Raft状态机的运行，tick的调用频率由上层Rawnode控制，通常是每隔一段时间调用一次，以模拟时间的流逝
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			r.heartbeatElapsed = 0
		}
	}
	if r.State == StateFollower || r.State == StateCandidate {
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			r.electionElapsed = 0
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.State = StateFollower

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	//清理投票与投票记录
	r.Vote = None
	r.votes = make(map[uint64]bool)
}

// becomeCandidate transform this peer's state to candidate
// 进入candidate 状态之后就开始进行选举，但是如果在下一次选举超时到来之前，都还没有选出一个新的leade，那么还会保持在candidate状态重新开始一次新的选举
// candidate状态的节点，如果收到了来自leader的消息，或者更高任期号的消息，都表示已经有leader了，将切换回到follower状态
// 当candidate状态的节点，收到了超过半数的节点选票，那么将切换状态成为新的leader。

// 【说明】：进入candidate状态后，一般会发送MsgRequestVote，但我们把这个功能放在实现Step方法时实现，这样就可以在收到MsgHup消息时进入candidate状态，并且发送MsgRequestVote消息了
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Lead = None
	r.State = StateCandidate

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	//清理投票记录并且投票给自己
	r.votes = make(map[uint64]bool)
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
// 一般是在candidate状态的节点收到了超过半数的选票之后，切换到leader状态，成为新的leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Lead = r.id
	r.State = StateLeader

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	//清理投票记录
	r.Vote = None
	r.votes = make(map[uint64]bool)
	// 初始化进度
	lastIndex := r.RaftLog.LastIndex()
	for id := range r.Prs {
		if id == r.id {
			// 自己已经拥有所有日志
			r.Prs[id].Match = lastIndex
			r.Prs[id].Next = lastIndex + 1
		} else {
			// 其他节点下一次从 lastIndex+1 开始追
			r.Prs[id].Match = 0
			r.Prs[id].Next = lastIndex + 1
		}
	}
	//成为leader后，应该在当前任期内提交一个no-op日志条目，这样可以让其他节点知道这个leader是合法的，并且可以进行日志复制了
	//但这里需要注意，这里是直接操作的entries，所以可能会导致索引不一致
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: lastIndex + 1,
		Data:  nil,
	})
	//更新leader自己的进度并且发送AppendEntries消息给其他节点，让他们知道这个leader已经提交了一个新的日志条目了
	r.Prs[r.id].Match = lastIndex + 1
	r.Prs[r.id].Next = lastIndex + 2
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// 【说明】：实现消息路由分发的功能，根据消息类型调用不同的处理函数来处理消息
// 如果有信息时，上层Rawnode用于传递信息给Raft，Raft根据消息类型进行处理，处理完成后会将需要发送的消息放入msgs中，等待上层Rawnode发送
// 【逻辑】：
// 任期检查、全局信息处理、特定角色状态消息处理
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//=======================【一.任期检查】=============================
	is_local := m.MsgType == pb.MessageType_MsgHup || m.MsgType == pb.MessageType_MsgBeat || m.MsgType == pb.MessageType_MsgPropose
	if !is_local {
		//如果消息不是本地生成的，那么就需要进行任期检查了
		if m.Term > r.Term {
			//如果有来自leader的消息（append/heartbeat/snapshot），记录下来leader
			switch m.MsgType {
			case pb.MessageType_MsgAppend, pb.MessageType_MsgHeartbeat, pb.MessageType_MsgSnapshot:
				r.becomeFollower(m.Term, m.From)
			default:
				//其他消息类型，直接切换到follower状态，任期号更新为消息中的任期号，leader置为None
				r.becomeFollower(m.Term, None)
			}
		}
		if m.Term < r.Term {
			//如果消息的任期号小于当前节点的任期号，那么这个消息就是过时的了，但也需要对三个response消息进行特殊处理，直接回复一个拒绝的消息给发送者
			switch m.MsgType {
			case pb.MessageType_MsgRequestVote:
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
				})
			case pb.MessageType_MsgAppend:
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgAppendResponse,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
					Index:   r.RaftLog.LastIndex(), //拒绝的消息中可以携带当前节点的最后一个日志条目的index，这样发送者就可以根据这个index来调整自己的日志复制策略了
				})
			case pb.MessageType_MsgHeartbeat:
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgHeartbeatResponse,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
				})
			default:
				return nil
			}
		}
	}
	//====================【二.全局信息处理】============================
	//处理本地信息，比如MsgHup（选举超时），MsgBeat（心跳超时），MsgPropose（客户端请求）
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		//收到这条消息后自己开始发起选举，但要判断自己是否满足选举条件
		if r.State == StateLeader {
			return nil
		}
		// （2C）如果当前正在处理 snapshot 相关的消息，不允许发起选举
		if r.RaftLog.pendingSnapshot != nil {
			return nil
		}
		r.becomeCandidate()
		//先处理单节点的问题，再向其他节点发送请求投票的消息(使用到raftlog)
		if len(r.Prs) == 1 {
			r.becomeLeader()
			return nil
		}
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, err := r.RaftLog.Term(lastIndex)
		if err != nil {
			return err
		}
		for id := range r.Prs {
			if id != r.id {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVote,
					To:      id,
					From:    r.id,
					Term:    r.Term,
					LogTerm: lastTerm,
					Index:   lastIndex,
				})
			}
		}
		return nil

	case pb.MessageType_MsgBeat:
		if r.State != StateLeader {
			return nil
		}
		//如果是leader节点收到心跳超时的消息，那么就需要发送心跳消息给其他节点了
		for id := range r.Prs {
			if id != r.id {
				r.sendHeartbeat(id)
			}
		}
		return nil

	case pb.MessageType_MsgPropose:
		//非leader节点收到客户端请求的消息，那么就直接拒绝掉了(但现实中的工程应用里，follower节点一般会将这个请求转发给leader节点）
		if r.State != StateLeader {
			return ErrProposalDropped
		}
		//如果是leader,也需要判断当前是否正在 transfer leader了，如果正在 transfer leader，那么也拒绝掉这个请求
		if r.leadTransferee != None {
			return ErrProposalDropped
		}
		//追加到本地日志并广播给其他节点
		last := r.RaftLog.LastIndex()
		for _, ent := range m.Entries { //从entries中取出每一个日志条目，追加到本地日志中，并且更新日志条目的index和term
			last++
			e := *ent
			e.Index = last
			e.Term = r.Term
			r.RaftLog.entries = append(r.RaftLog.entries, e)
		}
		//更新leader自己的progress
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
		//发送AppendEntries消息给其他节点，让他们知道leader已经有了新的日志条目了
		for id := range r.Prs {
			if id != r.id {
				r.sendAppend(id)
			}
		}
		return nil
	}
	//====================【三.特定角色状态消息处理】====================
	switch r.State {
	case StateFollower:
		//【follower节点】：处理msgappend、msgheartbeat,msgrequestvote和msgsnapshot的消息
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		}

	case StateCandidate:
		//【candidate节点】：处理msgappend、msgheartbeat,msgrequestvote和msgsnapshot的消息，以及requestvoteresponse的消息
		switch m.MsgType {
		case pb.MessageType_MsgAppend, pb.MessageType_MsgHeartbeat, pb.MessageType_MsgSnapshot:
			r.becomeFollower(m.Term, m.From)
			if m.MsgType == pb.MessageType_MsgAppend {
				r.handleAppendEntries(m)
			} else if m.MsgType == pb.MessageType_MsgHeartbeat {
				r.handleHeartbeat(m)
			} else {
				r.handleSnapshot(m)
			}
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		}

	case StateLeader:
		//【leader节点】：处理msgappend和msgheartbeat的response消息
		switch m.MsgType {
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		}
	}
	return nil

}

// handleAppendEntries handle AppendEntries RPC request
// 【说明】：这个函数的功能是处理leader发送过来的AppendEntries消息，但需要注意是否要操作raftlog（不太确定）
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// 【新添加】
func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
}

// 【新添加】
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// Your Code Here (2A).
}

// 【新添加】leader
func (r *Raft) handleAppendResponse(m pb.Message) {
	// Your Code Here (2A).
}

// 【新添加】leader
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Your Code Here (2A).
}

//===========================================================================================================

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
