package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
)

// raftWorker is responsible for run raft commands and apply raft logs.
type raftWorker struct {
	pr *router

	// receiver of messages should sent to raft, including:
	// * raft command from `raftStorage`
	// * raft inner messages from other peers sent by network
	raftCh chan message.Msg
	ctx    *GlobalContext

	closeCh <-chan struct{}
}

func newRaftWorker(ctx *GlobalContext, pm *router) *raftWorker {
	return &raftWorker{
		raftCh: pm.peerSender,
		ctx:    ctx,
		pr:     pm,
	}
}

// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var msgs []message.Msg
	for {
		msgs = msgs[:0]
		//一旦raftCh中有消息，就一直读取，直到没有消息为止，批量处理
		select {
		case <-closeCh:
			return
		case msg := <-rw.raftCh:
			msgs = append(msgs, msg)
		}
		//被唤醒后进行贪婪批处理raftch中的消息，直到没有消息为止
		pending := len(rw.raftCh)
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.raftCh)
		}
		//worker开始通过peerStateMap批量处理消息，先通过regionID找到对应的peerState，再通过peerState找到对应的peer，最后调用peerMsgHandler处理消息
		peerStateMap := make(map[uint64]*peerState)
		for _, msg := range msgs {
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			if peerState == nil {
				continue
			}
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleMsg(msg)
		}
		//统一的I/O处理，这里就是处理rawnode ready的消息，调用peerMsgHandler的HandleRaftReady方法
		for _, peerState := range peerStateMap {
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleRaftReady()
		}
	}
}

func (rw *raftWorker) getPeerState(peersMap map[uint64]*peerState, regionID uint64) *peerState {
	peer, ok := peersMap[regionID]
	if !ok {
		peer = rw.pr.get(regionID)
		if peer == nil {
			return nil
		}
		peersMap[regionID] = peer
	}
	return peer
}
