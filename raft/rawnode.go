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
	log2 "github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

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
// 传递给上层进行持久化?
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	// 需要持久化的日志条目
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	// 需要持久化的快照
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	// 已提交的日志条目，等待应用到状态机 committed -> applied
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	// 需要发送到其他节点的消息
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
// RawNode 通过生成 Ready 的方式给上层传递信息
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	// 之前的SoftState状态，用于判断是否需要更新
	prevSoftState *SoftState
	// 之前的HardState状态，用于判断是否需要更新
	prevHardState pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	nRawNode := &RawNode{}
	nRawNode.Raft = newRaft(config)
	nRawNode.prevSoftState = &SoftState{
		Lead:      nRawNode.Raft.Lead,
		RaftState: nRawNode.Raft.State,
	}
	nRawNode.prevHardState = pb.HardState{
		Commit: nRawNode.Raft.RaftLog.committed,
		Term:   nRawNode.Raft.Term,
		Vote:   nRawNode.Raft.Vote,
	}
	return nRawNode, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
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

// ApplyConfChange applies a config change to the local node.
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
func (rn *RawNode) Ready() Ready {
	var sendSoftState *SoftState
	// Your Code Here (2A).
	// SoftState will be nil if there is no update
	if rn.prevSoftState.Lead != rn.Raft.Lead || rn.prevSoftState.RaftState != rn.Raft.State {
		sendSoftState = &SoftState{
			Lead:      rn.Raft.Lead,
			RaftState: rn.Raft.State,
		}
		rn.prevSoftState = sendSoftState
	} else {
		sendSoftState = nil
	}

	var sendHardState pb.HardState
	// HardState will be equal to empty state if there is no update.
	if rn.prevHardState.Commit != rn.Raft.RaftLog.committed || rn.prevHardState.Term != rn.Raft.Term || rn.prevHardState.Vote != rn.Raft.Vote {
		sendHardState = pb.HardState{
			Commit: rn.Raft.RaftLog.committed,
			Term:   rn.Raft.Term,
			Vote:   rn.Raft.Vote,
		}
		rn.prevHardState = sendHardState
	}

	var sendMsgs []pb.Message
	if len(rn.Raft.msgs) > 0 {
		sendMsgs = rn.Raft.msgs
		rn.Raft.msgs = make([]pb.Message, 0)
	} else {
		sendMsgs = nil
	}
	var snapshot pb.Snapshot
	if IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) == false {
		snapshot = *rn.Raft.RaftLog.pendingSnapshot
		rn.Raft.RaftLog.pendingSnapshot = nil
	}
	ready := Ready{
		SoftState:        sendSoftState,
		HardState:        sendHardState,
		Entries:          rn.Raft.RaftLog.unstableEntries(),
		Snapshot:         snapshot,
		CommittedEntries: rn.Raft.RaftLog.nextEnts(),
		Messages:         sendMsgs,
	}
	return ready
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	currentHardState := pb.HardState{
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.Vote,
		Commit: rn.Raft.RaftLog.committed,
	}
	// SoftState与HardState是否发生改变
	if rn.prevSoftState.Lead != rn.Raft.Lead || rn.prevSoftState.RaftState != rn.Raft.State {
		return true
	}
	if IsEmptyHardState(currentHardState) == false && (rn.prevHardState.Commit != rn.Raft.RaftLog.committed || rn.prevHardState.Term != rn.Raft.Term || rn.prevHardState.Vote != rn.Raft.Vote) {
		return true
	}
	// 其他因素是否改变
	if len(rn.Raft.RaftLog.unstableEntries()) > 0 || len(rn.Raft.RaftLog.nextEnts()) > 0 || len(rn.Raft.msgs) > 0 {
		return true
	}
	// add snapshot handle
	if IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) == false {
		return true
	}
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
// update raft.Raft's internal state like the applied index, stabled log index, etc.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	if len(rd.CommittedEntries) > 0 {
		log2.Debugf("RawNode %v Advance applied to: %v", rn.Raft.id, rd.CommittedEntries[len(rd.CommittedEntries)-1].Index)
		rn.Raft.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
	if len(rd.Entries) > 0 {
		log2.Debugf("RawNode %v Advance stabled to: %v", rn.Raft.id, rd.Entries[len(rd.Entries)-1].Index)
		rn.Raft.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}
	if IsEmptyHardState(rd.HardState) == false {
		rn.prevHardState = rd.HardState
	}
	// add snapshot handle
	// change raftlog's info using compact
	rn.Raft.RaftLog.maybeCompact()
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
