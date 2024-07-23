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
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	// "github.com/pingcap/log"
	"github.com/pingcap-incubator/tinykv/log"
	"go.uber.org/zap"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
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
	Applied uint64
}

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
type Progress struct {
	// Match表示该节点已成功复制的日志条目索引
	// Next下一个期望接收的日志条目索引
	Match, Next uint64
}

// Raft struct 对应一个集群的节点
type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role, leader, candidate or follower
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randElectionTimeout is the current real tick of election
	randElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
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
	// 要从InitialState读取初始化的Vote与Term用于测试
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	nRaft := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	log.Debugf("newRaft id: %v", c.ID)
	log.Debugf("newRaft peers: %v", c.peers)
	// fmt.Printf("peer size %v\n", len(c.peers))
	//nRaft.Prs[nRaft.id] = &Progress{Match: nRaft.RaftLog.LastIndex(), Next: nRaft.RaftLog.LastIndex() + 1}
	for _, id := range c.peers {
		nRaft.Prs[id] = new(Progress)
		if id == c.ID {
			nRaft.Prs[id].Match = nRaft.RaftLog.LastIndex()
			nRaft.Prs[id].Next = nRaft.RaftLog.LastIndex() + 1
		} else {
			nRaft.Prs[id].Match = 0
			nRaft.Prs[id].Next = nRaft.RaftLog.LastIndex() + 1
		}
	}
	// fmt.Printf("peer size %v\n", len(nRaft.Prs))
	nRaft.id = c.ID
	nRaft.electionTimeout = c.ElectionTick
	nRaft.heartbeatTimeout = c.HeartbeatTick
	nRaft.getNewElectionTick()
	nRaft.randElectionTimeout = c.ElectionTick
	nRaft.RaftLog = newLog(c.Storage)
	return nRaft
}

// tick advances the internal logical clock by a single tick.
// 如果是leader，heartbeat增加，否则election增加
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.broadcastHeartbeat()
		}
		// 3A transfer timeout
		if r.leadTransferee != None {
			r.electionElapsed++
			if r.electionElapsed >= r.randElectionTimeout {
				r.leadTransferee = None
				r.electionElapsed = 0
				r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
			}
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.randElectionTimeout {
			// fmt.Printf("election time:%v - %v\n", r.electionElapsed, r.randElectionTimeout)
			r.electionElapsed = 0
			r.startCampaign()
		}
	}
}

func (r *Raft) getNewElectionTick() {
	// fmt.Println("id", r.id, "timeout", r.electionTimeout)
	r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// sendRequestVote 由候选人向其他节点发送选举请求
func (r *Raft) sendRequestVote() bool {
	// Your Code Here (2A).
	// 将选举请求发送给所有的peer
	lastLogIdx := r.RaftLog.LastIndex()            // 候选人的最后日志条目的索引值
	lastLogTerm, err := r.RaftLog.Term(lastLogIdx) // 候选人最后日志条目的任期号
	if err != nil {
		return false
	}
	log.Debugf("candidate %v start send VoteReq", r.id)
	for id, _ := range r.Prs {
		log.Debugf("current peer %v", id)
	}
	if len(r.Prs) == 1 {
		r.becomeLeader()
	} else {
		for peer := range r.Prs {
			if peer != r.id {
				// fmt.Printf("%v send req to %v\n", r.id, peer)
				mes := pb.Message{
					MsgType: pb.MessageType_MsgRequestVote,
					Term:    r.Term,
					From:    r.id,
					To:      peer,
					Index:   lastLogIdx,
					LogTerm: lastLogTerm,
				}
				r.msgs = append(r.msgs, mes)
			}
		}
	}
	return true
}

// follower处理投票请求
func (r *Raft) handleRequestVote(message pb.Message) {
	if r.Term > message.Term {
		r.sendRequestVoteResponse(message.From, true)
		return
	}
	candidateTerm := message.Term
	logIndex := message.Index
	logTerm := message.LogTerm
	rLogTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	// 自己比message的log更新，拒绝，还要注意message的Term的比较，看看是否要更新follower的信息。
	if logTerm < rLogTerm || (logTerm == rLogTerm && logIndex < r.RaftLog.LastIndex()) {
		if candidateTerm > r.Term {
			r.becomeFollower(candidateTerm, None)
		}
		r.sendRequestVoteResponse(message.From, true)
		return
	}
	// If leader or candidate receives 'MessageType_MsgRequestVote' with higher term, it will revert
	//	back to follower.
	if message.Term > r.Term { // 接收到更高的 term 说明当前节点应该跟随新的 Leader 或者 Candidate
		// fmt.Printf("%v becomeFollower\n", r.id)
		r.becomeFollower(message.Term, None)
		r.Vote = message.From
		r.sendRequestVoteResponse(message.From, false)
		return
	}
	if err != nil {
		panic(err)
	}
	// fmt.Println("node's ", r.id, "vote ", r.Vote)
	if r.Vote == message.From {
		r.getNewElectionTick()
		r.sendRequestVoteResponse(message.From, false)
		return
	}
	if r.Vote == None && (logTerm > rLogTerm || (logTerm == rLogTerm && logIndex >= r.RaftLog.LastIndex())) {
		// candidate的日志是否比自己更新
		r.Vote = message.From
		r.getNewElectionTick()
		r.sendRequestVoteResponse(message.From, false)
		return
	}
	r.getNewElectionTick()
	r.sendRequestVoteResponse(message.From, true)
}

// follower发送投票的应答
func (r *Raft) sendRequestVoteResponse(candidateId uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		From:    r.id,
		To:      candidateId,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat follower handle Heartbeat RPC request
// 根据 Commit 推进自己的 committed
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// 如果心跳消息中的任期小于跟随者的当前任期，则拒绝该消息
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	// 否则变为follower
	r.becomeFollower(m.Term, m.From)
	// 更新tick
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	term, err := r.RaftLog.Term(m.Index)
	if err != nil {
		fmt.Println(fmt.Errorf(err.Error()))
	}
	if term != m.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	// 更新跟随者已提交的日志索引，使其与领导者同步
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.sendHeartbeatResponse(m.From, false)
}

// sendHeartbeatResponse follower 发送heartbeat的response
func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// handleRequestVoteResponse candidate处理其他人投票的结果
func (r *Raft) handleRequestVoteResponse(message pb.Message) {
	if message.Term > r.Term {
		r.becomeFollower(message.Term, r.Lead)
		r.Vote = message.From
		return
	}

	if message.Reject == true {
		r.votes[message.From] = false
	} else {
		r.votes[message.From] = true
	}
	// fmt.Printf("%v vote %v, %v\n", message.From, r.id, r.votes[message.From])
	approveCount, denyCount := r.getPoll()

	// fmt.Printf("%v - %v\n", approveCount, denyCount)
	log.Debugf("peer vote info: %v - %v\n allPrs: %v", approveCount, denyCount, len(r.Prs))
	if approveCount > uint64(len(r.Prs)/2) {
		log.Debugf("peer %v has major appr votes, leader!", r.id)
		r.becomeLeader()
	}
	if denyCount > uint64(len(r.Prs)/2) {
		log.Debugf("peer %v has major deny votes, follower!", r.id)
		r.becomeFollower(r.Term, r.Lead)
	}
}

// getPoll candidate summarize all votes
func (r *Raft) getPoll() (uint64, uint64) {
	approveCount, denyCount := uint64(0), uint64(0)
	for _, vote := range r.votes {
		if vote {
			approveCount++
		} else {
			denyCount++
		}
	}
	return approveCount, denyCount
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	log.Debugf("peer %v becomeFollower", r.id)
	r.State = StateFollower
	r.votes = make(map[uint64]bool)
	r.Lead = lead
	r.Term = term
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// when leader become follower, leadTransferee = None
	r.leadTransferee = 0
	r.getNewElectionTick()
}

// becomeCandidate transform this peer's state to candidate
// 在转变成候选人后就立即开始选举过程
// 自增当前的任期号（currentTerm）
// 给自己投票
// 重置选举超时计时器
// 发送请求投票的 RPC 给其他所有服务器
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 自增当前的任期号（currentTerm）
	log.Debugf("peer %v becomes condidate", r.id)
	r.State = StateCandidate
	r.Term = r.Term + 1
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.getNewElectionTick()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Debugf("peer %v becomes leader", r.id)
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.getNewElectionTick()
	// 当一个领导人刚获得权力的时候，他初始化所有的 nextIndex 值为自己的最后一条日志的 index 加 1
	for peer := range r.Prs {
		r.Prs[peer].Match = 0
		r.Prs[peer].Next = r.RaftLog.LastIndex() + 1
	}
	// 发送空的附加日志（此项目为noopEntry）给其他所有的服务器
	// propose a noop entry on its terml.stabled+1
	// fmt.Println("noop index:", r.RaftLog.LastIndex()+1)
	noopEntry := pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, noopEntry)
	// update self after noop entry
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.broadcastAppend()
	r.maybeCommit()
}

func (r *Raft) isCampaignRelated(m pb.Message) bool {
	if m.MsgType == pb.MessageType_MsgTimeoutNow || m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgHup {
		return true
	} else {
		return false
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 对于Raft节点不在集群当中（尚未初始化）的情况，只能屏蔽选举相关的消息
	if _, exist := r.Prs[r.id]; !exist && r.isCampaignRelated(m) {
		fmt.Println("this peer not in Peers: ", m.To, "msg:", m)
		return nil
	}
	switch r.State {
	case StateFollower:
		r.stepForFollower(m)
	case StateCandidate:
		r.stepForCandidate(m)
	case StateLeader:
		r.stepForLeader(m)
	}
	return nil
}

// stepForFollower follower's step func
func (r *Raft) stepForFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup: // 发起一次选举
		// todo 要先判断自身的条件是否满足选举条件
		if r.RaftLog.pendingSnapshot == nil {
			r.startCampaign()
		}
	// case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	// case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	// case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	//case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.startCampaign()
	}
}

// stepForCandidate candidate's step func
// 如果接收到大多数服务器的选票，那么就变成领导人
// 如果接收到来自新的领导人的附加日志（AppendEntries）RPC，则转变成跟随者
// 如果选举过程超时，则再次发起一轮选举
func (r *Raft) stepForCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.RaftLog.pendingSnapshot == nil {
			r.startCampaign()
		}
	// case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose: // 如果当前没有leader存在,忽略这类消息；否则转发给leader
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	// case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	// case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
		r.startCampaign()
	}
}

// stepForLeader leader's step func
func (r *Raft) stepForLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	case pb.MessageType_MsgPropose:
		// 3A: don't accept any propose when transfer leader
		if r.leadTransferee == None {
			r.HandlePropose(m)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	// case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	// case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleLeaderTimeout()
	}
}

// HandlePropose leader处理propose消息
func (r *Raft) HandlePropose(m pb.Message) {
	entries := m.Entries
	if len(entries) == 0 {
		return
	}

	//// add conf change handle
	//if len(entries) == 1 {
	//	if entries[0].EntryType == pb.EntryType_EntryConfChange {
	//		// you must apply it through RawNode.ApplyConfChange with the pb.ConfChange in the entry
	//		// entry data is pb.ConfChange type
	//
	//		// only then you can add or remove peer to this raft node through raft.Raft.addNode
	//		// and raft.Raft.removeNode according to the pb.ConfChange
	//	}
	//}

	// 注意，propose的信息只有data信息，其他信息需要自己补充
	for _, entry := range entries {
		log.Debug("propose append entry Data:%v\n", zap.String("data", string(entry.Data)))
		// r.RaftLog.entries = append(r.RaftLog.entries, *entry)

		// PendingConfIndex, which
		// is set to a value >= the log index of the latest pending
		// configuration change (if any)
		if entry.EntryType == pb.EntryType_EntryConfChange {
			// Test code schedules the command of one conf change multiple times until the conf change is applied,
			// so you need to consider how to ignore the duplicate commands of the same conf change.
			if r.PendingConfIndex != None && r.PendingConfIndex > r.RaftLog.applied {
				continue
			}
			r.PendingConfIndex = entry.Index
		}
		appEntry := pb.Entry{
			EntryType: entry.EntryType,
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
			Data:      entry.Data,
		}
		r.RaftLog.entries = append(r.RaftLog.entries, appEntry)
	}
	// fmt.Println("propose entries", r.RaftLog.entries)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
	// 特殊处理一个节点的情况
	r.maybeCommit()
}

// broadcastAppend leader broadcast append RPC
func (r *Raft) broadcastAppend() {
	for peer := range r.Prs {
		if r.id != peer {
			r.sendAppend(peer)
		}
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// 用于 Leader 向其他节点同步数据 commit, append
// 在发送附加日志 RPC 的时候，领导人会把新的日志条目前紧挨着的条目的索引位置和任期号包含在日志内。
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1 // follower紧邻新日志条目之前的那个日志条目的索引
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		if errors.Is(err, ErrCompacted) {
			fmt.Println("sendSnapShot from ", r.id, " to ", to)
			// snapshot send scenario
			r.sendSnapshot(to)
			return false
		}
		fmt.Println("sendAppend err: ", err.Error())
		return false
	}

	// project2c bug fix, getPartEntries之前先判断prevLogIndex与LastIndex的关系
	if prevLogIndex > r.RaftLog.LastIndex() {
		return false
	}
	// 需要被保存的日志条目，发送所有在prevLog之后的log
	entries := r.RaftLog.getPartEntries(prevLogIndex+1, r.RaftLog.LastIndex()+1)
	// fmt.Println("sendAppend:", prevLogIndex, " ", r.RaftLog.LastIndex()+1)
	appendEntries := make([]*pb.Entry, 0)
	for _, entry := range entries {
		// fmt.Printf("append entry Term:%v Index:%v Data:%v\n", entry.Term, entry.Index, entry.Data)
		appendEntries = append(appendEntries, &pb.Entry{
			EntryType: entry.EntryType,
			Term:      entry.Term,
			Index:     entry.Index,
			Data:      entry.Data,
		})
	}

	mes := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: appendEntries,
		Commit:  r.RaftLog.committed,
	}
	log.Debugf("peer send Append msg from %v to: %v term: %v prevIndex:%v prevTerm: %v commit: %v, lenEnts %v",
		r.id, to, r.Term, prevLogIndex, prevLogTerm, r.RaftLog.committed, len(appendEntries))
	r.msgs = append(r.msgs, mes)
	return true
}

// handleAppendEntries handle AppendEntries RPC request
// 处理leader的append信息
// 如果跟随者在它的日志中找不到包含相同索引位置和任期号的条目，那么他就会拒绝接收新的日志条目...
func (r *Raft) handleAppendEntries(message pb.Message) {
	// Your Code Here (2A).
	// todo 图展示
	prevLogIndex := message.Index
	prevLogTerm := message.LogTerm
	leaderTerm := message.Term
	if leaderTerm < r.Term { // 如果领导人的任期小于接收者的当前任期，不接受
		r.sendAppendResponse(message.From, true, None)
		return
	}
	// 否则与心跳的处理类似，变为follower，更新当前任期与leader
	r.becomeFollower(message.Term, message.From)
	// 本节点的raftLog中不存在此Index，拒绝
	if prevLogIndex > r.RaftLog.LastIndex() {
		// LastIndex() + 1 -> Next
		r.sendAppendResponse(message.From, true, r.RaftLog.LastIndex()+1)
		return
	}
	if prevLogIndex >= r.RaftLog.entryFirstIdx {
		// check Term whether right
		log.Debugf("handleAppendEntries prevLogIndex >= r.RaftLog.entryFirstIdx")
		term, err := r.RaftLog.Term(prevLogIndex)
		if err != nil {
			fmt.Println("handleAppendEntries err:", fmt.Errorf(err.Error()))
			panic(err)
		}
		if term != prevLogTerm { // 不存在prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目
			r.sendAppendResponse(message.From, true, prevLogIndex)
			return
		}
	}
	// log.Debugf("handleAppendEntries can Append Entries")
	// 以下代码找到追加条目的初始index
	if len(message.Entries) > 0 {
		// fmt.Println(message.Entries)
		for idx, entry := range message.Entries {
			if entry.Index < r.RaftLog.entryFirstIdx {
				// entry.Index < r.RaftLog.entryFirstIdx：不在entries的范围中，跳过
				continue
			}

			// fmt.Println("handleAppendEntries idx:", idx, " entryIndex:", entry.Index, " LastIndex:", r.RaftLog.LastIndex())
			if entry.Index > r.RaftLog.LastIndex() { // entry的index比所有log中的index都大，可以直接把所有条目追加
				for replicaIdx := idx; replicaIdx < len(message.Entries); replicaIdx++ {
					r.RaftLog.entries = append(r.RaftLog.entries, *message.Entries[replicaIdx])
				}
				break
			} else {
				receiveEntryTerm := entry.Term
				logEntryTerm, _ := r.RaftLog.Term(entry.Index)
				//fmt.Println("handleAppendEntries recvTerm:", receiveEntryTerm, " logEntryTerm:", logEntryTerm)
				// 如果一个已经存在的条目和新条目发生了冲突（索引相同，任期不同），
				// 那么就删除这个已经存在的条目以及它之后的所有条目（强制跟随者直接复制自己的日志来处理不一致问题）
				if receiveEntryTerm != logEntryTerm {
					if int(entry.Index-r.RaftLog.entryFirstIdx) >= 0 {
						//fmt.Println("handleAppendEntries recvTerm:", receiveEntryTerm, " logEntryTerm:", logEntryTerm)
						r.RaftLog.entries[entry.Index-r.RaftLog.entryFirstIdx] = *entry
						// 删除这个已经存在的条目以及它之后的所有条目
						r.RaftLog.deleteFollowingEntries(entry.Index)
					}
				}
			}
		}
	}
	// 如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引（leaderCommit > commitIndex），
	// follower需要更新自己的commit进度。
	// 则把接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为：
	// min（领导人的已知已经提交的最高的日志条目的索引 leaderCommit，上一个新条目的索引(AppendEntries跟随者需要追加到自己日志中的最后一个条目的索引) ）
	// fmt.Println("message.Commit ", message.Commit, " lastNewEntryIndex ", r.RaftLog.committed)
	if message.Commit > r.RaftLog.committed {
		// lastNewEntryIndex := r.RaftLog.LastIndex()
		lastNewEntryIndex := message.Index
		if len(message.Entries) > 0 {
			lastNewEntryIndex = message.Entries[len(message.Entries)-1].Index
		}
		// fmt.Println("message.Commit ", message.Commit, " lastNewEntryIndex ", lastNewEntryIndex)
		r.RaftLog.committed = min(message.Commit, lastNewEntryIndex)
	}
	r.sendAppendResponse(message.From, false, r.RaftLog.LastIndex())
}

// sendAppendResponse follower向leader发送appendResp信息
func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		// 日志索引ID，用于节点向leader汇报自己保存的最大日志数据Index
		// 最新匹配的 index??????
		Index:  index, // todo 不同情景下的回复不同
		Term:   r.Term,
		Reject: reject,
	}
	log.Debugf("peer send AppendResponse msg : %+v", msg)
	r.msgs = append(r.msgs, msg)
}

// handleAppendResponse leader处理follower发来的appendResp
func (r *Raft) handleAppendResponse(message pb.Message) {
	if message.Term > r.Term {
		r.becomeFollower(message.Term, None)
		return
	}

	if message.Reject == false { // 响应同步成功
		// 更新 matchIndex 为响应中最后一个成功的日志条目索引
		if r.Prs[message.From].Match >= message.Index {
			// term > msg.Term
			return
		}
		r.Prs[message.From].Match = message.Index
		r.Prs[message.From].Next = message.Index + 1
		// fmt.Printf("index: %v % v\n", message.Index, r.RaftLog.LastIndex())
		// 检查当前有哪些日志是超过半数的节点同意的，再将这些可以提交（commit）的数据广播出去
		r.maybeCommit()
		// 3A, after sendAppend judge whether qualified
		if message.From == r.leadTransferee && message.Index == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(r.leadTransferee)
		}
	} else if r.Prs[message.From].Next > 0 { // 减小 nextIndex 值并进行重试append
		if message.Index == None {
			return
		}
		r.Prs[message.From].Next = message.Index // message中为lastIndex?
		r.sendAppend(message.From)
		return
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) broadcastHeartbeat() {
	// Your Code Here (2A).
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		Term:    r.Term,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match),
		To:      to,
	}
	log.Debugf("peer send Heartbeat msg : %+v", msg)
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeatResponse 如果 Leader 收到后发现其 matchIndex < r.RaftLog.LastIndex() 则触发 append 流程
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// discover server with higher term
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	// follower 日志是可能落后的, 触发 append 流程
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	if lastTerm > m.LogTerm || (lastTerm == m.LogTerm && lastIndex > m.Index) {
		r.sendAppend(m.From)
	}
}

// maybeCommit 判断leader能否进行日志的提交
// 在领导人将创建的日志条目复制到大多数的服务器上的时候，日志条目就会被提交(通过Prs中的信息得到）
// 领导人的日志中之前的所有日志条目也都会被提交，包括由其他领导人创建的条目
func (r *Raft) maybeCommit() {
	// 假设存在 N 满足 N > commitIndex，使得大多数的 matchIndex[i] ≥ N以及 log[N].term == currentTerm 成立，则令 commitIndex = N
	for i := r.RaftLog.committed + 1; i <= r.RaftLog.LastIndex(); i++ {
		matchCount := 0 // 当前日志的复制数量
		for peer := range r.Prs {
			// fmt.Println(matchCount)
			if r.Prs[peer].Match >= i {
				matchCount++
			}
		}

		if matchCount > len(r.Prs)/2 {
			logTerm, error := r.RaftLog.Term(i)
			if error != nil {
				fmt.Println(fmt.Errorf(error.Error()))
			}
			if logTerm == r.Term {
				r.RaftLog.committed = i
				for peer := range r.Prs {
					if peer != r.id {
						r.sendAppend(peer)
					}
				}
			}
		}
	}
}

// startCampaign 发起一次选举
func (r *Raft) startCampaign() {
	r.becomeCandidate()
	r.sendRequestVote()
}

// handleLeaderTimeout
func (r *Raft) handleLeaderTimeout() {
	log.Debugf("current timeElapsed: %v %v", r.electionElapsed, r.heartbeatElapsed)
	if r.leadTransferee != None {
		r.leadTransferee = None
		r.electionTimeout = 0
	}
}

// sendSnapshot send Snapshot RPC request
// Raft module maybe needs to send a snapshot. PeerStorage implements Storage.Snapshot()
// At the next time Raft calling Snapshot, it checks whether the snapshot generating is finished.
// If yes, Raft should send the snapshot message to other peers
// the snapshot message will be handled by onRaftMsg after the snapshot is received
func (r *Raft) sendSnapshot(to uint64) {
	// Raft 也包含一些少量的元数据到快照中：
	// 最后被包含索引指的是被快照取代的最后的条目在日志中的索引值（状态机最后应用的日志），最后被包含的任期指的是该条目的任期号
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		// ErrSnapshotTemporarilyUnavailable
		log.Debug("sendSnapshot ErrSnapshotTemporarilyUnavailable")
		return
	} else {
		// send the snapshot message to other peers
		msg := pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot,
			From:     r.id,
			To:       to,
			Snapshot: &snapshot,
			Term:     r.Term,
		}
		r.msgs = append(r.msgs, msg)
		// update the next index
		r.Prs[to].Next = snapshot.Metadata.Index + 1
	}
}

// handleSnapshot handle Snapshot RPC request
// 从消息 eraftpb.SnapshotMetadata 中恢复 raft 内部状态，如Term、提交索引和成员信息等
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	snapshot := m.Snapshot
	if m.Term < r.Term {
		r.sendSnapResponse(m.From, true, None)
		return
	}
	r.becomeFollower(m.Term, m.From)
	// 如果 snapshot 的 Index 小于或等于当前已经提交的日志的 Index，说明这个 snapshot 已经过时
	if snapshot.Metadata.Index <= r.RaftLog.committed {
		// 通知leader更新自己的next
		r.sendSnapResponse(m.From, false, r.RaftLog.committed)
		return
	}

	// 通常快照会包含没有在接收者日志中存在的信息。在这种情况下，跟随者丢弃其整个日志；它全部被快照取代，并且可能包含与快照冲突的未提交条目。
	// 如果接收到的快照是自己日志的前面部分（由于网络重传或者错误），那么被快照包含的条目将会被全部删除，但是快照后面的条目仍然有效，必须保留

	// 根据其中的 Metadata 来更新自己的 committed、applied、stabled 等等。
	// 丢弃整个日志
	r.RaftLog.entries = make([]pb.Entry, 0)
	r.RaftLog.committed = snapshot.Metadata.Index
	r.RaftLog.applied = snapshot.Metadata.Index
	r.RaftLog.stabled = snapshot.Metadata.Index
	r.RaftLog.entryFirstIdx = snapshot.Metadata.Index + 1

	r.RaftLog.pendingSnapshot = snapshot
	// ConfState contains the current membership information of the raft group
	// 根据其中的 ConfState 更新自己的 Prs 信息
	r.Prs = make(map[uint64]*Progress)
	for _, id := range snapshot.Metadata.ConfState.Nodes {
		r.Prs[id] = &Progress{}
	}
	r.sendSnapResponse(m.From, false, r.RaftLog.LastIndex())
}

func (r *Raft) sendSnapResponse(to uint64, reject bool, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	}
	log.Debugf("peer send SnapResponse msg : %+v", msg)
	r.msgs = append(r.msgs, msg)
}

// handleTransferLeader
func (r *Raft) handleTransferLeader(message pb.Message) {
	transferee := message.From
	log.Debugf("peer handle TransferLeader msg : %+v", message)
	if transferee == None || transferee == r.id || transferee < 0 {
		return
	}
	if transferee == r.leadTransferee {
		return
	}
	if _, exist := r.Prs[transferee]; !exist {
		return
	}

	// overlay before transferee
	r.leadTransferee = transferee
	// 如果在一个 electionTimeout 时间内都没有转移成功，则放弃本次转移，重置 leadTransferee(tick func)
	r.electionElapsed = 0
	// the current leader should first check the qualification of the transferee (namely transfer target) like:
	// is the transferee’s log up to date, etc
	if r.Prs[transferee].Match >= r.RaftLog.LastIndex() {
		// if the transferee is qualified (or after the current leader’s help), the leader should send a MsgTimeoutNow message to the transferee immediately,
		// and after receiving a MsgTimeoutNow message the transferee should start a new election immediately regardless of its election timeout,
		// with a higher term and up to date log, the transferee has great chance to step down the current leader and become the new leader
		log.Debugf("TransferLeader success")
		r.sendTimeoutNow(transferee)
	} else {
		// If the transferee’s log is not up to date, the current leader should 'send a MsgAppend message to the transferee'
		// and 'stop accepting new proposals' in case we end up cycling
		// fmt.Println("trans log not up to date", transferee)
		log.Debugf("TransferLeader no qualified, append")
		r.sendAppend(transferee)
	}
}

// sendTimeoutNow send timeout msg to peer to
func (r *Raft) sendTimeoutNow(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
	}
	r.msgs = append(r.msgs, msg)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.PendingConfIndex = None
	if id < 0 || r.Prs[id] != nil {
		return
	}
	log.Infof("node %v do addNode: %v", r.id, id)
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  1,
	}
	log.Infof("node %v after addNode peers: %v", r.id, r.Prs)
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	r.PendingConfIndex = None
	log.Infof("node %v do deleteNode: %v", r.id, id)
	if r.Prs[id] != nil {
		delete(r.Prs, id)
		if r.State == StateLeader {
			r.maybeCommit()
		}
	}
	log.Infof("node %v after deleteNode peers: %v", r.id, r.Prs)
}
