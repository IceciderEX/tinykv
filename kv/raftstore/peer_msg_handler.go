package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// HandleRaftReady After the message is processed, the Raft node should have some state updates.
// HandleRaftReady should get the ready from Raft module and do corresponding actions like persisting log entries,
// applying committed entries and sending raft messages to other peers through the network
// CommittedEntries是client propose的entry经过commit操作后需要apply的
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	// get the ready from Raft module
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()
		// use the provided method SaveReadyState() to persist the Raft related states
		_, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			fmt.Println("HandleRaftReady's error", fmt.Errorf(err.Error()))
			return
		}
		// When you are sure to apply the snapshot, you can update the peer storage’s memory state
		// like RaftLocalState, RaftApplyState, and RegionLocalState
		// also don’t forget to persist these states to kvdb and raftdb and remove stale state from kvdb and raftdb
		// Besides, you also need to update PeerStorage.snapState to snap.SnapState_Applying
		// and send runner.RegionTaskApply task to region worker through PeerStorage.regionSched and wait until region worker finishes

		// sending raft messages to other peers through the network
		// Use Transport to send raft messages to other peers, it’s in the GlobalContext
		d.Send(d.ctx.trans, ready.Messages)
		// applying committed entries
		// apply the committed entries and update the applied index in one write batch
		// 即为经过Raft节点Commit之后，需要apply的日志（里面存放着Request）
		wb := &engine_util.WriteBatch{}
		for _, entry := range ready.CommittedEntries {
			// process entry
			// applies the committed entry to the state machine
			d.processCommittedEntry(entry, wb)
			// 3B add destroy peer
			if d.stopped {
				return
			}
		}
		// applied index update -> kv's applyState
		if len(ready.CommittedEntries) > 0 {
			log.Debug("HandleRaftReady committed len: %v", len(ready.CommittedEntries))
			d.peerStorage.applyState.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
			err = wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				log.Error("HandleRaftReady's setMeta error", fmt.Errorf(err.Error()))
			}
			wb.MustWriteToDB(d.peerStorage.Engines.Kv)
		}
		// advance rd
		d.RaftGroup.Advance(ready)
	}
}

// processCommittedEntry applies the committed entry to the state machine(kv db)
// CommittedEntries是client propose的entry经过commit操作后需要apply的
// 每执行完一次 apply，都需要对 proposals 中的相应Index的proposal进行 callback 回应（调用 cb.Done()），
// 表示这条命令已经完成了（如果是Get命令还会返回取到的value），然后从中删除这个proposal
// 就是把entry中的操作拿给kvDB去实际执行（例如put、delete某个数据）
func (d *peerMsgHandler) processCommittedEntry(entry eraftpb.Entry, wb *engine_util.WriteBatch) {
	if entry.EntryType == eraftpb.EntryType_EntryNormal {
		d.applyToDbAndRespond(entry, wb)
		// wb.WriteToDB(d.peerStorage.Engines.Kv)
	} else if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		// 测试代码会多次调度一个 conf 更改的命令，直到应用 conf 更改，因此您需要考虑如何忽略同一 conf 更改的重复命令。
		// After the log is committed, change the RegionLocalState, including RegionEpoch and Peers in Region
		confChange := &eraftpb.ConfChange{}
		err := confChange.Unmarshal(entry.Data)
		if err != nil {
			fmt.Println("HandleRaftReady's confChange unmarshal error", err)
		}

		// fmt.Println("confChange: ", confChange)
		// get peer info from the request (ctx)
		peerInfo := &raft_cmdpb.RaftCmdRequest{}
		err = peerInfo.Unmarshal(confChange.Context)
		// fmt.Println("peerInfo: ", peerInfo)

		// add region epoch check
		err = util.CheckRegionEpoch(peerInfo, d.Region(), true)
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			if d.checkError(entry) == false {
				if len(d.proposals) > 0 {
					d.proposals = d.proposals[1:]
				}
			} else {
				correspondingProposal := d.proposals[0]
				correspondingProposal.cb.Done(ErrResp(errEpochNotMatching))
			}
		}

		d.Region().RegionEpoch.ConfVer++
		if confChange.ChangeType == eraftpb.ConfChangeType_AddNode {
			// todo already in peer's judge?
			d.Region().Peers = append(d.Region().Peers, peerInfo.AdminRequest.ChangePeer.Peer)
			d.insertPeerCache(peerInfo.AdminRequest.ChangePeer.Peer)
			log.Infof("processCommittedEntry addNode: %v", confChange.NodeId)
		} else if confChange.ChangeType == eraftpb.ConfChangeType_RemoveNode {
			// For executing RemoveNode, you should call the destroyPeer()
			// explicitly to stop the Raft module. The destroy logic is provided for you.
			if confChange.NodeId == d.peer.PeerId() {
				d.destroyPeer()
				return
			}
			for idx, peer := range d.Region().Peers {
				// find the node need to be removed
				if peer.Id == confChange.NodeId {
					d.Region().Peers = append(d.Region().Peers[:idx], d.Region().Peers[idx+1:]...)
				}
			}
			d.removePeerCache(confChange.NodeId)
			log.Infof("processCommittedEntry removede: %v", confChange.NodeId)
		}
		for _, peer := range d.Region().Peers {
			log.Infof("EntryConfChange now peers: %v", peer.Id)
		}
		// Call ApplyConfChange() of raft.RawNode
		d.peer.RaftGroup.ApplyConfChange(*confChange)
		// Do not forget to update the region state in storeMeta of GlobalContext
		d.ctx.storeMeta.Lock()
		// update the region state, include regions and regionRanges
		d.ctx.storeMeta.regions[d.regionId] = d.Region()
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
		d.ctx.storeMeta.Unlock()

		// write RegionLocalState to DB
		meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)

		if d.checkError(entry) == false {
			if len(d.proposals) > 0 {
				d.proposals = d.proposals[1:]
			}
		} else {
			correspondingProposal := d.proposals[0]
			resp := raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
					ChangePeer: &raft_cmdpb.ChangePeerResponse{},
				},
			}
			correspondingProposal.cb.Done(&resp)
		}
	}
}

func (d *peerMsgHandler) proposalResponse(prop proposal) {

}

// applyToDB EntryNormal类型，执行entry解码出来的Requests中的4种操作，并响应Proposal
func (d *peerMsgHandler) applyToDbAndRespond(entry eraftpb.Entry, wb *engine_util.WriteBatch) {
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(entry.Data)
	if err != nil {
		log.Error("HandleRaftReady's unmarshal error", fmt.Errorf(err.Error()))
	}
	log.Debugf("proposeRaftCommand new apply entry, peer %v, entry: %v", d.peer.PeerId(), entry)
	cmdResponse := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{},
	}
	// normal requests(4 basic Op)
	if len(msg.Requests) > 0 {
		for _, request := range msg.Requests {
			// 如果是Put或者Delete，把entry中的操作拿给kvDB去实际执行
			switch request.CmdType {
			case raft_cmdpb.CmdType_Put:
				log.Debug("applyToDB Put op", request.Put.GetCf(), string(request.Put.GetKey()), string(request.Put.GetValue()))
				wb.SetCF(request.Put.GetCf(), request.Put.GetKey(), request.Put.GetValue())
			case raft_cmdpb.CmdType_Delete:
				log.Debug("applyToDB Delete op", request.Delete.GetCf(), string(request.Delete.GetKey()))
				wb.DeleteCF(request.Delete.GetCf(), request.Delete.GetKey())
			}

			// log.Infof("applyToDB's proposals:%v", d.proposals)
			var correspondingProposal *proposal
			// len = 0, index not right, term not right
			// 如果proposal检查无错误，那么选择这个proposal进行回应，否则忽略这个proposal
			if d.checkError(entry) == false {
				log.Debug("proposal checkError error, no resp")
				if len(d.proposals) > 0 {
					d.proposals = d.proposals[1:]
				}
			} else {
				correspondingProposal = d.proposals[0]
				switch request.CmdType {
				case raft_cmdpb.CmdType_Invalid:
					cmdResponse.Responses = append(cmdResponse.Responses, &raft_cmdpb.Response{
						CmdType: raft_cmdpb.CmdType_Invalid,
					})
				case raft_cmdpb.CmdType_Get:
					// fmt.Println("applyToDB Get op", request.Get.GetCf(), string(request.Get.GetKey()))
					// Get操作需要返回value
					val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, request.Get.GetCf(), request.Get.GetKey())
					if err != nil {
						fmt.Println("HandleRaftReady's get error", fmt.Errorf(err.Error()))
					}
					log.Debugf("applyToDB Get %s %s %s", request.Get.GetCf(), request.Get.GetKey(), val)
					cmdResponse.Responses = append(cmdResponse.Responses, &raft_cmdpb.Response{
						CmdType: raft_cmdpb.CmdType_Get,
						Get:     &raft_cmdpb.GetResponse{Value: val},
					})
				case raft_cmdpb.CmdType_Put:
					cmdResponse.Responses = append(cmdResponse.Responses, &raft_cmdpb.Response{
						CmdType: raft_cmdpb.CmdType_Put,
						Put:     &raft_cmdpb.PutResponse{},
					})
				case raft_cmdpb.CmdType_Delete:
					cmdResponse.Responses = append(cmdResponse.Responses, &raft_cmdpb.Response{
						CmdType: raft_cmdpb.CmdType_Delete,
						Delete:  &raft_cmdpb.DeleteResponse{},
					})
				case raft_cmdpb.CmdType_Snap: // scan ?
					// in current exp
					// Scan -> Request -> CallCommandInStore -> here
					// need resp, cb.Txn
					log.Debug("applyToDB Scan op", request.Snap)
					cmdResponse.Responses = append(cmdResponse.Responses, &raft_cmdpb.Response{
						CmdType: raft_cmdpb.CmdType_Snap,
						Snap: &raft_cmdpb.SnapResponse{
							Region: d.Region(),
						},
					})
					// Scan操作需要返回一个Txn
					correspondingProposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				}
				correspondingProposal.cb.Done(cmdResponse)
				// 推进proposal进程
				d.proposals = d.proposals[1:]
			}
		}
	} else if msg.AdminRequest != nil {
		// admin command (snapshot...): CompactLogRequest
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			// CompactLogRequest modifies metadata, namely updates the RaftTruncatedState which is in the RaftApplyState.
			// After that, you should schedule a task to raftlog-gc worker by ScheduleCompactLog.
			// Raftlog-gc worker will do the actual log deletion work asynchronously
			// compactLog
			// fmt.Println("applyToDbAndRespond Compact Log")
			compactLog := msg.AdminRequest.CompactLog
			if compactLog.CompactIndex >= d.peerStorage.applyState.TruncatedState.Index {
				// TruncatedState: Record the index and term of the last raft log that have been truncated. (Used in 2C)
				d.peerStorage.applyState.TruncatedState.Index = compactLog.CompactIndex
				d.peerStorage.applyState.TruncatedState.Term = compactLog.CompactTerm
				wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
				// raftWb.DeleteMeta(key) some sort of things
				d.ScheduleCompactLog(compactLog.CompactIndex)
			}
		default:
		}
	}
}

func (d *peerMsgHandler) checkError(entry eraftpb.Entry) bool {
	log.Debugf("check error, peer %v, entry: %v", d.peer.PeerId(), entry)
	if len(d.proposals) > 0 {
		// clear stale
		for _, eachProposal := range d.proposals {
			if eachProposal.index < entry.Index {
				d.proposals = d.proposals[1:]
			}
		}
		correspondingProposal := d.proposals[0]
		d.printProposals(d.proposals)
		if correspondingProposal.index == entry.Index {
			if correspondingProposal.term != entry.Term { // 可能是由于领导者更改，某些日志未提交并被新领导者的日志覆盖
				log.Infof("callBackProposals Term unequal")
				NotifyStaleReq(entry.Term, correspondingProposal.cb)
				return false
			} else {
				return true
			}
		}
		log.Infof("correspondingProposal.index != entry.Index ")
		return false
	}
	log.Infof("checkError len(d.proposals) == 0")
	return false
}

func (d *peerMsgHandler) printProposals(proposals []*proposal) {
	log.Debugf("all proposals len: %v", len(proposals))
	for i, p := range proposals {
		log.Debugf("Proposal %d: index=%d, term=%d, callback=%v", i, p.index, p.term, p.cb)
	}
}

// HandleMsg processes all the messages received from raftCh
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage) // the message transported between Raft peers.
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd) // it wraps the request from clients
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

// preProposeRaftCommand do some checks about req, make sure the msg is valid
func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

// proposeRaftCommand
// 将上层命令打包成日志发送给raft
// proposals 是 封装的回调结构体，是一个切片。当上层想要让 peer 执行一些命令时，会发送一个 RaftCmdRequest 给该 peer，而这个 RaftCmdRequest 里有很多个 Request，
// 需要底层执行的命令就在这些 Request 中。这些 Request 会被封装成一个 entry 交给 peer 去在集群中同步。当 entry 被 apply 时，对应的命令就会被执行。
// callback 里面包含了response，txn与Done
// 上层怎么知道底层的这些命令真的被执行并且得到命令的执行结果呢？这就是 callback 的作用。
// 每当 peer 接收到 RaftCmdRequest 时，就会给里面的每一个 Request 一个 callback，然后封装成 proposal，其中 term 就为该 Request 对应 entry 生成时的 term 以及 index。
// 当 rawNode 返回一个 Ready 回去时，说明上述那些 entries 已经完成了同步，因此上层就可以通过 HandleRaftReady 对这些 entries 进行 apply（即执行底层读写命令）。
// 每执行完一次 apply，都需要对 proposals 中的相应 Index 的 proposal 进行 callback 回应（调用 cb.Done()）
// 表示这条命令已经完成了（如果是 Get 命令还会返回取到的 value），然后从中删除这个 proposal。
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	log.Debugf("proposeRaftCommand peer %v, msg %v", d.peer.PeerId(), msg)
	// Your Code Here (2B).
	// d.proposals Record the callback of the proposals
	// We can't enclose normal requests and administrator request at same time.
	if len(msg.Requests) > 0 { // normal requests (例如对键值存储的读写操作)
		prop := &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		}
		data, err := msg.Marshal()
		if err != nil {
			log.Errorf("proposeRaftCommand err: " + err.Error())
			cb.Done(ErrResp(err))
			return
		}
		// fmt.Println("proposeRaftCommand msg data ", data)
		err = d.RaftGroup.Propose(data)
		if err != nil {
			log.Errorf("proposeRaftCommand err: " + err.Error())
			cb.Done(ErrResp(err))
		}
		log.Debugf("proposeRaftCommand new normal req, peer %v", d.peer.PeerId())
		d.proposals = append(d.proposals, prop)
	} else if msg.AdminRequest != nil { // administrator request (如配置更改、成员管理等)
		// snapshot
		// request has: regionID, d.Meta, compactIdx, term
		// 将 CompactLogRequest 请求 propose 到 Raft 中，等待 Raft Group 确认
		log.Debugf("proposeRaftCommand new admin req, peer %v", d.peer.PeerId())
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			data, err := msg.Marshal()
			if err != nil {
				log.Errorf("proposeRaftCommand err: " + err.Error())
			}
			err = d.RaftGroup.Propose(data)
			if err != nil {
				log.Errorf("proposeRaftCommand err: " + err.Error())
			}
		case raft_cmdpb.AdminCmdType_TransferLeader:
			// 3B add transfer leader admin cmd
			// TransferLeader actually is an action with no need to replicate to other peers,
			// so you just need to call the TransferLeader() method of RawNode instead of Propose() for TransferLeader command.
			transferee := msg.AdminRequest.TransferLeader.Peer
			d.peer.RaftGroup.TransferLeader(transferee.Id)
			// response
			adminResp := raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			}
			resp := raft_cmdpb.RaftCmdResponse{
				Header:        &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &adminResp,
			}
			cb.Done(&resp)
		case raft_cmdpb.AdminCmdType_ChangePeer:
			// Propose conf change admin command by ProposeConfChange
			prop := &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			}
			d.proposals = append(d.proposals, prop)
			peerInfoCtx, err := msg.Marshal()
			if err != nil {
				log.Errorf("proposeRaftCommand marshal err: " + err.Error())
			}
			fmt.Println("eraftpb.ConfChange ctx:", peerInfoCtx)
			confChange := eraftpb.ConfChange{
				ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
				NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
				Context:    peerInfoCtx, // todo Context's init: changePeer's info
			}
			err = d.RaftGroup.ProposeConfChange(confChange)
			if err != nil {
				log.Errorf("proposeRaftCommand err: " + err.Error())
			}
		default:
		}
	}
}

func (d *peerMsgHandler) processRaftCommandMsgRequest(req *raft_cmdpb.Request, cb *message.Callback) {
	// 每当 peer 接收到 RaftCmdRequest 时，就会给里面的每一个 Request 一个 callback，然后封装成 proposal，
	// 其中 term 就为该 Request 对应 entry 生成时的 term 以及 index。
	prop := &proposal{
		index: d.RaftGroup.Raft.RaftLog.LastIndex(),
		term:  d.RaftGroup.Raft.Term,
		cb:    nil,
	}
	d.proposals = append(d.proposals, prop)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	// [LastCompactedIdx, truncatedIndex]
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

// onRaftMsg process the RaftMsg and Step it
func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		log.Debugf("%s handle raft message %s from %d to %d invalid msg",
			d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	log.Debugf("%s handle raft message %s from %d to %d and step it",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	// appliedIdx-firstIdx>= d.ctx.cfg.RaftLogGcCountLimit就需要生成一个新的快照
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
