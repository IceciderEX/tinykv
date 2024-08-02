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
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// 快照数据与日志条目数据的分界线 日志条目存储的起始索引，它用于将逻辑上的日志索引转换为实际存储中的索引位置
	entryFirstIdx uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		panic("storage is nil")
		return nil
	}
	nLog := &RaftLog{
		storage: storage,
	}
	// firstidx: Storage.ents数组的第一个数据索引，也就是Storage结构体中快照数据与日志条目数据的分界线
	firstIdx, err := storage.FirstIndex()
	if err != nil {
		panic(err.Error())
	}
	// lastIdx: Storage.ents数组的最后一个数据索引
	lastIdx, err := storage.LastIndex()
	if err != nil {
		panic(err.Error())
	}
	// hardState, confState, err := storage.InitialState()
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	nLog.committed = hardState.Commit
	nLog.applied = firstIdx - 1
	nLog.stabled = lastIdx
	// entryFirstIdx: entries数组第一个entry的Index
	nLog.entryFirstIdx = firstIdx
	// get all entries that have not yet compact, 初始化为storage中已经持久化的entry
	nLog.entries, _ = storage.Entries(firstIdx, lastIdx+1)
	log2.Debugf("committed %d, applied %d, lastIndex: %v, nLog.entryFirstIdx:%v", nLog.committed, nLog.applied, nLog.stabled, nLog.entryFirstIdx)
	return nLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// trunc index + 1
	compactIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic("maybeCompact err: " + err.Error())
	}
	if compactIndex > l.entryFirstIdx {
		// compact the log entries to [compactIndex:]
		if len(l.entries) > 1 {
			l.entries = l.entries[compactIndex-l.entryFirstIdx:]
		}
		l.pendingSnapshot = nil
		l.entryFirstIdx = compactIndex
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	// todo exclude any dummy entries from the return value and debug
	if len(l.entries) == 0 {
		return nil
	}
	// fmt.Println("allEntries", l.entries)
	return l.entries
}

// getPartEntries returns the go index if entries
func (r *RaftLog) getPartEntries(begin, end uint64) []pb.Entry {
	// log.Debug("getPartEntries", zap.Uint64("begin", begin), zap.Uint64("end", end))
	// fmt.Println("getPartEntries", begin, " ", end, " ", r.entryFirstIdx)
	if begin-r.entryFirstIdx >= 0 && end-r.entryFirstIdx <= uint64(len(r.entries)) {
		return r.entries[begin-r.entryFirstIdx : end-r.entryFirstIdx]
	} else {
		return nil
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	// all the unstable entries: [stabled:]
	// fmt.Println("stabled:", l.stabled)
	if len(l.entries) == 0 {
		return nil
	} else {
		// fmt.Println(l.stabled, " ", l.LastIndex())
		if l.stabled >= l.entryFirstIdx-1 && l.stabled-l.entryFirstIdx+1 < uint64(len(l.entries)) {
			return l.entries[l.stabled-l.entryFirstIdx+1:]
		} else {
			return nil
		}
	}
}

// committedEntries return all the committed entries
func (l *RaftLog) committedEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	} else {
		// fmt.Println(l.stabled, " ", l.LastIndex())
		if l.committed >= l.entryFirstIdx-1 {
			return l.entries[l.committed-l.entryFirstIdx+1:]
		} else {
			return nil
		}
	}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// all the committed but not applied entries: [l.applied + 1:l.committed + 1] ?
	if len(l.entries) == 0 {
		return nil
	}
	if l.applied > l.committed {
		return nil
	}
	return l.getPartEntries(l.applied+1, l.committed+1)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var snapIndex uint64
	// snapshot handle
	if IsEmptySnap(l.pendingSnapshot) == false {
		snapIndex = l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) == 0 {
		// entries为空，需要取值为storage的lastIndex
		storageIndex, err := l.storage.LastIndex()
		log.Debug("storage last:%v\n", zap.Uint64("last", storageIndex))
		if err != nil {
			panic(err)
		} else {
			return max(storageIndex, snapIndex)
		}
	} else {
		// entry's last index
		// fmt.Printf("entries last:%v\n", l.entries[len(l.entries)-1].Index)
		return max(snapIndex, l.entries[len(l.entries)-1].Index)
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// 检查索引是否在内存中的日志条目范围内。如果是，则计算内存中的索引并返回对应的条目的任期。
	if i >= l.entryFirstIdx && len(l.entries) > 0 {
		entryIndex := i - l.entryFirstIdx
		// 3b add boundary check
		if entryIndex >= uint64(len(l.entries)) {
			return 0, ErrUnavailable
		}
		if i > l.LastIndex() {
			panic("Term get index out of range")
		}
		return l.entries[entryIndex].Term, nil
	}
	// 否则从storage/snapshot得到
	term, err := l.storage.Term(i)
	if err != nil {
		log.Debug("Term get error", zap.Error(err))
		// 如果storage中返回ErrUnavailable，说明可能在unstable的sanpshot中
		if errors.Is(err, ErrUnavailable) {
			// index - offset > storage's ents
			// add snapshot handle
			if IsEmptySnap(l.pendingSnapshot) == false {
				if i == l.pendingSnapshot.Metadata.Index {
					return l.pendingSnapshot.Metadata.Term, nil
				} else if i < l.pendingSnapshot.Metadata.Index {
					// i < l.pendingSnapshot.Metadata.Index，在unstable的快照中
					return term, ErrCompacted
				}
			}
		} else if errors.Is(err, ErrCompacted) {
			// index < storage's ents[0].index
			// 不应该，返回Err
			return term, err
		}
	}
	return term, err
}

// deleteFollowingTerms 删除entries中Index为from后的元素
func (l *RaftLog) deleteFollowingEntries(from uint64) {
	if from < l.entryFirstIdx || from-l.entryFirstIdx >= uint64(len(l.entries)) {
		panic("deleteFollowingEntries err")
		return
	}
	l.stabled = min(l.stabled, from-1) // 删除可能会影响entries的结构
	l.entries = l.entries[:from-l.entryFirstIdx+1]
}
