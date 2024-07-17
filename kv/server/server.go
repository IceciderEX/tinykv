package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
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
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).

	// If the key to be read is locked by another transaction at the time of the KvGet request,
	// then TinyKV should return an error

	// A Get() operation first checks for a lock in the timestamp range [0, start timestamp),
	// which is the range of timestamps visible in the transaction’s snapshot (line 12).
	// If a lock is present, another transaction is concurrently writing this cell,
	// so the reading transaction must wait until the lock is released. If no conflicting lock is found,
	// Get() reads the latest write record in that timestamp range (line 19)
	// and returns the data item corresponding to that write record (line 22).
	key := req.Key
	version := req.Version // start ts
	// latch
	keyArr := make([][]byte, 0)
	keyArr = append(keyArr, key)
	server.Latches.WaitForLatches(keyArr)
	defer server.Latches.ReleaseLatches(keyArr)

	reader, err := server.storage.Reader(req.Context)
	mvccTxn := mvcc.NewMvccTxn(reader, version)
	resp := &kvrpcpb.GetResponse{
		RegionError: nil,
		Error:       nil,
		Value:       nil,
		NotFound:    false,
	}
	lock, err := mvccTxn.GetLock(key)
	if err != nil {
		return resp, err
	}
	if lock != nil {
		// lock before the startTs
		if lock.Ts < version {
			keyError := &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
				Retryable: "",
				Abort:     "",
				Conflict:  nil,
			}
			resp.Error = keyError
			return resp, nil
		}
	}
	value, err := mvccTxn.GetValue(key)
	if err != nil {
		return resp, err
	}
	if value == nil {
		resp.NotFound = true
	}
	resp.Value = value
	// latch.ReleaseLatches(keyArr)
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	startTs := req.StartVersion
	reader, err := server.storage.Reader(req.Context)
	resp := &kvrpcpb.PrewriteResponse{}
	if err != nil {
		return resp, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, startTs)
	for _, mutation := range req.Mutations {
		op := mutation.Op
		key := mutation.Key
		value := mutation.Value
		// Abort on writes after our start timestamp...
		// if (T.Read(w.row, c+"write", [start ts , ∞])) return false;
		recentWrite, commitTs, err := mvccTxn.MostRecentWrite(key) // commitTs
		// fmt.Printf("recentWrite: %v :", recentWrite)
		if err != nil {
			return resp, err
		}
		if recentWrite != nil {
			if commitTs >= startTs {
				resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
					Conflict: &kvrpcpb.WriteConflict{
						StartTs:    startTs,
						ConflictTs: recentWrite.StartTS,
						Key:        key,
						Primary:    req.PrimaryLock,
					},
				})
				return resp, nil
			}
		}
		// ...or locks at any timestamp [0, inf)
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    startTs,
					ConflictTs: lock.Ts,
					Key:        key,
					Primary:    req.PrimaryLock,
				},
			})
			return resp, nil
		}
		switch op {
		case kvrpcpb.Op_Put:
			// default
			mvccTxn.PutValue(key, value)
			// lock
			newLock := &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      startTs,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindPut,
			}
			mvccTxn.PutLock(key, newLock)
		case kvrpcpb.Op_Del:
			// default
			mvccTxn.DeleteValue(key)
			// lock
			newLock := &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      startTs,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindDelete,
			}
			mvccTxn.PutLock(key, newLock)
		case kvrpcpb.Op_Rollback:
		}
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	startTs := req.StartVersion
	commitTs := req.CommitVersion

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, startTs)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _, key := range req.Keys {
		// KvCommit will fail if the key is not locked or is locked by another transaction.
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return resp, err
		}
		// debug: tests recommitting a transaction (i.e., the same commit request is received twice)
		// the key is not locked
		if lock == nil {
			// scenario1: rollback
			write, _, err := mvccTxn.CurrentWrite(key)
			if err != nil {
				return resp, err
			}
			if write != nil && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "false",
				}
			}
			// scenario2: repeat
			return resp, nil
		}

		// is locked by another transaction
		if lock.Ts != startTs {
			// committing where a key is pre-written by a different transaction(retryable)
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    startTs,
					ConflictTs: lock.Ts,
					Key:        key,
					Primary:    lock.Primary,
				},
			}
			return resp, nil
		}
		newWrite := &mvcc.Write{
			StartTS: startTs,
			Kind:    lock.Kind,
		}
		mvccTxn.PutWrite(key, commitTs, newWrite)
		mvccTxn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// KvScan is the transactional equivalent of RawScan, it reads many values from the database.
// But like KvGet, it does so at a single point in time. Because of MVCC, KvScan is significantly more complex than RawScan -
// you can't rely on the underlying storage to iterate over values because of multiple versions and key encoding.
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	// For scanning, you might find it helpful to implement your own scanner (iterator) abstraction which iterates over logical values,
	// rather than the raw values in underlying storage. kv/transaction/mvcc/scanner.go is a framework for you.
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, mvccTxn)
	defer scanner.Close()

	resp := &kvrpcpb.ScanResponse{
		RegionError: nil,
		Pairs:       make([]*kvrpcpb.KvPair, 0),
	}
	cnt := uint32(0)
	for {
		if cnt >= req.Limit {
			break
		}
		// When scanning, some errors can be recorded for an individual key and should not cause the whole scan to stop.
		key, value, err := scanner.Next()
		if key == nil && value == nil && err == nil {
			break
		}
		if err != nil {
			cnt++
			continue
		}
		pair := kvrpcpb.KvPair{}
		// KvGet logics
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			if lock.Ts < req.Version {
				pair.Error = &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				}
			}
		} else if value != nil {
			pair.Key = key
			pair.Value = value
			resp.Pairs = append(resp.Pairs, &pair)
		}
		//fmt.Println("pair: ", pair)
		cnt++
	}
	return resp, nil
}

// KvCheckTxnStatus checks for timeouts, removes expired locks and returns the status of the lock.
// CheckTxnStatus reports on the status of a transaction and may take action to
// rollback expired locks.
// If the transaction has previously been rolled back or committed, return that information.
// If the TTL of the transaction is exhausted, abort that transaction and roll back the primary lock.
// Otherwise, returns the TTL information.
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	// A timestamp consists of a physical and a logical component.
	// The physical part is roughly a monotonic version of wall-clock time.
	// Usually, we use the whole timestamp, for example when comparing timestamps for equality.
	// However, when calculating timeouts, we must only use the physical part of the timestamp.
	// To do this you may find the PhysicalTime function in transaction.go useful.
	currentPhysicalTime := mvcc.PhysicalTime(req.CurrentTs)
	lockPhysicalTime := mvcc.PhysicalTime(req.LockTs)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	mvccTxn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, commitTs, err := mvccTxn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	// If the transaction has previously been rolled back or committed, return that information.
	// committed_version > 0 || lock_ttl == 0 && commit_version == 0
	if write != nil && commitTs > 0 {
		// resp.CommitVersion = commitTs
		if write.Kind == mvcc.WriteKindRollback {
			resp.CommitVersion = 0
		} else {
			resp.CommitVersion = commitTs
		}
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}
	lock, err := mvccTxn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		mvccTxn.Rollback(req.PrimaryKey, req.LockTs, false)
		err = server.storage.Write(req.Context, mvccTxn.Writes())
		if err != nil {
			return resp, err
		}
		return resp, nil
	}
	lockDeadTime := lockPhysicalTime + lock.Ttl
	// fmt.Println("currentPhysicalTime: ", currentPhysicalTime, "locktime: ", lockPhysicalTime)
	// If the TTL of the transaction is exhausted, abort that transaction and roll back the primary lock.
	if lockDeadTime < currentPhysicalTime {
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		mvccTxn.Rollback(req.PrimaryKey, req.LockTs, true)
	}
	// fmt.Println("writes: ", mvccTxn.Writes())
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return resp, err
	}
	resp.LockTtl = lock.Ttl
	return resp, nil
}

// KvBatchRollback checks that a key is locked by the current transaction,
// and if so removes the lock, deletes any value and leaves a rollback indicator as a write.
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	startTs := req.StartVersion
	reader, err := server.storage.Reader(req.Context)
	resp := &kvrpcpb.BatchRollbackResponse{}
	if err != nil {
		return resp, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, startTs)
	for _, key := range req.Keys {
		keyLock, err := mvccTxn.GetLock(key)
		if err != nil {
			return resp, err
		}
		write, commitTs, err := mvccTxn.CurrentWrite(key)
		// Will fail if the transaction has already been committed or keys are locked by a different transaction
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				if commitTs != 0 {
					resp.Error = &kvrpcpb.KeyError{}
					return resp, nil
				}
			}
		}
		// keys are locked by a different transaction
		if keyLock != nil && keyLock.Ts != startTs {
			// resp.Error = &kvrpcpb.KeyError{}
			mvccTxn.Rollback(key, startTs, false)
			continue
		}
		// missing prewrite
		if write == nil {
			mvccTxn.Rollback(key, startTs, true)
			continue
		}

		// If the keys were never locked, no action is needed but it is not an error.
		if keyLock == nil {
			continue
		}
		// no write, rollback
		mvccTxn.Rollback(key, startTs, true)
	}
	// fmt.Println("writes: ", mvccTxn.Writes())
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// KvResolveLock inspects a batch of locked keys and either rolls them all back or commits them all.
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	// Since KvResolveLock either commits or rolls back its keys,
	// you should be able to share code with the KvBatchRollback and KvCommit implementations.
	startTs := req.StartVersion
	reader, err := server.storage.Reader(req.Context)
	resp := &kvrpcpb.ResolveLockResponse{}
	if err != nil {
		return resp, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, startTs)
	// If commit_version is 0, TinyKV will rollback all locks
	if req.CommitVersion == 0 {
		newReq := &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: startTs,
			Keys:         nil,
		}

		iter := reader.IterCF(engine_util.CfLock)
		for ; iter.Valid(); iter.Next() {
			item := iter.Item()
			key := item.Key()
			lock, err := mvccTxn.GetLock(key)
			if err != nil {
				return resp, err
			}
			if lock == nil {

			} else if lock.Ts == startTs {
				newReq.Keys = append(newReq.Keys, key)
			}
		}
		_, err = server.KvBatchRollback(context.TODO(), newReq)
		if err != nil {
			return nil, err
		}
	} else {
		// If commit_version is greater than 0 it will commit those locks with the given commit timestamp
		newReq := &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  startTs,
			Keys:          nil,
			CommitVersion: req.CommitVersion,
		}

		iter := reader.IterCF(engine_util.CfLock)
		for ; iter.Valid(); iter.Next() {
			item := iter.Item()
			key := item.Key()
			lock, err := mvccTxn.GetLock(key)
			if err != nil {
				return resp, err
			}
			if lock.Ts == startTs {
				newReq.Keys = append(newReq.Keys, key)
			}
		}
		_, err = server.KvCommit(context.TODO(), newReq)
		if err != nil {
			return nil, err
		}
	}
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
