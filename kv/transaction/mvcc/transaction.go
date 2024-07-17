package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"math"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
// put a new write record
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	var newWrite storage.Modify
	// A user key and timestamp are combined into an encoded key
	newWrite.Data = storage.Put{
		Key:   EncodeKey(key, ts),
		Value: write.ToBytes(),
		Cf:    engine_util.CfWrite,
	}
	txn.writes = append(txn.writes, newWrite)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	// ref lock.go AllLocksForTxn returns all locks for the current transaction
	iter := txn.Reader.IterCF(engine_util.CfLock)
	defer iter.Close()

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		lock, err := ParseLock(val)
		if err != nil {
			return nil, err
		}
		if bytes.Compare(item.Key(), key) == 0 {
			return lock, nil
		}
	}
	return nil, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	var newLock storage.Modify

	newLock.Data = storage.Put{
		Key:   key, // no need to include ts
		Value: lock.ToBytes(),
		Cf:    engine_util.CfLock,
	}
	txn.writes = append(txn.writes, newLock)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	var newDeleteLock storage.Modify
	// A user key and timestamp are combined into an encoded key
	newDeleteLock.Data = storage.Delete{
		Key: key,
		Cf:  engine_util.CfLock,
	}
	txn.writes = append(txn.writes, newDeleteLock)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
// The most challenging methods to implement are likely to be GetValue and the methods for retrieving writes.
// You will need to use StorageReader to iterate over a CF. Bear in mind the ordering of encoded keys,
// and remember that when deciding when a value is valid depends on the commit timestamp, not the start timestamp, of a transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	// the ordering of encoded keys:.
	// Keys are encoded in such a way that the ascending order of encoded keys
	// orders first by user key (ascending), then by timestamp (descending)

	// get the most recent value committed 'before' the start of this transaction
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	// 找到第一个valid at the start timestamp of this transaction(ts<=txn.startTs)的write记录
	iter.Seek(EncodeKey(key, txn.StartTS))

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		// fmt.Println("GetValue key", userKey)
		if bytes.Compare(userKey, key) != 0 {
			break
		}
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		writeRec, err := ParseWrite(value)
		if err != nil {
			return nil, err
		}
		// fmt.Println("GetValue ts", writeRec.StartTS)
		if writeRec.Kind == WriteKindPut {
			return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, writeRec.StartTS))
		} else {
			return nil, nil
		}
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	var newPutValue storage.Modify
	// A user key and timestamp are combined into an encoded key
	newPutValue.Data = storage.Put{
		Key:   EncodeKey(key, txn.StartTS),
		Value: value,
		Cf:    engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, newPutValue)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	var newDeleteValue storage.Modify
	// A user key and timestamp are combined into an encoded key
	newDeleteValue.Data = storage.Delete{
		Key: EncodeKey(key, txn.StartTS),
		Cf:  engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, newDeleteValue)
}

func (txn *MvccTxn) Rollback(key []byte, startTs uint64, deleteLock bool) {
	rollback := Write{
		StartTS: startTs,
		Kind:    WriteKindRollback,
	}
	txn.PutWrite(key, startTs, &rollback)
	txn.DeleteValue(key)
	if deleteLock == true {
		txn.DeleteLock(key)
	}
}

// CurrentWrite searches for a 'write record' with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		if bytes.Compare(key, userKey) != 0 {
			continue
		}
		commitTs := decodeTimestamp(item.Key())
		value, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(value)

		if err != nil {
			return nil, 0, err
		}
		if write.StartTS == txn.StartTS {
			return write, commitTs, nil
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write record with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	// Keys are encoded in such a way that the ascending order of encoded keys
	// orders first by user key (ascending), then by timestamp (descending)
	iter.Seek(EncodeKey(key, math.MaxUint64))

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		if bytes.Compare(key, userKey) != 0 {
			continue
		}
		commitTs := decodeTimestamp(item.Key())
		value, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}
		return write, commitTs, nil
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
