package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	startKey    []byte
	txn         *MvccTxn
	currentIter engine_util.DBIterator
	valid       bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		startKey:    startKey,
		txn:         txn,
		currentIter: txn.Reader.IterCF(engine_util.CfWrite), // not CfDefault, like KvGet
	}
	scanner.currentIter.Seek(EncodeKey(startKey, txn.StartTS)) // first locate
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.currentIter != nil {
		scan.currentIter.Close()
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.currentIter == nil {
		return nil, nil, nil
	}
	// key not nil
	if !scan.currentIter.Valid() {
		scan.currentIter = nil
		return nil, nil, nil
	}
	item := scan.currentIter.Item()
	key := item.Key()
	userKey := DecodeUserKey(key)
	ts := decodeTimestamp(key)
	for scan.currentIter.Valid() {
		// data after txn's ts, skip
		if ts > scan.txn.StartTS {
			scan.currentIter.Seek(EncodeKey(userKey, scan.txn.StartTS))
			item = scan.currentIter.Item()
			userKey = DecodeUserKey(item.Key())
			ts = decodeTimestamp(item.Key())
		} else {
			break
		}
	}

	value, err := item.Value()
	if err != nil {
		return nil, nil, err
	}
	writeRecord, err := ParseWrite(value)
	if err != nil {
		return nil, nil, err
	}
	val := make([]byte, 0)
	// find next key/value pair
	for ; scan.currentIter.Valid(); scan.currentIter.Next() {
		currentUserKey := DecodeUserKey(scan.currentIter.Item().Key())
		if !bytes.Equal(currentUserKey, userKey) {
			// fmt.Println("nextkey", currentUserKey)
			break
		}
	}
	if writeRecord.Kind == WriteKindPut {
		val, err = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, writeRecord.StartTS))
		if err != nil {
			return nil, nil, err
		}
	} else {
		return userKey, nil, nil
	}
	return userKey, val, nil
}
