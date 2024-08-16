package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")

	// 创建数据库与引擎
	kvDB := engine_util.CreateDB(conf.DBPath, conf.Raft)
	engines := engine_util.NewEngines(kvDB, nil, kvPath, "")
	return &StandAloneStorage{engines: engines, config: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// 原来不需要写...
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	// 原来不需要写...
	return nil
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (reader *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	// 直接调用engine utils里面的API接口即可
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return val, err
}

func (reader *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, reader.txn)
	return iter
}

func (reader *StandAloneReader) Close() {
	reader.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// 创建一个只读事务
	txn := s.engines.Kv.NewTransaction(false)
	// 返回一个reader即可，接口方法已在上面实现
	return &StandAloneReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 创建一个读写事务
	txn := s.engines.Kv.NewTransaction(true)
	// 对于batch中的modify操作，根据类型对数据库进行写入操作
	for _, modify := range batch {
		switch modify.Data.(type) {
		// put
		case storage.Put:
			put := modify.Data.(storage.Put)
			// 要先加上CF前缀，再调用badger的set方法
			err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			if err != nil {
				return err
			}
			// delete
		case storage.Delete:
			del := modify.Data.(storage.Delete)
			// 要先加上CF前缀，再调用badger的delete方法
			err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key))
			if err != nil {
				return err
			}
		}
	}
	// 提交事务
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}
