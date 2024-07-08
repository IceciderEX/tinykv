package server

import (
	"context"
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	// 先得到reader对象，通过接口方法读取
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// 通过请求req里的cf与key调用GetCF方法读值？
	cf, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		// test 里面的小坑，捕获ErrKeyNotFound错误，NotFound: true，error置为nil
		if errors.Is(err, badger.ErrKeyNotFound) {
			return &kvrpcpb.RawGetResponse{NotFound: true}, nil
		}
		// Handle other errors
		return nil, err
	}

	if cf != nil {
		return &kvrpcpb.RawGetResponse{Value: cf}, nil
	} else {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	// 构造Storage.Modify数据结构（里面存放storage.Put数据），调用storage.Write方法
	putOp := storage.Put{Cf: req.Cf, Key: req.Key, Value: req.Value}

	modify := storage.Modify{Data: putOp}
	modifies := []storage.Modify{modify}
	err := server.storage.Write(req.Context, modifies)
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			RegionError: nil,
			Error:       err.Error(),
		}, err
	}
	return &kvrpcpb.RawPutResponse{
		RegionError: nil,
		Error:       "",
	}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	// 构造Storage.Modify数据结构（里面存放storage.Delete数据），调用storage.Write方法
	deleteOp := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	modify := storage.Modify{Data: deleteOp}
	modifies := []storage.Modify{modify}
	err := server.storage.Write(req.Context, modifies)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			RegionError: nil,
			Error:       err.Error(),
		}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	// 先得到reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)

	rawScanResponse := &kvrpcpb.RawScanResponse{
		Kvs: make([]*kvrpcpb.KvPair, 0),
	}
	// 使用reader的CF迭代器进行Scan操作，迭代次数上限为limit
	cnt := uint32(0)
	for iter.Seek(req.GetStartKey()); iter.Valid(); iter.Next() {
		if cnt >= req.Limit {
			break
		}
		item := iter.Item()
		// 获取该item的key与value
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		pair := kvrpcpb.KvPair{
			Error: nil,
			Key:   key,
			Value: value,
		}
		// 向response数组中增加结果的pair
		rawScanResponse.Kvs = append(rawScanResponse.Kvs, &pair)
		cnt++
	}
	// close迭代器
	iter.Close()
	return rawScanResponse, nil
}
