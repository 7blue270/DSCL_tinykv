package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

//【理解】这个代码文件的作用是实现RPC功能，即api接口层，职责是把客户端的请求转换为对 server.storage 的read/write调用，并组装为protobuf响应

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := new(kvrpcpb.RawGetResponse)
	reader, err := server.storage.Reader(req.Context) //创建一个storage的reader对象
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	defer reader.Close() //确保释放资源

	val, err := reader.GetCF(req.Cf, req.Key) //通过reader对象的GetCF方法获取数据
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	if val == nil {
		resp.NotFound = true
		return resp, nil
	}

	resp.Value = val //通过这个实现了get功能，即根据cf和key获取value，并将结果封装到响应中
	resp.NotFound = false
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := new(kvrpcpb.RawPutResponse)
	modify := storage.Modify{
		Data: storage.Put{
			Cf:    req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	}

	if err := server.storage.Write(req.Context, []storage.Modify{modify}); err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := new(kvrpcpb.RawDeleteResponse)
	modify := storage.Modify{
		Data: storage.Delete{
			Cf:  req.Cf,
			Key: req.Key,
		},
	}

	if err := server.storage.Write(req.Context, []storage.Modify{modify}); err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.Context) //进行scan操作，则需要创建一个reader对象
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf) //通过reader对象的IterCF方法获取迭代器
	defer iter.Close()

	//从start key开始迭代，直到达到limit或者迭代器无效为止
	for iter.Seek(req.StartKey); iter.Valid() && uint32(len(resp.Kvs)) < req.Limit; iter.Next() { //迭代器进行迭代，直到达到limit
		item := iter.Item()

		k := item.KeyCopy(nil)
		v, err := item.ValueCopy(nil)
		if err != nil {
			resp.Error = err.Error()
			return resp, nil
		}

		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{ //每次将key-value对添加到响应中，即实现了scan功能
			Key:   k,
			Value: v,
		})
	}
	return resp, nil
}
