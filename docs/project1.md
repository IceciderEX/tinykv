# Project1

本Project需要我们实现一个支持列族（Column Family）的单节点KV数据库。此数据库需要支持四个基本操作：`取值（Get）/存值（Put）/删除（Delete）/扫描（Scan）`。

> 列族Column Family（CF）是将数据库的多个列合并为一个CF进行管理。将相关的列（Column）分组在一起，存储在物理上连续的存储单元中，当需要查询某个列族的数据时，系统只需定位到这个CF所在的位置，而不用遍历整个表格，大大提高了查询效率。

其中，Put 将替换数据库中指定 CF 的特定键的值，Delete 将删除指定 CF 的键值，Get 将提取指定 CF 的键的当前值，Scan 将提取指定 CF 的一系列键的当前值。

## 1.1 实现过程

### 1.1.1 Implement standalone storage engine

首先按照官方文档，需要在 `kv/storage/standalone_storage/standalone_storage.go`文件实现接口`Storage`（StandAloneStorage）。

可以对照项目代码中`RaftServer`的实现对`StandAloneStorage`进行初始化，创建数据库与引擎即可（并不需要初始化raftKV，因为这只是一个单机数据库）。
使用`badger`来实现数据库的相关操作。

随后，`StandAloneStorage`应当提供两个方法Reader与Write。

* Reader方法

Reader方法需要返回一个`StandAloneStorage`的Reader对象，这个对象可以支持对快照执行KV的获取和扫描操作。

使用`badger.Txn`来实现Reader函数，创建并返回一个包含了`badger.Txn`的StandAloneReader对象即可。

读操作相关函数`GetCF()`（读取CF对应的值）与`IterCF()`（获取CF对应的迭代器）都只需要调用`engine_util`中的API实现即可。

* Write方法

Write方法应当为传入的数据库写入操作提供执行服务，写入服务包括两种，Put与Delete。

首先需要创建一个写事务，随后遍历修改请求数组batch，对每一个请求先判断它的类型，再相应调用`badger.Txn`中对应的方法即可。

**最后不要忘记进行事务的提交。**

### 1.1.2 Implement service handlers

随后是构建四个KV服务处理函数：`RawGet / RawScan / RawPut / RawDelete`，用于给TinyKvServer提供接口。

与读有关的方法都是通过上面Reader进行的，与写有关的方法也只需要根据传入的参数正确构建`storage.Modify`对象传递给上面实现的Write方法即可。

* RawGet方法：通过Reader.GetCF()方法得到指定列族下的值，再构建RawGetResponse返回。

* RawPut方法：构造`Storage.Modify`对象（里面存放storage.Put数据），调用storage.Write方法。

* RawDelete方法：构造`Storage.Modify`对象（里面存放着storage.Delete数据），调用storage.Write方法。

* RawScan方法通过查阅相关资料了解到，**RawScan根据指定的起始键（或者直接从开头扫描）开始，逐个键地扫描数据库中的数据。还可以通过设置的限制数量控制扫描的数据条目数，避免一次性扫描过多数据。**
Scan方法遍历数据库中符合条件的数据，并返回相应的数据项或数据集合。可以针对指定的列族（Column Family）进行扫描，以过滤和限定扫描的范围。

此方法使用Reader的CF迭代器进行Scan操作，需要注意在超过限制次数limit后就停止。

## 1.2 心得与体会

1. 官方doc文档写的比较简练，对照RaftServer的实现之后才慢慢上手。

2. 在测试过程中遇到了一些小问题，有些地方（比如RawGet）需要捕获`ErrKeyNotFound`错误（不返回这个Error）并放到Response里（置NotFound为true，error置为nil）。

3. 此部分只是实现了一个单机数据库，过程与内容都比较简单。



