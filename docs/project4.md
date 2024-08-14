# Project4

本部分需要实现事务相关功能，以支持对多个客户端的并发请求保持数据一致性的能力。

## 4.1 Project4A

这部分需要实现MVCC（多版本并发控制）层，基于Percolator的两阶段提交协议（PreWrite、Commit）。
具体为实现`transactions.go`下 MvccTxn 的各种方法，MvccTxn 是支持MVCC的事务，提供**原子方式的**基于用户键的读取和写入操作以及锁、写入和值的逻辑表示形式。数据的修改操作被收集到 MvccTxn ，一旦收集了命令的所有修改，它们将一次性写入数据库。

TinyKV 使用三个列族（CF）： default 保存用户值、 lock 存储锁和 write 记录更改。
**注意get/put时各个CF的key的包含内容**：
* lock CF：只包括User key
* default CF：包括User key与事务的开始时间戳
* write CF：包括User key与事务的提交时间戳
  
> User key和时间戳会被合并编码为Encoded key（有相关的方法可以调用）。密钥的编码方式是，编码密钥的升序**首先按用户密钥（升序）排序，然后按时间戳（降序）排序**。这可确保遍历各个Key时将首先提供最新版本。

事务具有开始时间戳，并且在提交事务时，它具有提交时间戳（必须大于起始时间戳）。整个事务从在开始时间戳处有效的Key版本中读取。

### 4.1.1 完成过程

本部分比较简单，需要完成各个事务支持的函数（它们与数据/锁/记录的读取/写入相关），测试也比较简洁，只说几个需要注意的方法：

1. `GetLock` 与 `PutLock`

以这两个方法为例子，说明基本的方法的思路。
读取方法`GetLock`通过Txn中的Reader来读取相关内容，通过返回的迭代器来遍历各KV值，找到对应的Lock后，返回即可。
写入方法`PutLock`则需要构造`storage.Modify`对象，放入到Txn的writes数组中，等待后续的写入操作的真正提交。
其他的类似方法的实现思路也是差不多的，

2. `GetValue`
此方法找到某个Key最近的在MvccTxn的开始时间戳时有效的(Write.Ts<=txn.StartTs)的write记录，返回Key对应的Value值。
使用迭代器遍历查找，可使用Seek函数快速定位。
需要注意的是：**只有最近的Write记录为Put类型时，才需要返回值**，否则该值已经被改变。

3. `MostRecentWrite`
此方法找到某个Key最近的Write记录，返回Write记录与提交时间戳。
使用iter.Seek()函数可以快速定位到该记录，commit时间戳包含在了Key中。

### 4.1.2 心得与体会

1. 三个类型的CF对应Key的包含内容是不同的，在赋值与取值的时候需要注意。
2. 在某些函数中，可以根据编码密钥的顺序，灵活使用`iter.Seek()`函数进行快速定位
3. 这部分的测试函数还是比较全面的，可以根据测试结果来对自己的代码进行调试修改。

## 4.2 Project4B

本部分需要利用在Project4A中完成的MvccTxn，实现`KvGet，KvPrewrite，KvCommit`三个方法，它们的大体流程可参考 Perlocator 论文。

KvGet 在**提供的时间戳处**从数据库中读取值。如果要读取的Key在 KvGet 请求时被另一个事务锁定，则 TinyKV 应该返回错误。否则，TinyKV 必须搜索Key的历史值版本以查找**最新**的有效值。

KvPrewrite 与 KvCommit 的功能是分两个阶段将值写入数据库。这两个请求都对多个键进行操作，但实现可以独立处理每个键。
* KvPrewrite将值实际写入数据库。
* KvCommit不改变数据库的值，它只记录某个值被提交。

### 4.2.1 实现过程

1. `KvGet`

KvGet 在**提供的时间戳处**从数据库中读取值。

大体流程如下（参考官方doc文档）：
* 给key加上Latch，防止有其他事务也想对该key进行操作
* 新建一个MvccTxn事务
* 检查该Key是否存在**比Get事务的Start时间戳更小的锁**，如果有，返回Error（因为该键已经被其他事务锁上了）
* 得到该Key的Value并返回，**注意处理NotFound错误**。

2. `KvPreWrite`

KvPrewrite则将值实际写入数据库，并生成一个写入记录。

KvPrewrite的接收参数PrewriteRequest中包含了许多Write请求（使用Mutation结构表示），需要对每一个Mutation请求分别处理。
处理过程可参考Perlocator论文。

* 如果发现**存在Write记录的CommitTs大于当前事务的StartTs**（可以使用Project4A中的MostRecentWrite方法），可能会发生写入冲突，返回Error。（另一个事务已经在本事务开始之前写入了该key）
```Go
if (T.Read(w.row, c+"write", [start ts , ∞])) return false
```

* 或者**该Key存在一个有着任何时间戳的lock**（存在一个key已经被其他事务锁定），返回Error。
```Go
if (T.Read(w.row, c+"lock", [0, ∞])) return false;
```

如果以上情况都不存在，就可以将PrewriteRequest中的修改请求Mutation一一生成对应的Modify存入MvccTxn，并创建对应的Lock。

```Go
T.Write(w.row, c+"data", start ts , w.value);
T.Write(w.row, c+"lock", start ts ,{primary.row, primary.col}); // The primary’s location.
```
最后**需要调用server.storage.Write()方法将上述修改数据实际写入数据库**。

3. `KvCommit`

此方法进行一个事务的提交操作。它不会更改数据库中的值，只会生成一个写入的记录。
与`KvPrewrite`对应，CommitRequest也包含了多个Commit请求。

* 给所有的key请求Latch，防止有其他事务也想对key进行操作
* 对于每一个key的Commit请求，**如果该Key没有lock或者被其他事务锁定**，那么Commit应该失败。（没有上锁说明此事务可能已经回滚/有其他事务的锁也不能提交）
* 把此次Commit请求写入到事务的writes数组中，然后删除对应的锁
* 最后需要调用server.storage.Write()方法将WriteRecord实际写入数据库。

此方法的测试中发现了一个bug：
对于每个Key的Commit请求，如果根据官方doc的说明，该Key没有lock或者被其他事务锁定，那么Commit应该失败。
但是在测试TestCommitConflictRollback4B和TestCommitConflictRepeat4B中，虽然都没有Key对应的锁，但是前者要求返回Error，后者要求不返回。查阅相关资料得知，事务回滚情形应该返回一个Error。
具体判断是不是回滚的方法：**调用mvccTxn.CurrentWrite()，如果返回的write的类型是WriteKindRollback的话，就说明这个事务被回滚了**。

### 4.2.2 心得与体会

1. 可以根据Perlocator论文中对应的伪代码、测试逻辑完成此部分。
2. 要充分了解事务、时间戳、事务的写入过程与三种列族CF的作用等等，这样在写代码的时候的思路就会比较清晰。

## 4.3 Project4C

本部分需要实现`KvScan 、KvCheckTxnStatus 、KvBatchRollback 和 KvResolveLock`四个方法。这四个方法都是 TinyKV Server 提供的 gRPC 的请求处理程序。

### 4.3.1 实现过程

1. `KvScan`

此方法是在Project1中完成的RawScan方法的支持MVCC事务的版本。
要求需要先完成一个Scanner类，用于遍历多个KV对，每次调用Next()方法会返回对当前事务时间戳有效的KV对。Value值的获取逻辑与KvGet类似。

总体流程就是：先创建一个自己实现的Scanner类，然后通过这个Scanner类中的`Next()`方法去一个个地遍历各个KV对（还要注意Value的有效性）。

注意的点：
* 需要在Scan时考虑writeRecord的提交时间戳于请求的开始时间戳之间的关系，**只有提交时间戳小于请求的开始时间戳，这个提交版本才有效。**
* 逻辑与KvGet类似，val值只有在**最近的writeRecord类型为WriteKindPut类型**时才有效。
* 注意在遍历完成所有的Key或者到达Limit限制后停止。

2. `KvCheckTxnStatus`

此方法检查当前事务的状态是否正常，**检查锁的超时情况，删除过期的锁并返回锁的状态**。客户端在由于另一个事务的锁定而无法预写事务时会调用。
具体的实现逻辑代码的注释中解释的比较清楚，
* 如果这个事务已经被回滚或是提交，直接返回即可
* 如果这个事务的primary key的锁已经不存在了，说明该事务不能再进行下去了，需要把这个事务回滚。
* 如果这个事务的锁已经过期，也需要把这个事务回滚

只要注意在修改了事务MvccTxn之后不要忘记调用storage的Write()方法写入数据库即可。

另外，一开始我以为lock的ttl就是当前lock的剩余生存时间，实际上这个ttl是不会变化的，**lock.ttl一直是最初设定好的TTL**。剩余时间是通过mvcc.PhysicalTime()方法通过计算得到的。

3. `KvBatchRollback`

此方法用于批量将被当前事务（开始时间戳相同）所锁定的key进行回滚操作，在Prewrite失败/主提交失败后会调用。

* 如果当前事务已经提交并且提交WriteRecord的类型为WriteKindRollback，说明该事务已经被回滚了，不作处理，否则返回Error。
* 如果该key被其他事务锁定，也需要回滚当前事务
* 如果该事务并不存在任何的预写记录，也需要回滚。
* 如果该key对应的Lock不存在，说明该事务可能已经回滚了，不作处理

其他情况下回滚事务就可以了。

4. `KvResolveLock`

此方法用于检查一批被锁定的Key，然后将它们对应的事务全部回滚或全部提交，

完成过程也比较简单，**根据CommitStamp是否为0来区分全部回滚和全部提交的情形**。
回滚与提交操作只需要先构建正确的`BatchRollbackRequest/CommitRequest`，再直接调用上面已经完成的`KvBatchRollback`与`KvCommit`即可。


### 4.3.2 其他信息

1. 事务的Primary Key

**事务对这个primary主row的lock列写入信息表明持有锁，而其他的rows则随后在其lock列写入primary锁的位置信息。**

Primary key 是从写入的 keys 里面随机选的，在 commit 时需要优先处理 primary key。 当 primary key 执行成功后，其他的 key 可以异步并发执行，因为 primary key 写入成功代表已经决议完成了，后面的状态都可以根据 primary key 的状态进行判断。

在 TiKV 中，每次事务操作都包含一个主键（primary lock）和多个二级键（secondary keys）。

如果主键提交成功，则客户端将提交其他区域中的所有其他键。这些请求应该始终成功，因为通过积极响应预写请求，服务器承诺如果它收到该事务的提交请求，那么它就会成功。所以，一旦客户端有了所有预写响应，事务失败的唯一方式就是超时，在这种情况下，提交主键应该会失败。提交主键后，其他键将无法再超时。

### 4.3.3 心得与体会

本部分也是比较简单的，首先也需要对事务的执行流程进行大致的了解，再去完成这几个函数就不难了。




