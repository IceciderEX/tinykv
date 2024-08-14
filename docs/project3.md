# Project3

本部分包括三个部分的内容：
* 对 Raft 算法实施成员变更和领导层变更
* 在 raftstore 上实现 conf 更改和 Region 的 Split
* 调度程序的相关函数

## 3.1 Project3A

Project3A是有关ConfChange与领导转移在Raft层面上的实现（暂时与上层调用无关），
需要实现领导转移的相关函数`LeaderTransfer`与增加节点`AddNode`与删除节点`RemoveNode`。

1. `LeaderTransfer`
对于领导转移，判断逻辑已经在doc文档说的比较清楚了，在当前的Leader收到了一条TransferLeader消息时，就需要进行领导转移操作，具体为：
* 先对转移对象的id进行合法性检查
* 如果转移对象的Log和自己对比是最新的，那么直接发送TimeoutNow消息，转移对象接收到后就会发起选举，并且很有可能赢得选举
* 如果转移对象的Log落后，那么Leader就发送Append消息帮助转移对象得到最新的日志，收到回答后再尝试转移
* 在领导转移时Leader不处理任何发来的Propose
* 在一个ElectionTimeout时间内没有转移成功，则放弃本次转移

2. `AddNode`&`RemoveNode`
在3A中，只需要在Raft节点的Prs加入或者删除增加节点/删除节点的信息即可，不需要真正地实现节点的增删。
需要注意的点：
**在移除节点之后，如果当前节点为Leader，需要尝试进行提交（因为节点数变少了）**


## 3.2 Project3B

本部分需要实现**TransferLeader的Propose操作，在RaftStore层实现ConfChange/Split消息的Propose与Apply流程**。

![alt text](image-4.png)

图中 Store 对应项目代码中的 RaftStore，负责管理多个 Raft 实例，还负责处理客户端请求、状态机操作和数据持久化。
图中 Peer 即为一个 Raft 节点，每个 Peer 都有一个 PeerStorage，用于存储该 Peer 所属 Region 的数据和 Raft 日志。 
图中 Region 为区域，一个 region 包含了多个peer，这些 peer 所在的 RaftStore互不相同。Region 管理着某一个范围的数据，并且一个 region 里面所有 peer 的数据是一致的（增加系统的安全性）。

### 3.2.1 完成过程

1. **TransferLeader的Propose操作**

这部分还是比较简单的，因为领导权转移不需要经过Raft节点进行提交再应用，只需要调用RawNode的`TransferLeader()`方法即可，最后会转到3A中的实现。


2. **ConfChange的Propose/Apply操作**

a.  ConfChange的Propose流程

和之前类似，也是新建一个Proposal，然后Propose到Raft节点中（调用RawNode的`ProposeConfChange()`方法），需要注意**将请求消息进行序列化保存到Context中，在Apply时再进行反序列化读取**

此外，需要考虑一些特殊情况的处理（具体情况见bug记录的部分），比如当**Leader收到了移除自己的消息且节点数只有2时/Leader还有Snapshot没有发送时**，都需要拒绝此次请求。


b. ConfChange的Apply流程

首先需要**检查此请求的RegionEpoch是否已经过期**（Version/VersionEpoch的值是否最新），如果过期，不能进行此次应用

如果为AddNode操作，首先检查该节点是否已经在Prs中了，如果已经在就不需要进行增加节点的操作了。
如果为RemoveNode操作，也是检查该节点是否在Prs中，如果是**当前节点需要被移除，需要调用`destroyPeer()`方法**，随后直接返回即可

其他的具体步骤如下：
* 更新Region的RegionEpoch与Peers信息
* 更新GlobalContext的storeMeta信息（记得上锁），包括region和regionRanges（AddNode操作需要更新regionRanges）
* 将更新后的Region信息写入数据库
* 调用`insertPeerCache()/removePeerCache()`方法，将id信息保存到peerCache中
* 调用RawNode的`ApplyConfChange()`方法，实际上就是调用3A中完成的`addNode()/removeNode()`方法，将peer信息更新到每个Peer中去
* 调用d.HeartbeatScheduler()方法，**将更新后的Region心跳信息发送给调度器，让调度器更新自己的区域信息**（可以解决No region错误）


c. Split的Propose流程

Split操作的Propose也是将数据序列化后发送到Raft节点中，不再详述
**注意需要检查SplitKey是否在Region中，防止过期指令的处理**

d. Split的Apply流程

首先还是需要检查该请求是否合法，**包括RegionEpoch和KeyInRegion两方面**，因为从请求提交到真正Apply的这段时间内，Region可能发生了变化。


随后是执行Split的操作步骤：
- 创建一个新的metapb.Region，使用`cloneMsg()`复制现在的Region数据
- 根据请求的NewPeerIds确定新增Peer的peerId（StoreId使用现在的storeId），赋值给newRegion.Peers(需要**判断新增Peers的个数是否相同**)
- 更新newRegion与prevRegion的相关信息（id，key与RegionEpoch），newRegion分配 [ splitKey, 原endKey ) 的KeyRange，region分配 [ 原startKey, splitKey )
- 更新storeMeta中的Region与RegionRange信息
- 将两个Region的状态写入数据库
- **初始化SizeDiffHint与ApproximateSize，因为Region的Split操作是根据ApproximateSize来进行判断的**
- 调用createPeer()方法，再通过Router的register()方法注册Peer, send()方法启动Region
- **调用heartbeatScheduler()方法发送区域心跳请求，让调度器更新自己的区域信息**

e. 其他修改

- 对于前面实现的4种基本操作的请求，如果包含有Key，需要**增加KeyInRegion的检查**。
- 在请求Apply时，也要**重新进行RegionEpoch的检查**，防止Region在请求到执行的过程中发生了变化。
- 前面模块的修改：发现Commit的逻辑太过繁琐（之前按Index从小到大检查是否可以Commit，导致有时候Leader会发送一堆Append），修改为从大到小检查Index。


### 3.2.2 bug记录

ConfChange部分：

bug1：
TestBasicConfChange3B出现**cluster.MustPut([]byte("k2"), []byte("v2")) request Timeout**的错误

原因：创建节点方法maybeCreatePeer()方法总是失败，原因是sendHeartbeat方法的Commit字段设置错误（应该为0）。因为在进行AddNode之后Leader发送heartbeat消息时，会进行判断，如果Commit字段为0（RaftInvalidIndex）时才会进行CreatePeer操作。
![alt text](image-22.png)

bug2：
d.Proposals[0]的Index在检查时一直小于需要Apply的Entry的Index，导致一直无法回复消息。

解决方法：在处理Proposals时，先把所有小于小于Entry的Index的Proposal删除，再进行进一步的处理。

bug3：
新建的节点收到消息不能进行处理，因为它的r.Peers未初始化。

最初的解决方法：在新建Raft节点时，至少要把自己先加进去。但是TestTransferNonMember3A就过不去了
最终解决方法：一个节点的Prs中如果自己都不存在（是一个新创建的节点），这时接收消息的逻辑为：选举相关（TimeOut/Hup...）的消息才不接收，否则接收。（特别是**快照消息一定要接收，因为需要通过快照的Conf初始化自己的Peers**）

bug4: 
meta corruption detected

原因：根据Raftstore的loadPeers()方法可知，storeMeta需要更新两个状态：regionRanges与regions。**在Addnode时需要插入regionRanges，否则DestroyPeer删除Ranges就会报错**。


bug5：
测试时出现了一种特殊情况：在AddPeer(1)时，1不能收到5发来的消息（step函数没有执行），maybeCreatePeer也没有调用。

原因：忘记将Proposal的回答进行移动（d.proposals = d.proposals[1:]），导致节点没有成功创建。


bug6：TestConfChangeRecoverManyClients3B有概率出现requestTimeout问题。
问题定位：Leader在完成了ConfChange的操作之后可能会停止工作，随后就会伴随着许多can't call command错误。
又检查了一遍ConfChange的实现，发现在在进行Epoch检查出错时，又忘记了Proposal的移动（d.proposals = d.proposals[1:]）......
**决定把所有Proposal的移动都检查一遍再继续往下做**。

bug7：还是requestTimeout的问题，小概率会出现。
![alt text](image-17.png)
原因：分析上图中的日志信息，发现情况为Leader 4被remove，剩下两个节点8、9，但是其中9节点是新增的，还没有完成初始化（snapshot还没有创建完成，Leader4就被destory了，导致9还没有得到所有的Prs信息，接受不了选举信息）。因此Leader的选举一直不成功，导致错误。

现在的解决思路：如果Leader还存在快照没有发送完成，就先拒绝ConfChange的Propose。

bug8: TestConfChangeUnreliable3B的requestTimeout问题，小概率出现。
又是和上面bug7相同的情况。**因为这个测试是Unreliable的，Leader发送的快照消息丢失了**，导致Leader以为自己的Snapshot已经发送完成，然后destory之后又出现与bug7相同的情况。
![alt text](image-25.png)
解决方法：一开始想着多发几次Snapshot信息，但是3A又过不了。
最终解决方法：当snapshot的回复被Leader收到之后，Leader才退出发送快照的状态。

bug9: TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B偶尔会出现RaftLog相关的边界问题，**包括但不限于nextEnts()，unstableEnts()等等**。
解决方法：加上边界判断条件 ？？？

bug10: TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B：

解决方法：再次分析日志信息，现有两个节点3，6，其中6还未初始化，且3与6因为分区Partition一直无法互相接收到消息，导致6的ElectionTimeout到期，成为了Candidate，Partition结束后3收到了6的Heartbeat的resp，变成了follower，此后就是一直3，6轮着变成Candidate，但一直选不出leader。
目前的解决方法：在没有Prs信息时，不能出现因为ElectionTimeout到期导致的becomeCandidate行为。

bug11: TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B：
在进行server的restart时，进行peerStorage的创建时，会读取db的raftState与applyState进行lastIndex与appliedIndex的初始化，这时候某个节点小概率会出现unexpected raft log index: lastIndex 4557 < appliedIndex 4563类似的错误。
![alt text](image-27.png)
对日志进行分析之后，发现是这个节点在接收了一个snapshot之后就进行了restart。再对LastIndex的赋值逻辑进行分析之后，是SaveReadyState的顺序错误，应当**先调用ApplySnapshot()，再调用Append()**，以这样的顺序更新各个Index，因为snapshot的信息可能已经不是最新的了。

此外，在Append()函数中，需要先对需要Append的Entries进行判断，如果需要Append的日志已经过期，不能将它写入数据库并更新raftState。（在2B中也可能会遇到）

----------------------------------------------

Split部分：

bug1：TestSplitRecover测试中，**Scan操作稳定会出现RequestTimeout错误**。如下图所示：

![alt text](image-21.png)
原因：**在Propose时进行KeyNotInRegion的检查时，不需要对Snap操作进行检查，因为Snap请求中不包含key**。

bug2：TestSplitRecoverManyClients测试中，Scan操作稳定会出现Key is not in region错误，在执行Snap操作时，Seek函数报错：Key不在对应的Region中。
![alt text](image-18.png)
原因：在回复Snap请求时会放入当前Region信息，**紧接着回复之后，在Scan操作实际执行之前，这个Region发生了一次Split操作**，导致Region的KeyRange发生了变化，见下图：
![alt text](image-23.png)
解决方法：在回复Snap请求时不直接回复d.Region()的指针，而是先拷贝一份Region的数据，将副本放入回复。

bug3: TestSplitRecoverManyClients测试中，test timed out after 10m0s
![alt text](image-24.png)
实在不知道为什么会超时，搜索后了解到了split checker会依据SizeDiffHint​来判断region承载的数据量是否超出阈值，从而触发split操作。
需要做以下操作：
1. 在Split操作进行Apply之后，**要对SizeDiffHint​和ApproximateSize进行初始化**
2. **在Put/Delete这两个对region大小有影响的操作之后，也要更新SizeDiffHint**。

其他小bug：
newPeerIds与现在的Peers长度应该相同。

### 3.2.3 其他信息

1. Split 的起因

在分布式存储系统中，数据通常被分割成多个区域（Region）进行存储和管理。每个 Region 包含一段连续的键范围（Key Range），并且可以分布在不同的节点上。随着数据的增长和访问频率的变化，某些 Region 可能会变得过大或负载过重，这会导致以下问题：

* **存储不均衡**：一些节点可能会存储大量的数据，而其他节点的存储利用率较低。
* **负载不均衡**：某些节点可能会收到大量的请求，而其他节点的负载较轻。
* **性能瓶颈**：单个 Region 过大可能导致操作延迟增加，影响整体系统性能。

为了应对这些问题，分布式存储系统通常会进行 Region Split 操作，将一个大的 Region 分割成两个或多个较小的 Region。这可以均衡数据存储和请求负载，并改善系统性能。

2. Split 的过程

* **检测与触发**：
   - 系统会检测每个 Region 的大小和访问负载。当检测到某个 Region 超过预设的大小阈值或负载较高时，就会触发 Split 操作。
   
* **确定分割点**：
   - 系统选择一个适当的分割点（Split Key），通常是在 Region 的键范围内。这个分割点通常由数据分布或策略来确定，以保证分割后的 Region 大小和负载相对均衡。

* **生成新 Region 信息**：
   - 根据分割点，将原始 Region 的键范围一分为二。生成两个新的键范围，其中一个从原始的起始键到分割点，另一个从分割点到原始的结束键。
   - 为新生成的 Region 分配唯一的 ID 和相关的元数据信息，包括其对应的 Peers 信息。
   - TiKV 是以 Region 为单位做数据的复制，也就是**一个 Region 的数据会保存多个副本，我们将每一个副本叫做一个 Replica（Peer）**。Replica 之间通过 Raft 来保持数据的一致。一个 Region 的多个 Replica 会保存在不同的节点上，构成一个 Raft Group。其中一个 Replica 会作为这个 Group 的 Leader，其他的 Replica 作为 Follower。所有的读和写都是通过 Leader 进行，再由 Leader 复制给 Follower。

### 3.2.4 心得体会

Project3B确实十分难，需要通过自己**不断地运行测试，输出debug日志信息**并**仔细分析日志**，分析出错原因才能有概率解决遇到的一个个的bug。

此外，在写代码时一定要十分的仔细，特别是各种错误检查的部分。

## 3.3 Project3C

本部分实现了调度程序更新本地区域信息的函数`processRegionHeartbeat`与区域不平衡调度函数`Schedule`。

每个区域都会定期向调度程序发送区域心跳请求RegionHeartbeatRequest（里面包含了Region，Leader，pendingPeers，ApproximateSize等等）接收到区域心跳请求后，调度程序`processRegionHeartbeat`将更新本地区域信息。

调度器会定期检查区域信息，以监测 TinyKV 集群中是否存在不平衡的问题。例如，**如果任何Store包含太多区域，则应将区域从该Store移动到其他Store**。这些移动命令就是相应RegionHeartbeatRequest的响应。

### 3.3.1 完成过程

本部分需要完成两个函数：`processRegionHeartbeat`与`Schedule`。

1. `processRegionHeartbeat`

此函数接收区域心跳请求RegionHeartbeatRequest，通过以下步骤决定是否更新本地区域信息。
* 先寻找本地存储是否存在一个有相同id的区域
** 如果存在，比较心跳请求的RegionEpoch是否比本地的更新，如果没有，无需更新，返回一个Err
** 如果不存在，找到**所有与心跳请求的KeyRange有重叠部分的区域**，比较心跳请求的RegionEpoch是否都比它们的更新，如果没有，也无需更新，返回一个Err
* 上述检查通过之后，还可以进一步确定是否可以跳过此更新，见官方doc中的4个条件（**不实现也可以，冗余更新不会影响正确性**）。如果可以跳过，那么无需更新。
* 更新操作使用`RaftCluster.core.PutRegion`与`RaftCluster.core.UpdateStoreStatus`两个函数。


2. `Schedule`

此函数可避免在一个Store中出现过多的区域，大概的操作是把 RegionSize最大的 store 中随机取出一个 region 放到 RegionSize最小的 store 中，防止各个 RaftStore 中包含的数据的不均衡。

* 首先从集群中选出那些符合条件的Store，把它们按照RegionSize从大到小排序，对每一个Store，依次调用 `GetPendingRegionsWithLock，GetFollowersWithLock, GetLeadersWithLock`，直到能够找到一个region
* 找到Region后，如果Region的Store数量小于集群的MaxReplicas，放弃转移
* 从所有符合条件的Store中选择：**不在选择的Region中的、 RegionSize最小**的那个Store作为转移的目标Store
* 判断两个Store的大小差值是否大于2倍的选择Region的大小，如果是，执行转移操作：调用`cluster.AllocPeer`创建 peer，创建一个新的`CreateMovePeerOperator`操作，返回结果即可。

### 3.3.2 其他信息
 
1. cluster.GetMaxReplicas() 函数相关

通常表示一个 region 在集群中应该有的副本数量（peers数量）。比如，如果一个 region 的副本数量是 3，那么这个 region 应该被复制到 3 个不同的 store 上。如果当前 region 的副本数量少于 cluster.GetMaxReplicas()，说明这个 region 可能在之前的调度中或者因为某些故障，导致其副本数量不够。如果此时再进行调度操作，可能会进一步影响数据的可用性和可靠性。

## 3.3.3 心得

这部分的官方doc文档写的比较详细，按照说明完成即可。
