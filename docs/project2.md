# Project2

此Project的任务是完成Raft分布式共识算法。

> Raft 是一种分布式一致性算法。Raft 将一致性算法分解成了几个关键模块，例如领导人选举、日志复制、状态机更新与安全性保证等等。
> 
> Raft算法的核心思想是将集群中的节点分为三种角色：
Leader（领导者）：处理客户端请求，并协调其他节点以保证数据一致性。
Follower（跟随者）：响应来自Leader的RPC（远程过程调用），并接收和应用Leader发送的日志条目。
Candidate（候选者）：在选举过程中竞选成为新的Leader。
>
> Raft被广泛应用于多种分布式系统中，如数据库、配置管理系统和分布式存储系统等。

## 2.2 完成过程

在完成这个项目之前，先读了一遍Raft论文，粗略了解了Raft算法的实现。

本部分Project2A的目的是实现基本的 raft 算法步骤，包括领导人选举、日志复制与RawNode接口实现。

### 2.2.1 Project2AA

在本项目中，
1.节点使用`step`函数处理它接收到的所有Message（如果有），这里我使用了根据不同节点的角色来分别为它们定义不同的`step`函数来更好地处理各种信息，在这些`step`函数中再根据消息类型处理，因为不同角色需要处理的信息是不同的。
2.节点通过`tick`方法来增加它的electionElapse或者heartbeatElapse，模拟超时的情况。此方法为上层的 RawNode 调用。
3.节点通过将消息放在自己的 r.msgs 数组中模拟消息的发送，r.msgs 数组会在对应的 RawNode 生成 Ready 的时候被处理并发送。

接下来便是完成Project2AA（领导人选举的功能），参考`eraftpb.proto`中msg的组织方式，了解了ectd Raft的实现，完成了一些有关领导人选举的函数（`MsgRequestVote`，`MsgAppend`的处理等等）。

#### 2.1.1 实现过程

首先需要完成与领导人选举相关的函数与消息的处理与发送，包括以下内容：

1.`tick()`
tick函数很简单，根据节点角色类型增加对应的Elapse即可。主要的作用：
在Follower经过了ElectionTimeout时间过后，它便会变为Candidate，发起选举。
在Leader经过了HeartbeatTimeout时间过后，它便会向其他节点广播心跳。

2.Raft节点的初始化与`becomeXXX`方法
**需要注意根据Config的数据初始化节点**，防止有的测试通过不了的情况。
需要注意的是**各个节点的electionTimeout时间需要不同，可以在[et, 2*et]中随机选择**。否则在某个测试中会导致electionConflict>=0.3的错误。
`becomeXXX`方法主要是为了在节点状态转换时，设置节点的State、Term、Votes与Elapsed相关字段。

3.`MsgRequestVote`
这是候选人请求投票的消息，只需要按照Raft论文来设置各个字段，广播给所有除自己的节点即可。
测试时遇到的问题：**需要处理总结点个数为1的特殊情况，这时候直接成为领导人即可。**

RequestVote的消息处理则麻烦一些，需要考虑**本节点的Term与Msg中的Term的大小关系、本节点是否已经投过票**等等情况。此后使用论文中的判断逻辑（检查Msg上的日志数据是否足够的新）设置Reject字段，最后不要忘记重置tick。

4.`MsgHeartbeat`
`MsgHeartbeat`是Leader发送的心跳信息，发送的Heartbeat消息内容根据论文来设置内容即可。需要特别注意Commit字段的设置，follower 收到后就可以根据 Commit 字段更新自己的已提交的日志索引。

```Go
Commit:  min(r.RaftLog.committed, r.Prs[to].Match)
```
处理Heartbeat时，在测试时注意到**可能会出现一个已经过期的leader（比如被隔离了一段时间，Term更小）向其他人发送心跳的情况。这时，心跳消息中的任期小于跟随者的当前任期，其他人需要拒绝该消息，这时，已经过期的leader接收到这个拒绝消息后需要变为follower**。

5.`RaftLog`相关函数

由于Project2AA中似乎并没有开始考察它实现的正确性，先按照自己的理解写完了各个Todo方法。

#### 2.1.2 其他细节

1. 如何判断候选人的日志至少和自己一样新？

具体来讲，日志新旧的比较有两个标准：
日志中的最后一个条目的任期（term）：
一个日志如果有更高的 term，它就被认为是更新的。
日志中的最后一个条目的索引（index）：
如果两个日志的最后一个条目的 term 相同，那么索引较大的那个日志更“新”。

2. Raft结构体中Prs的作用

对于Prs，它记录了peers的日志进度信息，它记录了**该节点当前已经同步的日志条目的最大索引（MatchIndex）和下一个需要发送的日志条目的索引（NextIndex）**。暂时在Project2AA中没有用到。

#### 2.1.3 心得

1. 一开始可以根据自己的理解写好各种函数，然后再根据测试结果（测试还是非常全面的，能够帮你识别出很多问题），一步步修改自己的代码逻辑，在这个过程中逐渐理解Raft的工作模式。
2. 相关消息函数的处理还是比较复杂的，需要仔细理清相关逻辑。

### 2.2 Project2AB

本部分是实现日志复制相关的功能。

要实现日志复制，首先还需要完成RaftLog相关功能函数的编写。

> RaftLog是各个节点记录并管理日志的数据结构。

接下来便是完成Project2AB（日志复制的功能），主要包括`MsgPropose`，`MsgAppend`与`MsgHeartbeat`的发送与处理，还有Commit操作。

#### 2.2.1 实现过程

1.`RaftLog`相关功能

这部分要完成RaftLog相关功能函数的编写，在编写之前，对整个RaftLog的结构有一个清晰的认识还是十分重要的。以下为本项目中的我理解的RaftLog的结构示意图。
![alt text](raftlog.png)

之后便是完成各种功能函数，需要先理清楚 **Log 的 Index 与日志数组 Entries 的下标、storage 中的 Entries 与 RaftLog 的 Entries 与各个变量(committed, applied, stabled)** 之间的关系与含义。

> committed 为最大的已提交日志的Index
> applied 为最大的应用日志的Index
> stabled 为最大的已经持久化（写入到数据库）的日志的Index

2.`MsgPropose`

此方法为客户端向leader请求数据操作（增加日志）时发送的消息。
leader接收到Propose信息后将日志添加到自己的RaftLog中，随后发送Append消息给其他节点。
需要**注意propose的信息只有data信息，其他信息（比如Term，Index）需要自己补充**。

3.`MsgAppend`

* Leader发送AppendRequest

用于 Leader 向其他节点同步日志数据。
具体的字段设置可以根据Raft论文中的说明进行设置，如下图所示。

![alt text](image-3.png)

需要注意只发送所有在prevLog之后的日志，这里的prevLog是相对于follower而言的。

* Follower接收AppendRequest
这里的处理逻辑还是比较复杂的，可以先根据论文里的处理方法写好相关逻辑，再根据测试一步步地修改。
需要注意的点：
   * 如果发生冲突（对Index相同的Log，RaftLog中的Term与Entry中的Term不同），删除这个已经存在的条目以及它之后的所有条目（Raft是强制跟随者直接复制自己的日志来处理不一致问题）
   * 只需要接收新的Log
   * 注意推进自己的Commit进度

* Follower发送AppendRequestResponse
需要向leader汇报自己保存的最大日志数据的Index，否则会出现Commit进度无法推进的情况。

* Leader接收AppendRequestResponse
leader再每收到一个节点的Response之后，首先也需要判断自己的Term是否已经落后，如果落后直接变为follower。再判断节点是否拒绝Append：
如果接受，更新节点的进度信息并尝试Commit。
如果拒绝，需要减小nextIndex值并重试append。

4. `MsgHeartbeat`的相关修改
   
由于新增了日志操作，leader在发送MsgHeartbeat是要增加Commit字段（设置值可以参考Raft论文）。
Leader在收到了MsgHeartbeatResponse之后需要判断follower日志是否落后, 如果落后也需要触发append流程。

5. `Commit操作`
   
leader将创建的日志条目复制到大多数（>50%）的服务器上的时候，日志条目就会被提交（通过Prs中的信息得到）。
领导人的日志中之前的所有日志条目也都会被提交，包括由其他领导人创建的条目。

> Leader提交日志成功后，Leader通过Append消息实现followers更新自己的commit进度。

#### 2.2.2 其他相关知识

1. Dummy Entry 与 No-op Entry 的区别
   
Dummy Entry：
 Raft 在实现中通常会在日志RaftLog初始化中插入一个初始的 dummy entry。

No-op Entry：
当一个新 Leader 被选举出来时，它会立即向日志中添加一个 No-op entry 并将其复制到所有的 Follower。这个条目有助于快速建立新的 Leader 的权限，确保当前任期的 Leader 开始执行日志复制，并且不会影响到状态机的实际操作，还可以保护之前Leader的Entry。

总结，**Dummy Entry是RaftLog初始化中插入的无实际操作意义的占位Entry，No-op Entry是新Leader被选举出来时向日志中添加的entry。**

2. HardState 与 SoftState
   
HardState与SoftState的解释：
**HardState 是节点的稳定状态，包含了当前节点的持久化状态。它包括一些关键的元数据，这些数据必须被持久化到存储中，以确保节点在重启后能够恢复到一致的状态。**
主要在用于newRaft时从storage中读取初始化信息。
```Go
// HardState contains the state of a node need to be persisted, including the current term, commit index
// and the vote record
message HardState {
    // 当前任期
    uint64 term = 1;
    // 当前任期中节点所投票的候选人ID
    uint64 vote = 2;
    // 当前已提交的日志条目的最大索引
    uint64 commit = 3;
} 
```
**SoftState 是节点的易变状态，它表示了节点的当前运行时状态，包含了一些不需要持久化的信息。这些信息在节点重启后可以通过重新选举或者其他机制重新获取。**

#### 2.2.3 心得

a. 在理解RaftLog时，自己画一张结构示意图会很有帮助。
b. 先自己完成相关函数，再根据测试的结果进行debug。
c. 通过观察输出相关的数据来进行debug。

### 2.3 Project2AC

本部分需要完成RawNode的相关部分，RawNode是我们与上层应用程序交互的接口，是Raft的一个包装类。

RawNode通过`Ready` 结构体给上层传递信息，它封装了当前节点在一个时间点上需要处理的日志条目、消息以及快照等状态。通过 `Ready`，Raft节点可以将状态信息传递给上层应用，以便上层应用进行持久化存储、状态更新和消息发送。

#### `Ready` 机制的工作流程

1. **收集状态信息**：Raft 节点在执行状态转换（例如日志追加、领导选举）之后，产生的状态变化和需要处理的日志条目、消息等会被收集到一个 `Ready` 结构体中。

2. **传递给上层应用**：生成 `Ready` 结构体后，Raft 节点会将它传递给上层应用。上层应用需要根据 `Ready` 中的信息来执行一系列操作，例如将**日志条目持久化、更新状态机、发送消息**等。

3. **执行应用逻辑**：上层应用获取 `Ready` 后，会依次处理其内部的日志条目、快照和消息。例如，将日志条目和快照持久化到磁盘，将 `CommittedEntries` 应用到状态机，发送 `Messages` 给其他节点等。

4. **通知进展**：上层应用完成了对 `Ready` 的处理后，需要调用 `Advance` 方法来通知 Raft 节点，表示已经处理完了这个 `Ready`，节点可以继续生成下一个 `Ready`。
   
本部分需要完成Ready的设置，Advance方法的实现。

#### 2.3.1 实现过程

1. `RawNode`、`Ready`结构的初始化

* RawNode的初始化按照文档与代码的说明实现即可，注意**保存之前的hardState与softState对象，用于判断是否存在更新。**
* Ready的初始化则是**根据RawNode中的hardState与softState与现在的hardState与softState是否相同**来进行更新，其他字段的设置则是从Raft结构体中得到。

2. `Advance`方法的实现逻辑
   
`Advance`方法在上层应用处理完 Ready 后调用，用于通知 Raft 节点可以继续处理下一批日志条目和消息。Advance 通常会**更新 RaftLog 的状态，清除已处理的日志条目和消息。**。
比较简单，根据Ready中的相关信息更新applied index, stabled index等等信息，推进节点RaftLog的相关信息即可（**持久化在storage中**）。

### 2.4 Project2B

本部分需要完成PeerStorage中的SaveReadyState函数：`peer_msg_handler.go`中的`proposeRaftCommand`函数与`HandleRaftReady`函数。

#### 2.4.1 实现过程

1.`SaveReadyState()`函数

SaveReadyState函数的作用是将Ready中的相关数据保存到数据库中，具体包括HardState、Entries（需要持久化的日志条目）、Snapshot（需要持久化的快照）。

* Append函数（日志的保存）

按照文档的教程，只需**将raft.Ready.Entries的所有日志条目保存到raftdb**，并删除所有无法提交的日志条目（为**RaftLog中Index大于entries[len(entries)-1].Index的那些日志**）。

* Raft HardState的保存

将ps.raftState.HardState写入到RaftDB中即可。

> 以上所有的写入，删除操作都是通过engine_util.WriteBatch的SetMeta/DeleteMeta完成的。

> 持久化：通过engine_util.WriteBatch的方法setMeta对RaftDB, KvDB（RaftDB存储着RaftLog与RaftLocalState）进行修改操作，注意Entry的Key为raft_log_key，State的key为raft_state_key（都可以通过meta下面的API得到）。

2. `proposeRaftCommand`函数

完成这个函数以及后面的函数需要对Request的执行流程有所了解。我查阅资料，看到了下面两张图，还是比较有帮助的。

![alt text](image-7.png)

![alt text](image-6.png)

具体的流程大概是：

在项目中，Server发送包含有各种请求信息的Request（包括Project1中的各种类型），通过RaftStorage封装一层成为Msg后传到`PeerMsgHandler`，PeerMsgHandler根据消息类型调用不同的处理方法，在本实验中要处理的是来自Client的请求（为MsgRaftCmd类型），**这个请求不能直接执行，需要先变为entry通过Propose方法提交到节点的RaftLog中去，运行Raft算法使entry提交之后**（这个Raft节点会返回一个Ready，它保存在Ready.CommittedEntries中），再在HandleRaftReady方法中对每个committed entry进行应用（Apply）操作，这时才是真正地相应Client的请求。、

> 在 Raft 中，propose 通常是由客户端发起的请求，目的是让 Raft 集群的领导者（leader）将一条新的日志条目加入到其日志中。这个新的日志条目通常包含一些客户端想要执行的操作，比如对数据库进行的更新、对状态机进行的命令等。

具体到此函数的实现，**此函数将上层命令Request打包成一个Entry发送给Raft节点**。

每当 peer 接收到一个 RaftCmdRequest 时，都会生成一个 proposal，每个 proposal 中都有一个 callback（用于在应用时返回Response）、Term 和 Index（都可以调用现有的方法得到，Index为新提出Entry的Index，Term为当前Raft节点的Term）。
序列化方法`Marshal()`与反序列化方法`UnMarshal()`已经提供了，请求之后就可以调用Raft节点的`Propose()`方法提出该日志了。

3. `HandleRaftReady`函数

这个函数处理RaftNode返回的Ready，包括**将unstableEntries写入raftDB（持久化），处理committedEnties里的操作（apply），还有向其他节点发送msgs里的消息**。
   
结合doc文档中的伪代码与资料可知，此函数主要的流程为：

* 调用SaveReadyState函数持久化有关Raft的状态
* 调用Send函数将ready中的msg消息发送到其他节点
* 将CommittedEntries进行Apply操作（执行Entry中保存的Request操作），并更新d.peerStorage.applyState
* 调用Advance方法

其他方法不多说了，重点说说将CommittedEntries进行Apply操作的流程。
现阶段我们需要处理的操作只有**Get、Put、Delete和Snap（对应Project1中的Scan操作）**。
首先对各个entry调用Unmarshal()反序列化为Msg，要处理的操作都在msg.Requests中。
对于每一个Request，如果是Put或者Delete操作，需要把Request中的请求传递到Storage中的kvDB去执行（调用WriteBatch中的相关函数即可）。
```Go
switch request.CmdType {
   case raft_cmdpb.CmdType_Put:
      wb.SetCF(request.Put.GetCf(), request.Put.GetKey(), request.Put.GetValue())
   case raft_cmdpb.CmdType_Delete:
      wb.DeleteCF(request.Delete.GetCf(), request.Delete.GetKey())
}
```
对于每一个Request，都要将 proposals 中的相应 Index 的 proposal 的回调函数 callback 传递对应操作类型的Response（调用`cb.Done()`函数）。
注意Get Response中要赋值Get操作得到的Value，Snap Response中要设置现在的Region。

注意，对于Snap操作，只需要返回一个包含正确Region的Response（包含事务对象）即可，除此之外**还需要在Callback中返回一个KvDB的只读事务对象Txn**，测试中会使用这个事务对象进行Scan操作的测试，不然会出现nil错误。

此外，还需要注意错误处理，本实验需要处理两个Error：
* ErrNotLeader：已经在preProposeRaftCommand中处理好了
* ErrStaleCommand：可能是由于领导者更改，某些日志未提交并被新领导者的日志覆盖，导致客户一直等待响应。这个错误可以通过比较Proposal的Term与Entry的Term是否相同检测出来。**如果不同，这个proposal就不处理了。**
* 处理错误也是通过调用cb.Done()传入错误。

#### 2.4.2 其他细节

1. `RaftLocalState`与`RaftApplyState`

RaftLocalState：存储 Raft 协议的持久化硬状态，包括最后的日志索引和任期，用于日志管理和恢复。**位于RaftKV**
RaftApplyState：存储状态机的持久化应用状态，包括已应用的日志索引和已截断日志的状态，用于确保日志不重复应用和维护日志的一致性。**位于KvDB**

```Go
// Used to store the persistent state for Raft, including the hard state for raft and the last index of the raft log.
message RaftLocalState {
    eraftpb.HardState hard_state = 1;
    uint64 last_index = 2;
    uint64 last_term = 3;
}

// Used to store the persistent state for Raft state machine.
message RaftApplyState {
    // Record the applied index of the state machine to make sure
    // not apply any index twice after restart.
    uint64 applied_index = 1;
    // Record the index and term of the last raft log that have been truncated. (Used in 2C)
    RaftTruncatedState truncated_state = 2; 
}
```
这些状态存储在两个 badger 实例中：raftdb 和 kvdb，它们各自负责保存的数据如下图：

![alt text](image-28.png)

#### 2.4.3 心得

a.先对于Request的处理机制有一定的了解再上手，这样会更加有思路
b.注意WriteBatch的使用方法：需要将Apply Entry与Apply State的写入都放到一个WriteBatch中，并且只在最后调用一次WriteToDB，不然会出现写入多次导致错误的情况。
c.多使用log.debug方法输出一些自己定义的辅助信息以帮助debug。

### 2.5 Project2C

本部分为快照的相关功能的完成。

快照是在节点日志超出一定限度之后保存了**最后日志的Index，Term与DB最后的状态**的数据结构。

RaftLogGcCountLimit是日志的最大条数，超过这个限制就需要生成Snapshot，日志范围是[ps.applyState.TruncatedState.Index + 1, ps.applyState.AppliedIndex]，即为\[上次生成快照的位置+1 ->  AppliedIndex\]

随后会生成一个新的AdminRequest类型`CompactLogRequest`，调用`proposeRaftCommand`函数，提醒节点需要生成一个新的快照。

#### 2.5.1 实现过程

首先是`peer_msg_handler.go`文件的相关修改：

1. `proposeRaftCommand`函数增加CompactLog命令的处理

与其他指令的处理逻辑类似，先进行序列化，再调用Raft节点的Propose函数，让节点保存到Entries中。

2. `processCommittedEntry`函数中增加CompactLogEntry的处理

在快照日志的Entry提交之后，需要修改RaftApplyState中对应的内容，使其与快照对应（TruncatedState的数据）。
最后调用`ScheduleCompactLog()`方法 向 raftlog-gc worker 安排一个任务，异步执行快照对应实际的日志删除工作。

随后是`raft.go`文件的相关修改，增加对快照的处理：

1. `sendAppend`函数中增加对snapshot的处理

Leader在此情况下需要进行发送快照：**在follower的NextEntry已经被自己压缩的情况下需要调用`sendSnapshot`函数发送快照**。
在`sendAppend`函数中增加这个逻辑的处理即可。

2. `sendSnapshot`函数

此函数发送快照信息给follower，通过调用`Storage.Snapshot()`函数得到快照，注意，由于**快照比较大，不能立即生成**，所以可能会出现`ErrSnapshotTemporarilyUnavailable`错误，这时候直接return就行，等待下一次的调用再尝试发送。

3. `handleSnapshot`函数

此函数处理收到的快照，快照信息的处理与Append信息的处理逻辑类似，这里不细说了。重点说说一些增加的判断逻辑：

* **如果 snapshot 的 Index 小于或等于当前已经提交的日志的 Index，说明这个 snapshot 已经过时**，拒绝
* 接收快照，则跟随者丢弃其整个日志，它全部被快照取代，操作包括：
a. 根据其中的 Metadata 来更新自己RaftLog的 committed、applied、stabled 等等信息
b. 丢弃整个日志
c. raftLog中pendingSnapshot的赋值（之后会保存到下一个Ready中）
d. 根据其中的 ConfState 更新自己的 Prs 信息

随后是`peer_storage.go`的相关函数的修改，增加快照的相关处理：

首先与Ready有关的函数`hasReady`与`Ready`中要增加与快照有关的逻辑：**hasReady更新快照存在的判断，Ready消息中加入快照。**

1. `SaveReadyState`函数的修改
如果Ready中存在调用，调用`ApplySnapshot`函数。

2. `ApplySnapshot`函数的实现
* 先调用`ps.clearMeta()`和`ps.clearExtraData()`来删除过时的数据（**要先调用`ps.isInitialized()`判断是否已经被初始化**）
* 再将快照应用到节点上：更新raftState、applyState和snapState中的对应数据，并持久化到数据库。
* 最后发送runner.RegionTaskApply任务到PeerStorage.regionSched。

3. `Advanced`函数
调用RaftLog的maybeCompact方法更新日志数组的第一个日志的Index，去除entries中包含在快照的部分。

#### 2.5.2 debug部分

以下为在本部分遇到的一些bug：
bug1：panic: can't get value 76313030 for key 6b313030 [recovered]
解决方法：这个错误是在本该应用Snapshot时没有完成应用，原因是忘记在生成Ready时加入新的snapshot了。

bug2：sendAppend在发送所有在prevLog之后的log需要先检查本节点日志的LastIndex是否大于prevLogIndex（follower紧邻新日志条目之前的那个日志条目的索引），如果不是，需要拒绝发送。

bug3：每次都会出现Request timeout问题，或者进度卡住：可以检查RaftLog中的快照更新相关函数（maybeCompact，lastIndex等等）的实现。
