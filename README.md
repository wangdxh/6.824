# mit 6.824 raft


You must write all the code you hand in for 6.824, except for code that we give you as part of assignments. 
You are not allowed to look at anyone else's solution, and you are not allowed to look at solutions from previous years. 
<br>**除了我们作为作业的一部分提供给您的代码外，您必须编写6.824的所有代码。
您不允许查看其他人的解决方案，也不允许查看前几年的解决方案。**

```
git clone git://g.csail.mit.edu/6.824-golabs-2022 6.824
```


## lab2
http://nil.csail.mit.edu/6.824/2022/labs/lab-raft.html

## lab3
http://nil.csail.mit.edu/6.824/2022/labs/lab-kvraft.html

## lab4
http://nil.csail.mit.edu/6.824/2022/labs/lab-shard.html

### shardkv
6.824
```
每个group的raft 3个副本server， 协商这些切片的key values；  
shard0
shard3 --->      1group ---> 3 raft servers   
shard5
1 raft servers 承载多个 shards  只有 raft 副本协商状态
```
ceph
```
pg0   --->   osd1  osd2  osd3
pg3   --->   osd2  osd5  osd6
pg5  ---->   osd1  osd2  osd4
1 osd server 承载多个 pg  每个pg 对应的副本server不尽相同 
所以一个osd上的多个pg协商时，需要osd和pg对应的其他osd进行交互
这一级，比6.824多一个复杂度
```
一个server上承载多个切片时，shardid + key 的组织形式，以及shard迁移过程一致性保障，6.824和ceph是逻辑等价的。


>6.824 迁移：
> 
> * Within a single replica group, all group members must agree on when a reconfiguration occurs relative to client Put/Append/Get requests. 
> For example, a Put may arrive at about the same time as a reconfiguration that causes the replica group to stop being responsible for the shard holding the Put's key. 
> All replicas in the group must agree on whether the Put occurred before or after the reconfiguration. 
> If before, the Put should take effect and the new owner of the shard will see its effect; 
> if after, the Put won't take effect and client must re-try at the new owner. 
> The recommended approach is to have each replica group use Raft to log not just the sequence of Puts, Appends, and Gets but also the sequence of reconfigurations. 
> You will need to ensure that at most one replica group is serving requests for each shard at any one time.
> <br>所有组成员必须就相对于客户端Put/Append/Get请求的重新配置发生时间达成一致。 
> <br> 例如，Put可能与重新配置同时到达，重新配置会导致副本组停止负责持有Put密钥的碎片。组中的所有复制副本必须就Put发生在重新配置之前还是之后达成一致。
如果之前，Put应该生效，碎片的新所有者将看到它的效果；如果之后，Put将不会生效，客户必须重新尝试新的所有者。
><br>  **建议的方法:** 是让每个副本组使用Raft不仅记录Puts、Appends和Gets的序列，还记录重新配置的序列。
您需要确保在任何时候最多有一个副本组为每个碎片提供请求。
> 
> 
> * This lab uses "configuration" to refer to the assignment of shards to replica groups. This is not the same as Raft cluster membership changes. You don't have to implement Raft cluster membership changes.
> <br>这个实验室使用“配置”指的是将切片分配给副本组。这与Raft集群成员身份的更改不同。您不必实现Raft集群成员身份更改。
> 
> 
> *  the lab doesn't evolve the sets of peers in each Raft group; its data and query models are very simple; and handoff of shards is slow and doesn't allow concurrent client access.<br>本实验不会演化/修改每个Raft组中的对等体集合；它的数据和查询模型非常简单；并且碎片的切换很慢，切换时不允许并发客户端访问。
> 
> 
> * 

