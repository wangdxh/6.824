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
1 raft servers 承载多个 shards； group 中的raft servers 不会变更
```
ceph
```
pg0   --->   osd1  osd2  osd3
pg3   --->   osd2  osd5  osd6
pg5  ---->   osd1  osd2  osd4
1 osd server 承载多个 pg  每个pg 对应的副本server不尽相同 
所以一个osd上的多个pg协商时，需要osd和pg对应的其他osd进行交互
这一级，比6.824多一个复杂度,相当于一个pg 一个raft组；osd out时，相当于raft组中的servers还会变更
osd迁移时： 有 all participant 保存各个epoch中 osd列表， 类似于 raft 中的 clustermembership 变更
```


