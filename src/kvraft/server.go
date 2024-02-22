package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func (kv *KVServer) DPrintf(format string, a ...interface{}) {
	if Debug {
		//log.Printf(format, a...)
		str := fmt.Sprintf(format, a...)
		format := "2006-01-02 15:04:05.000"
		fmt.Printf("kvserver %d %s -- %s\n", kv.me, time.Now().Format(format), str)
	}
	return
}

const (
	OpPut    = iota
	OpAppend // 1
	OpGet    // 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Optype    int
	Key       string
	Value     string
	ClerkId   int
	SerialNum int
}

type OpReturn struct {
	Err   Err
	Value string
	//LeaderId int
	//Index    int
}

type IndexInfo struct {
	retchn chan OpReturn
	term   int
	op     Op
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mapindex map[int]IndexInfo

	kv             map[string]string
	cleckserialnum map[int]int
}

/*
kvserver 2 2024-02-20 19:27:49.078 --  ----------------- start i am dead
kvserver 2 2024-02-20 19:27:49.078 --  get apply {2 0 }
kvserver 2 2024-02-20 19:27:49.078 --  get apply {2 0 }
2 term 1 2024-02-20 19:27:49.079 -- ************************* i kill myself now over!!!!
kvserver 2 2024-02-20 19:27:49.079 --  ----------------- i am dead!!!

这里的锁 加上 raft appendentries 逻辑 造成一个死锁
*/

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// 直接执行查询？ 还是发起get ？ 如果发起get肯定没啥大问题
	if kv.killed() {
		reply.Err = "i am dead "
		return
	}

	op := Op{
		Optype:    OpGet,
		Key:       args.Key,
		ClerkId:   args.ClerkId,
		SerialNum: args.SerialNum,
	}

	ret := kv.StartOp(op)
	reply.Err = ret.Err
	reply.Value = ret.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = "i am dead "
		return
	}
	optype := OpPut
	if strings.Compare(args.Op, "Append") == 0 {
		optype = OpAppend
	}
	op := Op{
		Optype:    optype,
		Key:       args.Key,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		SerialNum: args.SerialNum,
	}
	ret := kv.StartOp(op)
	reply.Err = ret.Err
}

func (kv *KVServer) StartOp(op Op) OpReturn {
	retchn := make(chan OpReturn, 1)
	find := false
	index := -1

	kv.mu.Lock()
	for key, val := range kv.mapindex {
		if val.op == op {
			// 新的来到了，老的肯定超时了  如果已经commit了，但是没有发现，这里再次提交
			index = key
			find = true
			val.retchn = retchn
			kv.mapindex[key] = val
			break
		}
	}
	kv.mu.Unlock()

	if find {

	} else {
		_index, _term, ok := kv.rf.Start(op)
		kv.DPrintf(" call start return %v %v %v", _index, _term, ok)
		if ok == false {
			return OpReturn{Err: " i am not leader "}
		}
		kv.mu.Lock()
		kv.mapindex[_index] = IndexInfo{
			retchn: retchn,
			term:   _term,
			op:     op,
		}
		kv.mu.Unlock()
		index = _index
	}

	select {
	case ok := <-retchn:
		{
			return ok
		}
	case <-time.After((Clerk_Server_Timeout + 10) * time.Millisecond):
		{
			// 虽然有了超时，但是带来的问题是 当网络有问题的时候，发起的操作越来越多，raft这一级就直接返回了，这样会
			// 非常危险啊。。。。。。 超时处理没有问题，因为客户端已经等待超时，发起了下一次操作， 虽然raft 的网络不稳定
			// 但是 leader 依然会快速处理，所以 raft的leader 要有一个自动卸任的机制，否则这个start发起的频率太快了
			kv.mu.Lock()
			delete(kv.mapindex, index)
			kv.mu.Unlock()
			return OpReturn{Err: " time out putappend"}
		}
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	kv.DPrintf(" ----------------- start i am dead ")
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)

	kv.mu.Lock()
	for _, v := range kv.mapindex {
		ret := OpReturn{
			Err: Err("i am dead"),
		}
		v.retchn <- ret
	}
	kv.mapindex = make(map[int]IndexInfo, 64)
	kv.mu.Unlock()

	// 如果 raft 正在调用 appendentries，需要往里面写数据， 但是这边先把applychn 退出了， 那边退不出来，这边的kill锁也拿不到
	kv.DPrintf(" ----------------- i am dead!!!")
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applychan() {
	for {
		select {
		case apply, ok := <-kv.applyCh:
			{
				if ok == false || kv.killed() {
					return
				}
				kv.mu.Lock()
				termnow, isleader := kv.rf.GetState()
				if apply.CommandValid {
					val := ""
					p := (apply.Command).(Op)
					kv.DPrintf(" get apply %v ", p)

					if p.Optype == OpGet {
						val, _ = kv.kv[p.Key]
					} else {
						serialnum, has := kv.cleckserialnum[p.ClerkId]
						if !has || p.SerialNum > serialnum {
							kv.cleckserialnum[p.ClerkId] = p.SerialNum
							if p.Optype == OpPut {
								kv.kv[p.Key] = p.Value
							} else if p.Optype == OpAppend {
								val, ok := kv.kv[p.Key]
								if ok {
									kv.kv[p.Key] = val + p.Value
								} else {
									kv.kv[p.Key] = p.Value
								}
							}
						}

					}

					if inxinfo, ok := kv.mapindex[apply.CommandIndex]; ok {
						ret := OpReturn{
							Value: val,
						}
						if isleader {

						} else if inxinfo.term != termnow {
							ret.Err = Err(fmt.Sprintf(" i am not leader term diff %d from %d ", termnow, inxinfo.term))
						} else {
							panic(" this may can not happen")
						}
						inxinfo.retchn <- ret
						delete(kv.mapindex, apply.CommandIndex) // 发送结束后，就清除map
					}
					kv.snapshot(apply)
				} else if apply.SnapshotValid {
					kv.decodesnapshot(apply)
				}
				kv.mu.Unlock()
			}
		case <-time.After(100 * time.Millisecond):
			{
				if kv.killed() {
					kv.DPrintf("go routinue  kv.applychan() exit ")
					return
				}
			}
		}
	}
}

func (kv *KVServer) snapshot(msg raft.ApplyMsg) {
	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSizee() > (kv.maxraftstate-100) {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		err := e.Encode(kv.cleckserialnum)
		err = e.Encode(kv.kv)
		if err != nil {
			panic(" encode kvserver snapshot error")
		}
		kv.rf.Snapshot(msg.CommandIndex, w.Bytes())
	}
}

func (kv *KVServer) decodesnapshot(msg raft.ApplyMsg) {
	r := bytes.NewBuffer(msg.Snapshot)
	d := labgob.NewDecoder(r)
	kv.cleckserialnum = make(map[int]int, 16)
	kv.kv = make(map[string]string, 64)

	if d.Decode(&kv.cleckserialnum) != nil ||
		d.Decode(&kv.kv) != nil {
		panic("snapshot decode error")
	}
	// 将Map转换为切片类型
	keys := make([]int, len(kv.mapindex))
	i := 0
	for k := range kv.mapindex {
		keys[i] = k
		i++
	}
	// 按字母顺序对切片进行排序
	sort.Ints(keys)
	for i := range keys {
		if i <= msg.SnapshotIndex {
			if val, has := kv.mapindex[i]; has {
				retval := ""
				if val.op.Optype == OpGet {
					retval = kv.kv[val.op.Key]
				}
				ret := OpReturn{
					Value: retval,
				}
				val.retchn <- ret
			}
		} else {
			break
		}
	}
	// 发送结束后，就清除map
	for i := range keys {
		if i <= msg.SnapshotIndex {
			delete(kv.mapindex, i)
		} else {
			break
		}
	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.kv = make(map[string]string, 64)
	kv.mapindex = make(map[int]IndexInfo, 64)
	kv.cleckserialnum = make(map[int]int, 16)

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.DPrintf("make kvserver ")

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applychan() // 确保kv.rf 赋值了，然后才执行 raft的恢复操作

	return kv
}
