package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
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
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mapindex       map[int]IndexInfo
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
	retchn := make(chan OpReturn, 1)
	{
		op := Op{
			Optype: OpGet,
			Key:    args.Key,
		}
		_index, _term, ok := kv.rf.Start(op)
		if ok == false {
			reply.Err = " i am not leader "
			return
		}
		// 这里用一个什么来等待？ index 和 chan 来等待吧
		kv.mu.Lock()
		kv.mapindex[_index] = IndexInfo{
			retchn: retchn,
			term:   _term,
		}
		kv.mu.Unlock()
	}
	select {
	case ok := <-retchn:
		{
			reply.Err = ok.Err
			reply.Value = ok.Value
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	retchn := make(chan OpReturn, 1)
	{
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
		_index, _term, ok := kv.rf.Start(op)
		kv.DPrintf(" call start return %v %v %v", _index, _term, ok)
		if ok == false {
			reply.Err = " i am not leader "
			return
		}
		kv.mu.Lock()
		kv.mapindex[_index] = IndexInfo{
			retchn: retchn,
			term:   _term,
		}
		kv.mu.Unlock()
	}
	select {
	case ok := <-retchn:
		{
			reply.Err = ok.Err
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
					}
				} else if apply.SnapshotValid {

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
	kv.DPrintf("make kvserver ")

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kv = make(map[string]string, 64)
	kv.mapindex = make(map[int]IndexInfo, 64)
	kv.cleckserialnum = make(map[int]int, 4)

	go kv.applychan()

	return kv
}
