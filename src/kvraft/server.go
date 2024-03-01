package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func (kv *KVServer) DPrintf0(format string, a ...interface{}) {
	//log.Printf(format, a...)
	str := fmt.Sprintf(format, a...)
	formattime := "2006-01-02 15:04:05.000"
	fmt.Printf("kvserver %d %s -- %s\n", kv.me, time.Now().Format(formattime), str)
	return
}

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

const DELETE_MAPINDEX = false

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Optype    int
	Key       string
	Value     string
	ClerkInfo ClerkSerial
}

type OpReturn struct {
	Err   Err
	Value string
	//LeaderId int
	//Index    int
}

type IndexInfo struct {
	retchn     chan OpReturn
	term       int
	index      int
	op         Op
	opret      OpReturn
	opretvalid bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mapclerkreqs map[ClerkSerial]IndexInfo

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
		ClerkInfo: args.ClerkInfo,
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
		ClerkInfo: args.ClerkInfo,
	}
	ret := kv.StartOp(op)
	reply.Err = ret.Err
}

func (kv *KVServer) StartOp(op Op) OpReturn {
	retchn := make(chan OpReturn, 1)

	kv.mu.Lock()
	for key, val := range kv.mapclerkreqs {
		term, _ := kv.rf.GetState()
		if (val.op.ClerkInfo.ClerkId == op.ClerkInfo.ClerkId && val.op.ClerkInfo.SerialNum < op.ClerkInfo.SerialNum) ||
			val.term < term {
			delete(kv.mapclerkreqs, key)
		}
		// 某些情况下，首先leader 发起了start； 但是leader 挂了， 重新选举又中举了，但是后续没有其他的操作了，所以会导致
		// 上次start的操作，并不会进行commit，因为一个leader启动之后，只有在它的term内，发起了新的操作，才会commit 之前term内提交给它的数据
		// term 防不胜防啊
	}
	valhas := false
	index := -1
	if val, has := kv.mapclerkreqs[op.ClerkInfo]; has {
		valhas = has
		index = val.index
		if val.opretvalid {
			kv.DPrintf0(" startop call op ret valid %d-%d", op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum)
			kv.mu.Unlock()
			return val.opret
		}
		kv.DPrintf0(" startop call reset channel %d-%d", op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum)
		val.retchn = retchn
		kv.mapclerkreqs[op.ClerkInfo] = val
	}
	kv.mu.Unlock()

	if valhas {

	} else {
		_index, _term, ok := kv.rf.Start(op)
		if ok == false {
			return OpReturn{Err: " i am not leader "}
		}
		kv.DPrintf0(" startop call  %v %v isleader: %v  %d-%d ", _index, _term, ok, op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum)
		//这里的start 和 map 是分开的锁，会出现start 刚结束，这里未继续，那边已经applychn 成功了
		kv.mu.Lock()
		index = _index
		if val, has := kv.mapclerkreqs[op.ClerkInfo]; has && val.opretvalid {
			kv.DPrintf0(" startop return %d-%d ok: true index: %d", op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum, index)
			kv.mu.Unlock()
			return val.opret
		} else {
			kv.mapclerkreqs[op.ClerkInfo] = IndexInfo{
				retchn: retchn,
				index:  _index,
				term:   _term,
				op:     op,
			}
			kv.mu.Unlock()
		}
	}

	select {
	case ok := <-retchn:
		{
			kv.DPrintf0(" startop return %d-%d err: %s  isok: %v index: %d", op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum,
				ok.Err, len(ok.Err) == 0, index)
			return ok
		}
	case <-time.After((Clerk_Server_Timeout + 10) * time.Millisecond):
		{
			kv.DPrintf0(" startop return %d-%d timeout index: %d ", op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum, index)
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
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	kv.DPrintf0("-----------------  kvserver i am dead mapclerkreqs size is %d ", len(kv.mapclerkreqs))
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
					kv.DPrintf0("apply channel index %d  %d-%d", apply.CommandIndex, p.ClerkInfo.ClerkId, p.ClerkInfo.SerialNum)

					if p.Optype == OpGet {
						val, _ = kv.kv[p.Key]
					} else {
						serialnum, has := kv.cleckserialnum[p.ClerkInfo.ClerkId]
						if !has || p.ClerkInfo.SerialNum > serialnum {
							kv.cleckserialnum[p.ClerkInfo.ClerkId] = p.ClerkInfo.SerialNum
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

					ret := OpReturn{
						Value: val,
					}
					if inxinfo, ok := kv.mapclerkreqs[p.ClerkInfo]; ok {
						if inxinfo.opretvalid == false && isleader {
							/*if inxinfo.term != termnow {
								ret.Err = Err(fmt.Sprintf(" may i am not leader term diff %d from %d ", termnow, inxinfo.term))
								inxinfo.retchn <- ret
								delete(kv.mapclerkreqs, p.ClerkInfo) // ?
							} else
							if isleader {*/
							inxinfo.opretvalid = true
							inxinfo.opret = ret
							kv.mapclerkreqs[p.ClerkInfo] = inxinfo
							/*} else {
								panic(" this may can not happen")
							}*/
							inxinfo.retchn <- ret
						}
					} else if isleader {
						//重启的时候，变成了leader 可能会发生 map index 不存在的情况
						inxin := IndexInfo{
							opretvalid: true,
							opret:      ret,
							term:       termnow,
							index:      apply.CommandIndex,
							op:         p,
							retchn:     make(chan OpReturn, 1),
						}
						kv.mapclerkreqs[p.ClerkInfo] = inxin
					}
					kv.snapshot(apply)
				} else if apply.SnapshotValid {
					kv.DPrintf0("apply channel snapshot index %d  term %d", apply.SnapshotIndex, apply.SnapshotTerm)
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

// 不对 snapshot 过于频繁了
func (kv *KVServer) snapshot(msg raft.ApplyMsg) {
	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSizee() > kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		err := e.Encode(kv.cleckserialnum)
		err = e.Encode(kv.kv)
		if err != nil {
			panic(" encode kvserver snapshot error")
		}
		//kv.DPrintf0(" snapshot index: %d ", msg.CommandIndex)
		kv.rf.Maxraftstate = kv.maxraftstate
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
	kv.mapclerkreqs = make(map[ClerkSerial]IndexInfo, 64)
	kv.cleckserialnum = make(map[int]int, 16)

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 8)
	kv.DPrintf("make kvserver ")

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applychan() // 确保kv.rf 赋值了，然后才执行 raft的恢复操作

	return kv
}

/*
可以优化的部分1
raft snapshot的时机不太对 正常在操作，虽然commit 了 38 ， 之后可以snapshot
但是此时 server2  38 还没有回应， 这里就做了snapshot， 但是snapshot的触发时机，好像论文里并没有明确指定
逻辑上倒是没啥问题

kvserver 0 2024-02-24 01:09:07.777 -- apply channel index 36  4-37
kvserver 0 2024-02-24 01:09:07.777 --  snapshot index: 36
kvserver 0 2024-02-24 01:09:07.777 -- apply channel index 37  4-38
kvserver 0 2024-02-24 01:09:07.777 --  snapshot index: 37
kvserver 1 2024-02-24 01:09:07.777 -- apply channel index 38  4-39
kvserver 1 2024-02-24 01:09:07.777 --  snapshot index: 38
kvserver 1 2024-02-24 01:09:07.777 --  startop return 4-39 err:   isok: true index: 38
kvserver 1 2024-02-24 01:09:07.777 --  startop call  39 1 isleader: true  4-40
kvserver 2 2024-02-24 01:09:07.777 -- apply channel snapshot index 38  term 1
kvserver 1 2024-02-24 01:09:07.777 -- apply channel index 39  4-40
kvserver 1 2024-02-24 01:09:07.777 --  startop return 4-40 err:   isok: true index: 39
kvserver 1 2024-02-24 01:09:07.777 --  startop call  40 1 isleader: true  4-41

*/
/*
可以优化的部分2
leader 的卸任，可以主动触发， 不然kvserver这里发起时，需要操作一下
*/

/*
可以优化的部分3 ok
mapclerkreqs 使用clerkid _ serialnum 作为key 一开始没有引进这两个参数，导致有点跑偏了
index 和raft 关系太紧密，这样应该使用 clerk 的唯一信息来判断
*/

/*
可以优化的部分4  ok
snapshot 降低频率 kvserver 配合 raft
*/
