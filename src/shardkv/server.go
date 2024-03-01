package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

func (kv *ShardKV) DPrintf0(format string, a ...interface{}) {
	//log.Printf(format, a...)
	str := fmt.Sprintf(format, a...)
	formattime := "2006-01-02 15:04:05.000"
	fmt.Printf("shardkvserver %d-%d %s -- %s\n", kv.gid, kv.me, time.Now().Format(formattime), str)
	return
}

const (
	OpPut    = iota
	OpAppend // 1
	OpGet    // 2
	OpReConfig
)

var mapOpString = map[int]string{
	OpPut:      "OpPut",
	OpAppend:   "OpAppend",
	OpGet:      "OpGet",
	OpReConfig: "OpReConfig",
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Optype int
	Key    string
	Value  string
	ShardState
	ClerkInfo ClerkSerial
}
type OpReturn struct {
	Err   Err
	Value string
}

type IndexInfo struct {
	retchn     chan OpReturn
	term       int
	index      int
	op         Op
	opret      OpReturn
	opretvalid bool
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	ctrlerck     *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead           int32 // set by Kill()
	mapclerkreqs   map[ClerkSerial]IndexInfo
	kv             map[string]string
	cleckserialnum map[int]int
	configs        []shardctrler.Config
}

type ShardState int

const (
	ShardStateInvalid ShardState = iota
	ShardStateStarted
	ShardStateDeleted // 1
)

type ShardIdInfo struct {
	ClerkToSerialNum map[int]int
	CurConfigNum     int
	ConfigState      map[int]ShardState // SardState
}

func NewShardInfo() *ShardIdInfo {
	return &ShardIdInfo{
		ClerkToSerialNum: make(map[int]int),
		ConfigState:      make(map[int]ShardState),
	}
}
func (si *ShardIdInfo) clone() *ShardIdInfo {
	ret := NewShardInfo()
	ret.CurConfigNum = si.CurConfigNum
	for k, v := range si.ClerkToSerialNum {
		ret.ClerkToSerialNum[k] = v
	}
	for k, v := range si.ConfigState {
		ret.ConfigState[k] = v
	}
	return ret
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

func (kv *ShardKV) Config(config2 shardctrler.Config) {
	config2.Clone()
}

func (kv *ShardKV) StartOp(op Op) OpReturn {
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

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	kv.DPrintf0("-----------------  kvserver i am dead mapclerkreqs size is %d ", len(kv.mapclerkreqs))
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) applychan() {
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
					kv.DPrintf0("apply channel index %d  %d-%d op: %s", apply.CommandIndex, p.ClerkInfo.ClerkId, p.ClerkInfo.SerialNum, mapOpString[p.Optype])

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
					kv.DPrintf0("go routinue  kv.applychan() exit ")
					return
				}
			}
		}
	}
}

// 不对 snapshot 过于频繁了
func (kv *ShardKV) snapshot(msg raft.ApplyMsg) {
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

func (kv *ShardKV) decodesnapshot(msg raft.ApplyMsg) {
	r := bytes.NewBuffer(msg.Snapshot)
	d := labgob.NewDecoder(r)
	kv.cleckserialnum = make(map[int]int, 16)
	kv.kv = make(map[string]string, 64)

	if d.Decode(&kv.cleckserialnum) != nil ||
		d.Decode(&kv.kv) != nil {
		panic("snapshot decode error")
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ShardIdInfo{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	// Use something like this to talk to the shardctrler:
	kv.ctrlerck = shardctrler.MakeClerk(kv.ctrlers)
	kv.kv = make(map[string]string, 64)
	kv.mapclerkreqs = make(map[ClerkSerial]IndexInfo, 64)
	kv.cleckserialnum = make(map[int]int, 16)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.PrintfPrefix = fmt.Sprintf("shardkvserver gid %d ", gid)

	go kv.applychan() // 确保kv.rf 赋值了，然后才执行 raft的恢复操作
	return kv
}
