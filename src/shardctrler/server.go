package shardctrler

import (
	"6.824/raft"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Optype int
	// 可以串行化，这里就先不实现了
	QueryArgs
	MoveArgs
	JoinArgs
	LeaveArgs
	ClerkInfo ClerkSerial
}

type OpReturn struct {
	Err   Err
	Value Config
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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead           int32 // set by Kill()
	mapclerkreqs   map[ClerkSerial]IndexInfo
	configs        []Config // indexed by config num
	cleckserialnum map[int]int
	DebugLevel     int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	if sc.killed() {
		reply.Err = "i am dead "
		return
	}
	optype := JOIN
	op := Op{
		Optype:    optype,
		ClerkInfo: args.ClerkInfo,
		JoinArgs:  *args,
	}
	ret := sc.StartOp(op)
	reply.Err = ret.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	if sc.killed() {
		reply.Err = "i am dead "
		return
	}
	optype := LEAVE
	op := Op{
		Optype:    optype,
		ClerkInfo: args.ClerkInfo,
		LeaveArgs: *args,
	}
	ret := sc.StartOp(op)
	reply.Err = ret.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	if sc.killed() {
		reply.Err = "i am dead "
		return
	}
	optype := MOVE
	op := Op{
		Optype:    optype,
		ClerkInfo: args.ClerkInfo,
		MoveArgs:  *args,
	}
	ret := sc.StartOp(op)
	reply.Err = ret.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	if sc.killed() {
		reply.Err = "i am dead "
		return
	}
	optype := QUERY
	op := Op{
		Optype:    optype,
		ClerkInfo: args.ClerkInfo,
		QueryArgs: *args,
	}
	ret := sc.StartOp(op)
	reply.Err = ret.Err
	reply.Config = ret.Value
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	atomic.StoreInt32(&sc.dead, 1)
	sc.DPrintf0("-----------------  sc i am dead mapclerkreqs size is %d ", len(sc.mapclerkreqs))
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.mapclerkreqs = make(map[ClerkSerial]IndexInfo, 64)
	sc.cleckserialnum = make(map[int]int, 16)
	sc.DebugLevel = 0 // 1  0 关闭 strctl 的debug print

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.rf.DebugLevel = sc.DebugLevel

	sc.rf.PrintfPrefix = "shardctrler "

	go sc.applychan() // 确保kv.rf 赋值了，然后才执行 raft的恢复操作
	// Your code here.
	sc.DPrintf0("make shardctrler server ")
	return sc
}

func (sc *ShardCtrler) StartOp(op Op) OpReturn {
	retchn := make(chan OpReturn, 1)

	sc.mu.Lock()
	for key, val := range sc.mapclerkreqs {
		term, _ := sc.rf.GetState()
		if (val.op.ClerkInfo.ClerkId == op.ClerkInfo.ClerkId && val.op.ClerkInfo.SerialNum < op.ClerkInfo.SerialNum) ||
			val.term < term {
			delete(sc.mapclerkreqs, key)
		}
		// 某些情况下，首先leader 发起了start； 但是leader 挂了， 重新选举又中举了，但是后续没有其他的操作了，所以会导致
		// 上次start的操作，并不会进行commit，因为一个leader启动之后，只有在它的term内，发起了新的操作，才会commit 之前term内提交给它的数据
		// term 防不胜防啊
	}
	valhas := false
	index := -1
	if val, has := sc.mapclerkreqs[op.ClerkInfo]; has {
		valhas = has
		index = val.index
		if val.opretvalid {
			sc.DPrintf0(" startop call op ret valid %d-%d", op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum)
			sc.mu.Unlock()
			return val.opret
		}
		sc.DPrintf0(" startop call reset channel %d-%d", op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum)
		val.retchn = retchn
		sc.mapclerkreqs[op.ClerkInfo] = val
	}
	sc.mu.Unlock()

	if valhas {

	} else {
		_index, _term, ok := sc.rf.Start(op)
		if ok == false {
			return OpReturn{Err: " i am not leader "}
		}
		sc.DPrintf0(" startop call  %v %v isleader: %v  %d-%d ", _index, _term, ok, op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum)
		//这里的start 和 map 是分开的锁，会出现start 刚结束，这里未继续，那边已经applychn 成功了
		sc.mu.Lock()
		index = _index
		if val, has := sc.mapclerkreqs[op.ClerkInfo]; has && val.opretvalid {
			sc.DPrintf0(" startop return %d-%d ok: true index: %d", op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum, index)
			sc.mu.Unlock()
			return val.opret
		} else {
			sc.mapclerkreqs[op.ClerkInfo] = IndexInfo{
				retchn: retchn,
				index:  _index,
				term:   _term,
				op:     op,
			}
			sc.mu.Unlock()
		}
	}

	select {
	case ok := <-retchn:
		{
			sc.DPrintf0(" startop return %d-%d err: %s  isok: %v index: %d", op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum,
				ok.Err, len(ok.Err) == 0, index)
			return ok
		}
	case <-time.After((Clerk_Server_Timeout + 10) * time.Millisecond):
		{
			sc.DPrintf0(" startop return %d-%d timeout index: %d ", op.ClerkInfo.ClerkId, op.ClerkInfo.SerialNum, index)
			return OpReturn{Err: " time out putappend"}
		}
	}

}

func (sc *ShardCtrler) applychan() {
	for {
		select {
		case apply, ok := <-sc.applyCh:
			{
				if ok == false || sc.killed() {
					return
				}
				sc.mu.Lock()
				termnow, isleader := sc.rf.GetState()
				if apply.CommandValid {
					//If the number is -1 or bigger than the biggest known configuration number, the shardctrler should reply with the latest configuration.
					val := sc.configs[len(sc.configs)-1]
					p := (apply.Command).(Op)
					sc.DPrintf0("apply channel index %d  %d-%d  op: %s", apply.CommandIndex, p.ClerkInfo.ClerkId, p.ClerkInfo.SerialNum, maprpccall[p.Optype])

					if p.Optype == QUERY {
						for _, cfg := range sc.configs {
							if cfg.Num == p.QueryArgs.Num {
								val = cfg
							}
						}
					} else {
						serialnum, has := sc.cleckserialnum[p.ClerkInfo.ClerkId]
						if !has || p.ClerkInfo.SerialNum > serialnum {
							sc.cleckserialnum[p.ClerkInfo.ClerkId] = p.ClerkInfo.SerialNum
							if p.Optype == MOVE {
								sc.configs = append(sc.configs, MoveConfig(&sc.configs[len(sc.configs)-1], p.MoveArgs.Shard, p.MoveArgs.GID))
							} else if p.Optype == JOIN {
								sc.configs = append(sc.configs, AddConfig(&sc.configs[len(sc.configs)-1], p.JoinArgs.Servers))
							} else if p.Optype == LEAVE {
								sc.configs = append(sc.configs, LeaveConfig(&sc.configs[len(sc.configs)-1], p.LeaveArgs.GIDs))
							}
						}
					}

					ret := OpReturn{
						Value: val,
					}
					if inxinfo, ok := sc.mapclerkreqs[p.ClerkInfo]; ok {
						if inxinfo.opretvalid == false && isleader {
							inxinfo.opretvalid = true
							inxinfo.opret = ret
							sc.mapclerkreqs[p.ClerkInfo] = inxinfo
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
						sc.mapclerkreqs[p.ClerkInfo] = inxin
					}
					//sc.snapshot(apply)
				} else if apply.SnapshotValid {
					panic("no snapshot in shardctrler")
					//sc.decodesnapshot(apply)
				}
				sc.mu.Unlock()
			}
		case <-time.After(100 * time.Millisecond):
			{
				if sc.killed() {
					sc.DPrintf0("go routinue  sc.applychan() exit ")
					return
				}
			}
		}
	}
}
func (sc *ShardCtrler) DPrintf0(format string, a ...interface{}) {
	//log.Printf(format, a...)
	if sc.DebugLevel > 0 {
		str := fmt.Sprintf(format, a...)
		formattime := "2006-01-02 15:04:05.000"
		fmt.Printf("shardctrler server %d %s -- %s\n", sc.me, time.Now().Format(formattime), str)
		return
	}
}
