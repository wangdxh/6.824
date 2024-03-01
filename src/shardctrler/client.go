package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"fmt"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clerkid    int
	serialnum  int
	lastleader int
	mu         sync.Mutex
}

const Debug = false
const Clerk_Server_Timeout = 300

func (ck *Clerk) DPrintf(format string, a ...interface{}) {
	if Debug {
		//log.Printf(format, a...)
		str := fmt.Sprintf(format, a...)
		format := "2006-01-02 15:04:05.000"
		fmt.Printf("clerk %d %s -- %s\n", ck.clerkid, time.Now().Format(format), str)
	}
	return
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var G_CLERK_ID int = 0
var G_mu sync.Mutex

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	G_mu.Lock()
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	G_CLERK_ID += 1
	ck.clerkid = G_CLERK_ID
	ck.serialnum = 0

	fmt.Printf("shardctrler makeclerk %d  servers %d \r\n", ck.clerkid, len(servers))
	G_mu.Unlock()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClerkInfo.ClerkId = ck.clerkid
	args.ClerkInfo.SerialNum = ck.getNextSerialnum()
	var reply QueryReply
	ck.sendMsg(QUERY, &args, &reply, Clerk_Server_Timeout)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClerkInfo.ClerkId = ck.clerkid
	args.ClerkInfo.SerialNum = ck.getNextSerialnum()
	var reply JoinReply
	ck.sendMsg(JOIN, &args, &reply, Clerk_Server_Timeout)
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClerkInfo.ClerkId = ck.clerkid
	args.ClerkInfo.SerialNum = ck.getNextSerialnum()
	var reply LeaveReply
	ck.sendMsg(LEAVE, &args, &reply, Clerk_Server_Timeout)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClerkInfo.ClerkId = ck.clerkid
	args.ClerkInfo.SerialNum = ck.getNextSerialnum()

	var reply MoveReply
	ck.sendMsg(MOVE, &args, &reply, Clerk_Server_Timeout)
}
func (ck *Clerk) getNextSerialnum() int {
	ck.mu.Lock()
	ck.serialnum += 1
	nextserialnum := ck.serialnum
	ck.mu.Unlock()
	return nextserialnum
}

const (
	QUERY = iota + 1
	MOVE
	JOIN
	LEAVE
)

var maprpccall = map[int]string{
	QUERY: "ShardCtrler.Query",
	MOVE:  "ShardCtrler.Move",
	JOIN:  "ShardCtrler.Join",
	LEAVE: "ShardCtrler.Leave",
}

func (ck *Clerk) getErrInfo(rpccall int, reply interface{}) (Err, bool) {
	if rpccall == QUERY {
		ret := reply.(*QueryReply)
		return ret.Err, ret.WrongLeader
	} else if rpccall == MOVE {
		ret := reply.(*MoveReply)
		return ret.Err, ret.WrongLeader
	} else if rpccall == JOIN {
		ret := reply.(*JoinReply)
		return ret.Err, ret.WrongLeader
	} else if rpccall == LEAVE {
		ret := reply.(*LeaveReply)
		return ret.Err, ret.WrongLeader
	} else {
		panic(" can not happen")
	}
}

func (ck *Clerk) clearreply(rpccall int, reply interface{}) {
	if rpccall == QUERY {
		ret := reply.(*QueryReply)
		ret.Err = ""
		ret.WrongLeader = false
		ret.Config = Config{}
	} else if rpccall == MOVE {
		ret := reply.(*MoveReply)
		ret.Err = ""
		ret.WrongLeader = false
	} else if rpccall == JOIN {
		ret := reply.(*JoinReply)
		ret.Err = ""
		ret.WrongLeader = false
	} else if rpccall == LEAVE {
		ret := reply.(*LeaveReply)
		ret.Err = ""
		ret.WrongLeader = false
	} else {
		panic(" can not happen")
	}
}

func (ck *Clerk) sendMsg(rpccall int, args interface{}, reply interface{}, timeout int64) {
	leader := ck.lastleader
	for i := leader; i < len(ck.servers); {
		// labgob warning: Decoding into a non-default variable/field Err may not work
		ck.clearreply(rpccall, reply)

		ok := ck.sendRPC(i, maprpccall[rpccall], args, reply, timeout)
		if ok {
			err, wrongleader := ck.getErrInfo(rpccall, reply)

			if len(err) == 0 && wrongleader == false {
				ck.lastleader = i
				return
			}
		}
		i++
		i = i % len(ck.servers)
		if i == leader {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) sendRPC(server int, rpccall string, args interface{}, reply interface{}, timeout int64) bool {
	retchn := make(chan bool)
	stopchn := make(chan int)
	defer func() {
		close(stopchn)
	}()

	go func(server int) {
		start := time.Now()
		ok := ck.servers[server].Call(rpccall, args, reply)
		timems := time.Now().UnixMilli() - start.UnixMilli()
		ck.DPrintf("send to server %d rpc %s return %v  reply: %v  speed %d ms isobselete %v",
			server, rpccall, ok, reply, timems, timems > timeout)
		select {
		case <-stopchn:
			{
				return
			}
		case retchn <- ok:
			{
				return
			}
		}
	}(server)

	select {
	case ok := <-retchn:
		{
			return ok
		}
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		{
			return false
		}
	}
}
