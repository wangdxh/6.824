package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"fmt"
	"sync"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	ctrlerclerk *shardctrler.Clerk
	config      shardctrler.Config
	make_end    func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	clerkid       int
	serialnum     int
	gidlastleader map[int]int // gid to index
	mu            sync.Mutex
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
var G_CLERK_ID int = 0
var G_mu sync.Mutex

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.ctrlerclerk = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	// You'll have to add code here.
	ck.gidlastleader = make(map[int]int)
	G_mu.Lock()
	G_CLERK_ID += 1
	ck.clerkid = G_CLERK_ID
	ck.serialnum = 0
	fmt.Printf("shardkv makeclerk %d \r\n", ck.clerkid)
	G_mu.Unlock()
	return ck
}

func (ck *Clerk) getlastleader(gid int) int {
	val := ck.gidlastleader[gid]
	return val
}

func (ck *Clerk) getNextSerialnum() int {
	ck.mu.Lock()
	ck.serialnum += 1
	nextserialnum := ck.serialnum
	ck.mu.Unlock()
	return nextserialnum
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClerkInfo = MetaInfo{
		ClerkId:   ck.clerkid,
		SerialNum: ck.getNextSerialnum(),
		ShardId:   key2shard(key),
	}

	for {
		if ck.config.Num > 0 {
			args.ClerkInfo.ConfigNum = ck.config.Num
			shard := key2shard(key)
			gid := ck.config.Shards[shard]
			if servers, ok := ck.config.Groups[gid]; ok {
				// try each server for the shard.
				lastleader := ck.getlastleader(gid)
				for si := lastleader; si < len(servers); {
					var reply GetReply
					ok := ck.sendRPC(servers[si], "ShardKV.Get", &args, &reply, Clerk_Server_Timeout)

					if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
						ck.gidlastleader[gid] = si
						return reply.Value
					}
					if ok && (reply.Err == ErrWrongGroup) {
						ck.DPrintf(" return ErrWrongGroup ")
						break
					}
					// ... not ok, or ErrWrongLeader
					si += 1
					si %= len(servers)
					if si == lastleader { // 一轮了， 都没有返回成功
						time.Sleep(100 * time.Millisecond)
					}
					//fmt.Printf(" get 0 %s  err %s \r\n", key, reply.Err)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		//fmt.Printf(" get 1 %s config num %d \r\n", key, ck.config.Num)
		// ask controler for the latest configuration.
		ck.config = ck.ctrlerclerk.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClerkInfo = MetaInfo{
		ClerkId:   ck.clerkid,
		SerialNum: ck.getNextSerialnum(),
		ShardId:   key2shard(key),
	}
	for {
		if ck.config.Num > 0 {
			args.ClerkInfo.ConfigNum = ck.config.Num
			shard := key2shard(key)
			gid := ck.config.Shards[shard]
			if servers, ok := ck.config.Groups[gid]; ok {
				// try each server for the shard.
				lastleader := ck.getlastleader(gid)
				for si := lastleader; si < len(servers); {
					var reply PutAppendReply
					ok := ck.sendRPC(servers[si], "ShardKV.PutAppend", &args, &reply, Clerk_Server_Timeout)

					if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
						ck.gidlastleader[gid] = si
						return
					}
					if ok && (reply.Err == ErrWrongGroup) {
						break
					}
					// ... not ok, or ErrWrongLeader
					si++
					si %= len(servers)
					if si == lastleader { // 一轮了， 都没有返回成功
						time.Sleep(100 * time.Millisecond)
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.ctrlerclerk.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendRPC(server string, rpccall string, args interface{}, reply interface{}, timeout int64) bool {
	retchn := make(chan bool)
	stopchn := make(chan int)
	defer func() {
		close(stopchn)
	}()

	go func(server string) {
		start := time.Now()
		ok := ck.make_end(server).Call(rpccall, args, reply)
		timems := time.Now().UnixMilli() - start.UnixMilli()
		ck.DPrintf("send to server %s rpc %s return %v  reply: %v  speed %d ms isobselete %v",
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
