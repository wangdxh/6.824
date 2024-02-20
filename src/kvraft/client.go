package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkid    int
	serialnum  int
	lastleader int
	mu         sync.Mutex
}

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
	ck.serialnum = 1

	fmt.Printf(" makeclerk %d  servers %d \r\n", ck.clerkid, len(servers))
	G_mu.Unlock()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// 不需要加锁
	for i := ck.lastleader; i < len(ck.servers); {
		args := GetArgs{
			Key: key,
		}
		reply := GetReply{}
		//ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		ok := ck.sendRPC(i, "KVServer.Get", &args, &reply, 30)
		if ok {
			if len(reply.Err) == 0 {
				ck.lastleader = i
				ck.DPrintf(" get key %s reutrn %s ", key, reply.Value)
				return reply.Value
			}
		}
		i++
		i = i % len(ck.servers)
	}
	panic(" here can not happen")
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.serialnum += 1
	nextserialnum := ck.serialnum
	ck.mu.Unlock()

	for i := ck.lastleader; i < len(ck.servers); {
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClerkId:   ck.clerkid,
			SerialNum: nextserialnum,
		}
		reply := PutAppendReply{}

		ck.DPrintf(" send to %d ", i)
		//ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		ok := ck.sendRPC(i, "KVServer.PutAppend", &args, &reply, 300)
		ck.DPrintf(" server %d  return  %v  ", i, reply)
		if ok {
			if len(reply.Err) == 0 {
				ck.lastleader = i
				ck.DPrintf(" PutAppend key %s value %s reutrn ", key, value)
				return
			}
		}
		i++
		i = i % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
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
