package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	//OK             = "OK"
	OK                  = ""
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrCongifNumTooSall = "ECongifNumTooSall"
)

type Err string

type MetaInfo struct {
	ClerkId   int
	SerialNum int
	ShardId   int
	ConfigNum int
}

func (meta *MetaInfo) ClerkReq() ClerkReq {
	return ClerkReq{
		ClerkId:   meta.ClerkId,
		SerialNum: meta.SerialNum,
		ShardId:   meta.ShardId,
	}
}

type ClerkReq struct {
	ClerkId   int
	SerialNum int
	ShardId   int
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkInfo MetaInfo
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkInfo MetaInfo
}

type GetReply struct {
	Err   Err
	Value string
}

type ProposeShardArgs struct {
	Kv            map[string]string
	ClerkSerial   map[int]int
	ShardId       int
	Gidfrom       int
	Confignumfrom int
	Gidto         int
	Confignumto   int
}

type ProposeShardReply struct {
	Err      Err
	IsLeader bool
}

/*type CommitShardArgs struct {
	ShardId       int
	Gidfrom       int
	Confignumfrom int
	Gidto         int
	Confignumto   int
}
type CommitShardReply struct {
	Err Err
}*/
