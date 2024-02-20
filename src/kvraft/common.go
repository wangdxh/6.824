package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId   int
	SerialNum int
}

type PutAppendReply struct {
	Err Err
	//LeaderId int
}

type GetArgs struct {
	Key string
	//ClerkId   int
	//SerialNum int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
	//LeaderId int
}
