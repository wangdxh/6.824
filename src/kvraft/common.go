package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type ClerkSerial struct {
	ClerkId   int
	SerialNum int
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkInfo ClerkSerial
}

type PutAppendReply struct {
	Err Err
	//LeaderId int
}

type GetArgs struct {
	Key       string
	ClerkInfo ClerkSerial
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
	//LeaderId int
}

const Clerk_Server_Timeout = 300
