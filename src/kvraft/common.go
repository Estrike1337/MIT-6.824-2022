package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

type Args struct {
	ClientId  int32
	RequestId int64
}

type PutOrAppend int

const(
	PutOp PutOrAppend = iota
	AppendOp
)

// Put or Append
type PutAppendArgs struct {
	Type  PutOrAppend
	Key   string
	Value string
	Args
	// "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
