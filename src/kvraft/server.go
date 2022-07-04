package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const TimeoutInterval = 500 * time.Millisecond

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	serversLen   int
	maxraftstate int // snapshot if log grows this big

	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	kv    map[string]string
	dedup map[int32]interface{}
	get   map[int]chan string
	done  map[int]chan struct{}

	lastApplied int

	// Your definitions here.
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	var dedup map[int32]interface{}
	var kvmap map[string]string
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&dedup); e != nil {
		dedup = make(map[int32]interface{})
	}
	if e := d.Decode(&kvmap); e != nil {
		kvmap = make(map[string]string)
	}

	kv.dedup = dedup
	kv.kv = kvmap
}

func (kv *KVServer) DoApply() {
	for v := range kv.applyCh {
		if kv.killed() {
			return
		}

		if v.CommandValid {
			kv.apply(v)
			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}
			kv.mu.Lock()
			switch args := v.Command.(type) {
			case GetArgs:
				getCh := kv.get[v.CommandIndex]
				val := ""
				if s, ok := kv.kv[args.Key]; ok {
					val = s
				}
				kv.mu.Unlock()
				go func() {
					getCh <- val
				}()
				break
			case PutAppendArgs:
				putCh := kv.done[v.CommandIndex]
				kv.mu.Unlock()
				go func() {
					putCh <- struct{}{}
				}()
			}
		} else {
			b := kv.rf.CondInstallSnapshot(v.SnapshotTerm, v.SnapshotIndex, v.Snapshot)
			DPrintf("CondInstallSnapshot %t SnapshotTerm %d SnapshotIndex %d len(Snapshot) %d", b, v.SnapshotTerm, v.SnapshotIndex, len(v.Snapshot))
			if b {
				kv.lastApplied = v.SnapshotIndex
				kv.readSnapshot(v.Snapshot)
			}
		}
	}
}

func (kv *KVServer) apply(v raft.ApplyMsg) {
	if v.CommandIndex <= kv.lastApplied {
		return
	}
	var key string
	switch args := v.Command.(type) {
	case GetArgs:
		key = args.Key
		kv.lastApplied = v.CommandIndex
		break
	case PutAppendArgs:
		key = args.Key
		if dup, ok := kv.dedup[args.ClientId]; ok {
			if putDup, ok := dup.(PutAppendArgs); ok && putDup.RequestId == args.RequestId {
				DPrintf("deplicate found for putDup=%+v args=%+v", putDup, args)
				break
			}
		}
		if args.Type == PutOp {
			kv.kv[args.Key] = args.Value
		} else {
			kv.kv[args.Key] += args.Value
		}
		kv.dedup[args.ClientId] = v.Command
		kv.lastApplied = v.CommandIndex
	}
	DPrintf("applied {%d %+v} value :%s", v.CommandIndex, v.Command, kv.kv[key])
	if kv.rf.GetStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		if err := e.Encode(kv.dedup); err != nil {
			panic(err)
		}
		if err := e.Encode(kv.kv); err != nil {
			panic(err)
		}
		kv.rf.Snapshot(v.CommandIndex, w.Bytes())
	}

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := *args
	i, _, isLeader := kv.rf.Start(op)
	DPrintf("raft start Get log idx=%d %+v", i, op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan string, 1)
	kv.mu.Lock()
	kv.get[i] = ch
	kv.mu.Unlock()
	select {
	case v := <-ch:
		reply.Value = v
		reply.Err = OK
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeOut
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := *args
	i, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan struct{}, 1)
	kv.mu.Lock()
	kv.done[i] = ch
	kv.mu.Unlock()
	select {
	case <-ch:
		DPrintf("raft Put done:%+v", op)
		reply.Err = OK
		return
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeOut
		return
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.serversLen = len(servers)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.get = make(map[int]chan string)
	kv.done = make(map[int]chan struct{})
	kv.readSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.DoApply()

	return kv
}
