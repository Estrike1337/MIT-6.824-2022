package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

func Max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	applyMu   sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    int
	log         []*Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCond      *sync.Cond
	replicateCond  []*sync.Cond

	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

const (
	ElectionTimeoutMax = int64(600 * time.Millisecond)
	ElectionTimeoutMin = int64(500 * time.Millisecond)
	HeartbeatInterval  = 100 * time.Millisecond
)

func NewElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Int63n(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin)
}

func (rf *Raft) Majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) beCandidate() {
	rf.electionTimer.Reset(NewElectionTimeout())
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	fmt.Printf("peer %v become candidate term:%v\n", rf.me, rf.currentTerm)

}

func (rf *Raft) beLeader() {
	rf.state = Leader
	rf.electionTimer.Reset(NewElectionTimeout())
	rf.nextIndex = make([]int, len(rf.peers))
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = LogTail(rf.log).Index + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchIndex[rf.me] = LogTail(rf.log).Index
	fmt.Printf("peer %v become leader term:%v\n", rf.me, rf.currentTerm)
	rf.SendHeartbeat()
	rf.heartbeatTimer.Reset(HeartbeatInterval)
}

func (rf *Raft) beFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
	fmt.Printf("peer %v become follower term:%v\n", rf.me, rf.currentTerm)

}

func (rf *Raft) LogHead() *Log {
	return rf.log[0]
}

func (rf *Raft) GetLogAtIndex(index int) *Log {
	if index < rf.LogHead().Index {
		return nil
	}
	subscript := index - rf.LogHead().Index
	if len(rf.log) > subscript {
		return rf.log[subscript]
	}

	return nil
}

func (rf *Raft) StateData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	err := e.Encode(rf.log)
	if err != nil {
		fmt.Printf("encode log err")
	}
	err = e.Encode(rf.currentTerm)
	if err != nil {
		fmt.Printf("encode log err")
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		fmt.Printf("encode log err")
	}

	return w.Bytes()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	var isLeader bool
	isLeader = rf.state == Leader
	rf.mu.Unlock()
	return rf.currentTerm, isLeader
}

func(rf *Raft) GetStateSize()int{
	return rf.persister.RaftStateSize()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.StateData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	fmt.Printf("peer %v try boost\n", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var votedFor int
	log := make([]*Log, 0)
	var currentTerm int

	if err := d.Decode(&log); err != nil {
		fmt.Printf("decode failed error:%v\n", err)
	}

	if err := d.Decode(&currentTerm); err != nil {
		fmt.Printf("decode failed error:%v\n", err)

	}

	if err := d.Decode(&votedFor); err != nil {
		fmt.Printf("decode failed error:%v\n", err)

	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.commitIndex = rf.LogHead().Index
	rf.lastApplied = rf.LogHead().Index
}

func LogTail(l []*Log) *Log {
	return l[len(l)-1]
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.applyMu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	fmt.Printf("peer %v install snapshot index %v\n", rf.me, lastIncludedIndex)
	if lastIncludedIndex > LogTail(rf.log).Index {
		rf.log = make([]*Log, 1)
		rf.log[0] = &Log{
			Term:    0,
			Index:   0,
			Command: nil,
		}
	} else {
		//fmt.Printf("peer %v logHeadIdx %v logs %+v\n", rf.me, rf.LogHead().Index, rf.log)
		logIdx := rf.LogHead().Index
		for idx := range rf.log[:lastIncludedIndex-logIdx] {
			rf.log[idx] = nil
		}
		//fmt.Printf("peer %v logs %+v\n", rf.me, rf.log)
		rf.log = append([]*Log{}, rf.log[lastIncludedIndex-logIdx:]...)
		//rf.LogHead().Command = nil
	}
	rf.LogHead().Term, rf.LogHead().Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.StateData(), snapshot)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.LogHead().Index

	if index <= snapshotIndex || index > LogTail(rf.log).Index {
		return
	}
	//fmt.Printf("peer %v logs %+v\n", rf.me, rf.log)
	for idx := range rf.log[:index-snapshotIndex] {
		rf.log[idx] = nil
	}

	rf.log = rf.log[index-snapshotIndex:]
	//rf.log[0].Command = nil

	data := rf.StateData()
	fmt.Printf("peer %v snapshot %v\n", rf.me, index)
	//fmt.Printf("peer %v logHeadIdx %v logs %+v\n", rf.me, rf.LogHead().Index, rf.log)
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

func (rf *Raft) SendHeartbeat() {
	installSnapshotArgs := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LogHead().Index,
		LastIncludedTerm:  rf.LogHead().Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.state != Leader {
			return
		}
		if rf.nextIndex[i] <= rf.LogHead().Index {
			go func(x int) {
				installSnapshotReply := &InstallSnapshotReply{}
				ok := rf.sendInstallSnapshot(x, installSnapshotArgs, installSnapshotReply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm != installSnapshotArgs.Term {
						return
					}

					if rf.currentTerm < installSnapshotReply.Term {
						rf.beFollower(installSnapshotReply.Term)
						rf.electionTimer.Reset(NewElectionTimeout())
						return
					}
					rf.nextIndex[x] = installSnapshotArgs.LastIncludedIndex + 1
				}
			}(i)
			continue
		}

		prev := rf.GetLogAtIndex(rf.nextIndex[i] - 1)
		entries := make([]*Log, 0)
		if rf.nextIndex[i] <= LogTail(rf.log).Index {
			//给落后的follower复制日志
			//fmt.Printf("leader %v term %v send log %v to peer %v\n", rf.me, rf.currentTerm, rf.nextIndex[x], x)
			entries = append([]*Log{}, rf.log[rf.nextIndex[i]-rf.LogHead().Index:]...)
		}
		appendEntriesArgs := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prev.Index,
			PrevLogTerm:  prev.Term,
			LeaderCommit: rf.commitIndex,
			Entries:      entries,
		}
		//广播心跳包
		go func(x int) {
			appendEntriesReply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(x, appendEntriesArgs, appendEntriesReply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if appendEntriesArgs.Term != rf.currentTerm {
					//过期的回复
					return
				}

				if appendEntriesReply.Term > rf.currentTerm {
					rf.beFollower(appendEntriesReply.Term)
					rf.electionTimer.Reset(NewElectionTimeout())

					return
				}

				if appendEntriesReply.Success {
					if len(appendEntriesArgs.Entries) == 0 {
						return
					}
					//fmt.Printf("leader %v term %v receive success send log %v from peer %v \n", rf.me, rf.currentTerm, rf.nextIndex[x], x)
					rf.nextIndex[x] = Max(rf.nextIndex[x], LogTail(appendEntriesArgs.Entries).Index+1)
					rf.matchIndex[x] = Max(rf.matchIndex[x], LogTail(appendEntriesArgs.Entries).Index)

					//检查是否可提交
					for idx := rf.commitIndex; idx <= LogTail(rf.log).Index; idx++ {
						count := 0
						for p := range rf.peers {
							if rf.matchIndex[p] >= idx {
								count += 1
								//fmt.Printf("leader %v term %v log %v vote count %v\n", rf.me, rf.currentTerm, i, count)
							}
						}

						if count >= rf.Majority() && rf.GetLogAtIndex(idx).Term == rf.currentTerm {
							rf.commitIndex = idx
							//fmt.Printf("leader %v commit log %v\n", rf.me, rf.commitIndex)
							//fmt.Printf("leader %v term %v commit log %v\n", rf.me, rf.currentTerm, rf.commitIndex)
						}
					}
					//应用到状态机
					if rf.commitIndex > rf.lastApplied {
						rf.applyCond.Broadcast()
					}
					return
				}

				//日志冲突
				nextIndex := rf.nextIndex[x]

				//目标索引有日志，但是任期冲突
				if appendEntriesReply.ConflictingTerm > 0 {
					for idx := len(rf.log) - 1; idx >= 1; idx-- {
						if rf.log[idx].Term == appendEntriesReply.ConflictingTerm {
							rf.nextIndex[x] = Min(nextIndex, rf.log[idx].Index+1)
							return
						}
					}
				}
				//
				rf.nextIndex[x] = Max(Min(nextIndex, appendEntriesReply.ConflictingIndex), 1)
			}
		}(i)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	//
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if LogTail(rf.log).Term < args.LastLogTerm {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.electionTimer.Reset(NewElectionTimeout())
			return
		}
		if LogTail(rf.log).Term == args.LastLogTerm {
			reply.VoteGranted = LogTail(rf.log).Index <= args.LastLogIndex
			if reply.VoteGranted {
				rf.votedFor = args.CandidateId
				rf.persist()
				rf.electionTimer.Reset(NewElectionTimeout())
			}
			return
		}
		return

	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []*Log
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingIndex int
	ConflictingTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		//领导人的任期小于接受者当前任期
		return
	}

	if rf.currentTerm < args.Term {
		rf.beFollower(args.Term)
		rf.electionTimer.Reset(NewElectionTimeout())

	}

	if rf.state == Candidate && args.Term >= rf.currentTerm {
		rf.beFollower(args.Term)
		rf.electionTimer.Reset(NewElectionTimeout())

	}
	rf.electionTimer.Reset(NewElectionTimeout())

	if args.PrevLogIndex > 0 && args.PrevLogIndex >= rf.LogHead().Index {
		//日志一致性检查
		if prev := rf.GetLogAtIndex(args.PrevLogIndex); prev == nil || prev.Term != args.PrevLogTerm {
			fmt.Printf("peer %v conflict log %v\n", rf.me, args.PrevLogIndex)
			//日志发生冲突
			if prev != nil {
				//索引有日志
				for _, entry := range rf.log {
					if entry.Term == prev.Term {
						reply.ConflictingIndex = entry.Index
						break
					}
				}
				reply.ConflictingTerm = prev.Term
			} else {
				//索引没日志，试试尾部
				reply.ConflictingIndex = LogTail(rf.log).Index
				reply.ConflictingTerm = 0
			}

			return
		}
	}

	if len(args.Entries) > 0 {
		//fmt.Printf("peer %v receive log %v from leader %v term %v\n", rf.me, args.PrevLogIndex+1, args.LeaderId, args.Term)
		//非心跳包,复制日志
		if LogTail(args.Entries).Index > rf.LogHead().Index {
			//防止过时rpc截断log
			appendLeft := 0
			for i, entry := range args.Entries {
				if local := rf.GetLogAtIndex(entry.Index); local != nil {

					if local.Index != entry.Index {
						panic("LMP violated")
					}

					if local.Term != entry.Term {
						rf.log = rf.log[:entry.Index-rf.LogHead().Index]
						appendLeft = i
						break
					}
					//appendLeft之前的都无冲突
					appendLeft = i + 1
				}
			}
			//if appendLeft < len(args.Entries) {
			//	rf.log = append(rf.log, args.Entries[appendLeft:]...)
			//}
			for i := appendLeft; i < len(args.Entries); i++ {
				entry := args.Entries[i]
				rf.log = append(rf.log, entry)
			}
			//fmt.Printf("peer %v conflict resolve log %v\n", rf.me, jsonhelper.JsonExpr(rf.log))
		}
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		//提交日志
		rf.commitIndex = Min(args.LeaderCommit, LogTail(rf.log).Index)
		fmt.Printf("peer %v commit log %v\n", rf.me, rf.commitIndex)
		rf.applyCond.Broadcast()
	}

	reply.Success = true
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.electionTimer.Reset(NewElectionTimeout())

	if args.Term > rf.currentTerm || rf.state == Candidate {
		rf.beFollower(args.Term)
	}

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyMu.Lock()
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.electionTimer.Reset(NewElectionTimeout())
				rf.mu.Unlock()
				break
			}
			rf.doElection()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.SendHeartbeat()
				rf.heartbeatTimer.Reset(HeartbeatInterval)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) doElection() {
	rf.beCandidate()

	count := 1
	voteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: LogTail(rf.log).Index,
		LastLogTerm:  LogTail(rf.log).Term,
	}

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			//fmt.Printf("peer %v voted for peer %v\n", rf.me, rf.me)
			continue
		}

		go func(x int) {
			voteReply := &RequestVoteReply{}
			isOk := rf.sendRequestVote(x, voteArgs, voteReply)
			if isOk {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Candidate || rf.currentTerm != voteArgs.Term {
					//过期的回复
					return
				}

				//新leader来了
				if voteReply.Term > rf.currentTerm {
					rf.beFollower(voteReply.Term)
					rf.electionTimer.Reset(NewElectionTimeout())
					return
				}

				if voteReply.VoteGranted {
					fmt.Printf("peer %v voted for peer %v\n", x, rf.me)
					count++
					if count >= rf.Majority() {
						rf.beLeader()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) doApply() {
	for rf.killed() == false {
		rf.mu.Lock()

		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		rf.mu.Unlock()

		//lastAppIndex := rf.GetLogAtIndex(rf.lastApplied).Index - rf.LogHead().Index
		//commitIndex := rf.commitIndex - rf.LogHead().Index
		//
		//c := rf.commitIndex
		//
		//entries := make([]*Log, commitIndex-lastAppIndex)
		//copy(entries, rf.log[lastAppIndex+1:commitIndex+1])
		//fmt.Printf("peer %v apply entries log %v\n", rf.me, LogTail(entries).Index)

		for {
			rf.mu.Lock()
			if rf.commitIndex == rf.lastApplied {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()

			rf.applyMu.Lock()

			rf.mu.Lock()
			if rf.commitIndex <= rf.lastApplied {
				rf.applyMu.Unlock()
				continue
			}
			rf.lastApplied += 1
			entry := rf.GetLogAtIndex(rf.lastApplied)
			if entry == nil {
				panic("entry nil")
			}

			toCommit := *entry

			fmt.Printf("peer %v apply log %v\n", rf.me, entry.Index)

			rf.mu.Unlock()

			rf.applyCh <- ApplyMsg{
				Command:      toCommit.Command,
				CommandValid: true,
				CommandIndex: toCommit.Index,
			}
			rf.applyMu.Unlock()

		}

		//for _, entry := range entries {
		//	//fmt.Printf("peer %v log %v\n", rf.me, jsonhelper.JsonExpr(rf.log))
		//	rf.mu.Lock()
		//	if entry.Index < rf.lastApplied {
		//		fmt.Printf("old log conflict snapshot")
		//		rf.mu.Unlock()
		//		continue
		//	}
		//	rf.mu.Unlock()
		//	rf.applyCh <- ApplyMsg{
		//		Command:      entry.Command,
		//		CommandValid: true,
		//		CommandIndex: entry.Index,
		//	}
		//}

	}

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	log := &Log{
		Term:    rf.currentTerm,
		Index:   LogTail(rf.log).Index + 1,
		Command: command,
	}
	//fmt.Printf("leader %v get command %v\n", rf.me, log.Index)

	rf.log = append(rf.log, log)
	rf.persist()

	rf.matchIndex[rf.me] += 1

	rf.SendHeartbeat()

	return log.Index, log.Term, rf.state == Leader
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		votedFor:       -1,
		currentTerm:    0,
		electionTimer:  time.NewTimer(NewElectionTimeout()),
		heartbeatTimer: time.NewTimer(HeartbeatInterval),
	}
	rf.mu.Lock()
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.log = append(rf.log, &Log{
		Term:    0,
		Index:   0,
		Command: nil,
	})

	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.doApply()

	return rf
}
