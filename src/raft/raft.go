package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
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
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type Entry struct {
	Index int
	Term  int
	/*Key   string
	Value []byte*/
	Command interface{}
}

const (
	RoleFollower  int = iota // 0
	RoleLeader               // 1
	RoleCandidate            // 2
)
const (
	DEBUGELECTION       = 1 << 1 // 0
	DEBUGLOGREPLICATION = 1 << 2 // 1
	DEBUGSNAPSHOT       = 1 << 3 // 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	MyTerm           int // currentTerm in paper
	VotedFor         int
	role             int
	CommitIndex      int // index of highest log entry known to be committed
	LastAppliedIndex int // index of highest log entry applied to state machine
	Logs             []Entry
	nextIndex        []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex       []int

	applyChn        chan ApplyMsg
	electionTimeout time.Time
	leaderTimeout   time.Time
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.MyTerm = 0
	rf.CommitIndex = 0
	rf.LastAppliedIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.VotedFor = -1
	rf.role = RoleFollower
	rf.applyChn = applyCh

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.MyTerm, rf.role == RoleLeader
}
func (rf *Raft) Print() {
	fmt.Printf(" %d term %d isleader %v commitedindex: %d  applyiedindex: %d logslen: %d ",
		rf.me, rf.MyTerm, rf.role == RoleLeader, rf.CommitIndex, rf.LastAppliedIndex, len(rf.Logs))
	if len(rf.Logs) > 0 {
		fmt.Printf(" first: %v end: %v all %v ", rf.Logs[0], rf.Logs[len(rf.Logs)-1], rf.Logs)
	}
	fmt.Printf("\n")
}

// return logs len, index, term
func (rf *Raft) lastLog() (int, int, int) {
	inx := 0
	term := 0
	x := len(rf.Logs)
	if x > 0 {
		inx = rf.Logs[x-1].Index
		term = rf.Logs[x-1].Term
	}
	return x, inx, term
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {

	// Your code here (2C).
	// Example:
	start := time.Now()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.MyTerm)
	err = e.Encode(rf.CommitIndex)
	err = e.Encode(rf.LastAppliedIndex)
	err = e.Encode(rf.VotedFor)
	err = e.Encode(rf.Logs)
	if err != nil {
		panic(" err := e.Encode(rf) : " + err.Error())
		return
	}
	// e.Encode(rf.yyy)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	end := time.Now()
	timelen := end.UnixMilli() - start.UnixMilli()
	if timelen > 5 {
		rf.MyPrintf(DEBUGLOGREPLICATION, "------------------------------------------persist use %d ms", timelen)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var commitindex int
	var applyiedindex int
	var logs []Entry
	var votedfor int
	err := d.Decode(&term)
	err = d.Decode(&commitindex)
	err = d.Decode(&applyiedindex)
	err = d.Decode(&votedfor)
	err = d.Decode(&logs)
	if err != nil {
		panic(" decode rf error ")
	}
	rf.MyTerm = term
	rf.CommitIndex = commitindex
	rf.LastAppliedIndex = applyiedindex
	rf.Logs = logs
	rf.VotedFor = votedfor
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A, 2B)
	reply.VoteGranted = false
	reply.Term = rf.MyTerm
	if args.Term < rf.MyTerm {
		return
	}
	if args.Term > rf.MyTerm {
		//If RPC request or response contains Term T > currentTerm: set currentTerm = T, convert to follower (§5.1
		rf.MyTerm = args.Term
		rf.role = RoleFollower
		reply.Term = rf.MyTerm
		rf.VotedFor = -1
	}
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		//If votedFor is null or candidateId, and candidate’s log is at
		//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		l, mylastinx, mylastterm := rf.lastLog()
		if l == 0 ||
			//If the logs have last entries with different terms, then the log with the later term is more up-to-date.
			//If the logs end with the same term, then whichever log is longer is more up-to-date.
			(args.LastLogTerm > mylastterm) ||
			(args.LastLogTerm == mylastterm && args.LastLogIndex >= mylastinx) {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.extendElectionTimeout()
		}
	}
}

// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

/*
1 term 1 2024-01-30 22:02:41.508 --  initNowIAmLeader

1 term 1 2024-01-30 22:02:41.509 --  start go routine sendlog 2
0 term 1 2024-01-30 22:02:41.508 --  initNowIAmLeader
这里有个状态差， 开始 election 的时候，可能已经选了别人 加锁
*/
func (rf *Raft) dealElectionTimeout() {
	rf.mu.Lock()
	_, lastloginx, lastlogterm := rf.lastLog()
	// 超时时间没到呢，返回
	if rf.electionTimeout.After(time.Now()) {
		rf.mu.Unlock()
		return
	} else {
		rf.MyTerm += 1
		rf.role = RoleCandidate
		rf.VotedFor = rf.me
	}
	rf.persist()
	rf.extendElectionTimeout()
	rf.mu.Unlock()

	rf.MyPrintf(DEBUGLOGREPLICATION, "start election %d  %d", rf.electionTimeout.UnixMilli(), time.Now().UnixMilli())
	// 不能创建带缓冲的，否则 在reply 返回之后select的时候，即使stop已经关闭了，依然会写成功，导致打印输出
	retchn := make(chan RequestVoteReply)
	stopchn := make(chan int)

	for inx, _ := range rf.peers {
		if inx == rf.me {
			continue
		}
		// 如果网络不通，rpc 返回不及时，这个定时器会造成，协程发送过多
		go func(inx int, retchn chan RequestVoteReply, stopchn chan int) {
			req := RequestVoteArgs{
				Term:         rf.MyTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastloginx,
				LastLogTerm:  lastlogterm,
			}
			reply := RequestVoteReply{}
			start := time.Now()
			ret := rf.sendRequestVote(inx, &req, &reply)
			if ret {
				select {
				case <-stopchn:
					{
						return
					}
				case retchn <- reply:
					{
						end := time.Now()
						rf.MyPrintf(DEBUGLOGREPLICATION, "send request to %d vote spend %d ms ret %v reply granted %v replyterm %d",
							inx, end.UnixMilli()-start.UnixMilli(), ret, reply.VoteGranted, reply.Term)

						if end.UnixMilli()-start.UnixMilli() > 300 {
							panic(" dealElectionTimeout can not happen")
						}
						return
					}
				}
			}

		}(inx, retchn, stopchn)
	}

	//oknums := 0 sb了
	defer func() {
		close(stopchn)
		rf.extendElectionTimeout()
	}()

	deadline := time.Now().Add(200 * time.Millisecond).UnixMilli()

	oknums := 1
	for {
		millilen := deadline - time.Now().UnixMilli()
		if millilen < 0 {
			return
		}
		select {
		case reply := <-retchn:
			{
				if reply.VoteGranted {
					oknums++
					if oknums >= len(rf.peers)/2+1 {
						rf.role = RoleLeader
						rf.MyPrintf(DEBUGLOGREPLICATION, " become leader %d term %d\n", rf.me, rf.MyTerm)
						rf.initNowIAmLeader()
						return
					}
				} else if reply.Term > rf.MyTerm {
					rf.role = RoleFollower
					rf.MyTerm = reply.Term
					return
				}
			}
		case <-time.After(time.Duration(millilen) * time.Millisecond):
			{
				return
			}
		}
	}

}

func (rf *Raft) initNowIAmLeader() {
	rf.MyPrintf(DEBUGLOGREPLICATION, " initNowIAmLeader")
	rf.leaderTimeout = time.UnixMilli(0)
	_, logindex, _ := rf.lastLog()
	for inx, _ := range rf.peers {
		if inx == rf.me {
			continue
		}
		rf.nextIndex[inx] = logindex + 1
		rf.matchIndex[inx] = 0
		go rf.dealLogReplication(inx, rf.MyTerm)
	}
}

type AppendEntries struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
	// 论文7页底，8页头部分
	ReplicaLogIndex int
	ReplicaLogTerm  int
}

// example RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	// Your code here (2A, 2B)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.MyTerm
	if args.Term < rf.MyTerm {
		return
	}
	rf.MyPrintf(DEBUGLOGREPLICATION, "getappendentry from %d term %d previndex %d prevterm %d leadercommit %d entryies %d ",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))

	if args.Term >= rf.MyTerm {
		//If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
		//then the candidate recognizes the leader as legitimate and returns to follower state
		rf.MyTerm = args.Term
		rf.role = RoleFollower
		reply.Term = rf.MyTerm
		rf.VotedFor = -1
		rf.persist()
	}
	if len(args.Entries) == 0 {
		reply.Success = true
	} else {
		if args.PrevLogIndex == 0 {
			rf.Logs = rf.Logs[:0]
			rf.Logs = append(rf.Logs, args.Entries...)
		} else {
			//2. Reply false if log doesn’t contain an entry at prevLogIndex
			//whose term matches prevLogTerm (§5.3)
			previnlog := -1
			mylogindex, mylogterm := 0, 0
			for i, val := range rf.Logs {
				if val.Term == args.PrevLogTerm && mylogindex == 0 {
					mylogindex = val.Index
					mylogterm = val.Term
				}
				if val.Index == args.PrevLogIndex && val.Term == args.PrevLogTerm {
					previnlog = i
					break
				}
			}
			if previnlog == -1 {
				//when rejecting an AppendEntries request, the follower
				//can include the term of the conflicting entry and the first index it stores for that term.
				reply.ReplicaLogIndex = mylogindex
				reply.ReplicaLogTerm = mylogterm
				return
			}
			//3. If an existing entry conflicts with a new one (same index
			//but different terms), delete the existing entry and all that follow it (§5.3)
			i := previnlog + 1
			j := 0
			for i < len(rf.Logs) && j < len(args.Entries) {
				if rf.Logs[i].Index != args.Entries[j].Index ||
					rf.Logs[i].Term != args.Entries[j].Term {
					rf.Logs = rf.Logs[:i] // delete i and all follow it
					break
				}
				i++
				j++
			}
			//4. Append any new entries not already in the log
			_, lastinx, _ := rf.lastLog()
			for _, val := range args.Entries {
				if val.Index > lastinx {
					rf.Logs = append(rf.Logs, val)
				}
			}
			rf.persist()
		}
		rf.MyPrintf(DEBUGLOGREPLICATION, "replica logs: %v %v %v", len(rf.Logs), rf.Logs[0], rf.Logs[len(rf.Logs)-1])
		reply.Success = true
	}
	rf.updateReplicaCommitIndex(args)
	rf.updateAppliedIndex()
	rf.extendElectionTimeout()
}
func (rf *Raft) updateReplicaCommitIndex(args *AppendEntries) {
	// 5. If leaderCommit > commitIndex, set commitIndex =
	//			min(leaderCommit, index of last new entry)
	// 这里增加一个额外的限制，必须保证已经收到了最新的term的请求，确保新的leader上 所有的数据被同步过来了
	// 这个条件的保证，就是收到了一条新leader 新term上写的数据
	_, lastlogindex, lastlogterm := rf.lastLog()
	if lastlogterm == args.Term && rf.CommitIndex < args.LeaderCommit {
		if args.LeaderCommit > lastlogindex {
			rf.CommitIndex = lastlogindex
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
	}
}

func (rf *Raft) updateAppliedIndex() {
	if rf.CommitIndex > rf.LastAppliedIndex {
		for _, val := range rf.Logs {
			if val.Index > rf.LastAppliedIndex {
				if val.Index <= rf.CommitIndex {
					// apply this
					start := time.Now()
					rf.applyChn <- ApplyMsg{
						CommandValid:  true,
						Command:       val.Command,
						CommandIndex:  val.Index,
						SnapshotValid: false,
					}
					end := time.Now()
					if end.UnixMilli()-start.UnixMilli() > 20 {
						rf.MyPrintf(DEBUGLOGREPLICATION, "write to apply chn too lang use %d ms  ********************************************  \n",
							end.UnixMilli()-start.UnixMilli())
					}
				} else {
					break
				}
			}
		}
		rf.LastAppliedIndex = rf.CommitIndex
		rf.MyPrintf(DEBUGLOGREPLICATION, " update applyiedindex to %d ", rf.LastAppliedIndex)
		// commit 更新，apply 一定会更新
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) dealHeartBeatTimeout() {
	if rf.role == RoleLeader {
		for inx, _ := range rf.peers {
			if inx == rf.me {
				continue
			}
			//_, lastloginx, lastlogterm := rf.lastLog()
			go func(inx int) {
				args := AppendEntries{
					Term:         rf.MyTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.CommitIndex,
					//PrevLogIndex: lastloginx,
					//PrevLogTerm:  lastlogterm,
				}
				reply := AppendEntriesReply{}
				ret := rf.sendAppendEntries(inx, &args, &reply)
				if ret && reply.Term > rf.MyTerm {
					rf.role = RoleFollower
					rf.MyTerm = reply.Term
					return
				}
			}(inx)
		}
	}
}

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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if RoleLeader != rf.role {
		return -1, -1, false
	}
	_, index, term := rf.lastLog()
	if rf.MyTerm < term {
		panic(fmt.Sprintf(" term error  myterm %d lastlogterm %d \n", rf.MyTerm, term))
	}
	index += 1
	term = rf.MyTerm

	rf.Logs = append(rf.Logs, Entry{
		Index:   index,
		Term:    term,
		Command: command,
	})
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.MyPrintf(DEBUGLOGREPLICATION, " logs: %v %v %v", len(rf.Logs), rf.Logs[0], rf.Logs[len(rf.Logs)-1])

	//rf.persist() 这里肯定不能串行化啊，耗时太高了，影响了其他消息的发送
	return index, term, rf.role == RoleLeader
}

func (rf *Raft) dealLogReplication(topeer int, nowmyterm int) {
	rf.MyPrintf(DEBUGLOGREPLICATION, " start go routine sendlog %d ", topeer)
	for rf.killed() == false {
		time.Sleep(time.Millisecond * time.Duration(10))

		//If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		for {
			if RoleLeader != rf.role || nowmyterm != rf.MyTerm {
				goto out // 新的协程会继续起来
			}
			entries := AppendEntries{
				Term:         rf.MyTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.CommitIndex,
			}

			rf.mu.Lock()
			loglen, loglastindex, _ := rf.lastLog()
			if loglen > 0 && loglastindex >= rf.nextIndex[topeer] {
				//第一条数据时，PrevLogIndex 为0
				entries.PrevLogIndex = rf.nextIndex[topeer] - 1
				for _, val := range rf.Logs {
					if val.Index == entries.PrevLogIndex {
						entries.PrevLogTerm = val.Term
					}
					MaxEntriesOneAppend := 100
					if val.Index >= rf.nextIndex[topeer] && len(entries.Entries) < MaxEntriesOneAppend {
						entries.Entries = append(entries.Entries, val)
					}
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
				break
			}

			if len(entries.Entries) == 0 {
				panic(" len(entries.Entries) can not be 0")
			}

			reply := AppendEntriesReply{}
			bok := rf.sendAppendEntries(topeer, &entries, &reply)
			if false == bok {
				break // 失败之后，延迟再发，不着急
			} else {
				if reply.Success {
					rf.mu.Lock()
					// If successful: update nextIndex and matchIndex for follower
					rf.nextIndex[topeer] = entries.Entries[len(entries.Entries)-1].Index + 1
					rf.matchIndex[topeer] = entries.Entries[len(entries.Entries)-1].Index
					rf.updateLeaderCommitIndex()
					rf.updateAppliedIndex()
					rf.mu.Unlock()
				} else {
					//rf.nextIndex[topeer]--
					rf.nextIndex[topeer] = reply.ReplicaLogIndex - 1
					if rf.nextIndex[topeer] <= 0 {
						rf.nextIndex[topeer] = rf.Logs[0].Index
					}
					continue
				}
			}
		}
	}
out:
	rf.MyPrintf(DEBUGLOGREPLICATION, " end go routine sendlog %d ", topeer)
}

// 更新 leader的commit index
// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
func (rf *Raft) updateLeaderCommitIndex() {
	checkcommit := make([]int, len(rf.peers))
	for i, _ := range rf.peers {
		checkcommit[i] = rf.matchIndex[i]
	}
	sort.Ints(checkcommit)
	quorum := len(rf.peers)/2 + 1 - 1 // 3个  3/2+1 = 2 ?
	if checkcommit[quorum] > rf.CommitIndex {
		for _, v := range rf.Logs {
			if v.Index == checkcommit[quorum] {
				// 只有leader 的新term 写入数据到大部分之后，才会进行commitindex的更新，
				// 副本更新的时候，也是有限制的，只有收到了新leader 新term的数据支持，副本才会更新commitindex
				if v.Term == rf.MyTerm {
					rf.CommitIndex = checkcommit[quorum]
				}
				break
			}
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
	rf.extendElectionTimeout()

	for rf.killed() == false {
		time.Sleep(time.Millisecond * time.Duration(10))

		if rf.role == RoleLeader {
			if rf.leaderTimeout.Before(time.Now()) {
				rf.dealHeartBeatTimeout()
				rf.MyPrintf(DEBUGELECTION, "leader %d term %d send heartbeat time %v\n", rf.me, rf.MyTerm, time.Now().UnixMilli())
				rf.extendLeaderHeartbeatTimeout()
			}
		} else {
			rf.dealElectionTimeout()
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

/*
3 term 152 2024-02-01 17:22:44.476 -- getappendentry from 4 term 152 previndex 0 prevterm 0 leadercommit 5 entryies 0
3 term 152 2024-02-01 17:22:44.481 -- getappendentry from 4 term 152 previndex 30 prevterm 98 leadercommit 5 entryies 10
3 term 152 2024-02-01 17:22:44.481 -- replica logs: 40 {1 1 285} {40 98 2555}

3 term 153 2024-02-01 17:22:44.777 -- start election 1706779365028  1706779364777
实际2C测试中，数据的时长，达到了300ms的间隔,所以我们要再放大一些
*/
func (rf *Raft) extendElectionTimeout() {
	x := rand.Intn(200) + 400
	rf.MyPrintf(DEBUGELECTION, "%d term %d election timeout add %d \n", rf.me, rf.MyTerm, x)
	rf.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(x))
}

func (rf *Raft) extendLeaderHeartbeatTimeout() {
	x := 100
	rf.leaderTimeout = time.Now().Add(time.Millisecond * time.Duration(x))
}

// var globaldebug = DEBUGELECTION
var globaldebug = DEBUGLOGREPLICATION

func (rf *Raft) MyPrintf(level int, format string, a ...interface{}) {
	if globaldebug&level == level {
		str := fmt.Sprintf(format, a...)
		format := "2006-01-02 15:04:05.000"
		fmt.Printf("%d term %d %s -- %s\n", rf.me, rf.MyTerm, time.Now().Format(format), str)
	}
}

// 多次成功
//x := rand.Intn(200) + 100
//x := 50

//x := rand.Intn(100) + 100
//x := 10

// x := rand.Intn(200) + 100
// x := 30
