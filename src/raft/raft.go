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
	DEBUGDETAIL         = 1 << 4
)
const ()

type SnapshotInfo struct {
	LastIncludedIndex int
	LastIncludedTerm  int
}

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
	Snapshotinfo     SnapshotInfo
	nextIndex        []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex       []int

	applyChn        chan ApplyMsg
	electionTimeout time.Time
	leaderTimeout   time.Time

	persistindex int
	persistmu    sync.Mutex

	Maxraftstate int
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

	if len(rf.Logs) > 0 && rf.Snapshotinfo.LastIncludedIndex+1 != rf.Logs[0].Index {
		panic(fmt.Sprintf(" snapshot and logs not consistent %d  %d ", rf.Snapshotinfo.LastIncludedIndex+1, rf.Logs[0].Index))
	}

	// 重启之后置为0， 在下一个层新之前，把数据发送给tester 这里只是这个测试这样设计的，实际过程中，可以不需要重放
	// 这里值得商榷，因为如果信息存储在db中，实际上读取出来就可以了，不需要这样重放一下所有的数据
	// snapshot + crash 这里的bug 就显现出来了，
	// 跟踪Make函数的调用，发现每次 Make之后，tester的Applyid 相关信息被置为了0，所以这里我们也得置为0，然后重放一下，
	// 这里一直没有搞懂 tester的流程，花费了不少时间

	/*rf.CommitIndex = 0
	rf.LastAppliedIndex = 0*/

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.VotedFor = -1
	rf.role = RoleFollower
	rf.applyChn = applyCh
	rf.Maxraftstate = -1
	retchn := make(chan int)
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		retchn <- 1
		rf.LastAppliedIndex = 0
		rf.updateAppliedIndex()
	}()
	<-retchn
	close(retchn)

	rf.MyPrintf(DEBUGLOGREPLICATION, " *************** i am created now %d ", rf.me)
	rf.Print()

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

func (rf *Raft) getBiggerTerm(replyterm int) {
	if replyterm > rf.MyTerm {
		if rf.role == RoleLeader {
			rf.MyPrintf(0, " leader get bigger term i become fellower")
		}
		rf.MyTerm = replyterm
		rf.VotedFor = -1
		rf.role = RoleFollower
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.MyTerm, rf.role == RoleLeader
}
func (rf *Raft) Print() {
	rf.printX(DEBUGDETAIL)
}
func (rf *Raft) Print0() {
	rf.printX(0)
}
func (rf *Raft) printX(level int) {

	if len(rf.Logs) > 0 {
		if len(rf.Logs) < 30 {
			rf.MyPrintf(level, "        fullinfo: %d term %d isleader %v commitedindex: %d  applyiedindex: %d snapshot %v logslen: %d  first: %v end: %v all %v ",
				rf.me, rf.MyTerm, rf.role == RoleLeader, rf.CommitIndex, rf.LastAppliedIndex, rf.Snapshotinfo, len(rf.Logs),
				rf.Logs[0], rf.Logs[len(rf.Logs)-1], rf.Logs)
		} else {
			slice := rf.Logs[len(rf.Logs)-30:]
			rf.MyPrintf(level, "        fullinfo: %d term %d isleader %v commitedindex: %d  applyiedindex: %d snapshot %v logslen: %d  first: %v end: %v last30 %v",
				rf.me, rf.MyTerm, rf.role == RoleLeader, rf.CommitIndex, rf.LastAppliedIndex, rf.Snapshotinfo, len(rf.Logs),
				rf.Logs[0], rf.Logs[len(rf.Logs)-1], slice)
		}

	} else {
		rf.MyPrintf(level, "        fullinfo %d term %d isleader %v commitedindex: %d  applyiedindex: %d snapshot %v logslen: %d  ",
			rf.me, rf.MyTerm, rf.role == RoleLeader, rf.CommitIndex, rf.LastAppliedIndex, rf.Snapshotinfo, len(rf.Logs))
	}
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

func (rf *Raft) lastEntryLogandSnapshot() (int, int) {
	x := len(rf.Logs)
	if x > 0 {
		return rf.Logs[x-1].Index, rf.Logs[x-1].Term
	} else {
		return rf.Snapshotinfo.LastIncludedIndex, rf.Snapshotinfo.LastIncludedTerm
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) getpersistbytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.MyTerm)
	err = e.Encode(rf.CommitIndex)
	err = e.Encode(rf.LastAppliedIndex)
	err = e.Encode(rf.VotedFor)
	err = e.Encode(rf.Logs)
	err = e.Encode(rf.Snapshotinfo)
	if err != nil {
		panic(" err := e.Encode(rf) : " + err.Error())
	}
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	// Your code here (2C).
	start := time.Now()
	data := rf.getpersistbytes()
	rf.persister.SaveRaftState(data)
	timelen := time.Now().UnixMilli() - start.UnixMilli()
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	err := d.Decode(&rf.MyTerm)
	err = d.Decode(&rf.CommitIndex)
	err = d.Decode(&rf.LastAppliedIndex)
	err = d.Decode(&rf.VotedFor)
	err = d.Decode(&rf.Logs)
	err = d.Decode(&rf.Snapshotinfo)
	if err != nil {
		panic(" decode rf error ")
	}
}

// 在外部发送时加锁
// true ,return info and bytes
// false , just return info
func (rf *Raft) readSnapshot(readbytes bool) (SnapshotInfo, []byte) {
	var snapshotbuf []byte
	if readbytes {
		snapshotbuf = rf.persister.ReadSnapshot()
	}
	return rf.Snapshotinfo, snapshotbuf
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, timeout int64) bool {
	retchn := make(chan bool)
	stopchn := make(chan int)
	defer func() {
		close(stopchn)
	}()

	go func() {
		start := time.Now()
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		timems := time.Now().UnixMilli() - start.UnixMilli()
		rf.MyPrintf(DEBUGLOGREPLICATION, "send to peer %d InstallSnapshot rpc return %v speed %d ms isobselete %v replytrem %d",
			server, ok, timems, timems > timeout, reply.Term)
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
	}()

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

// return findindexok, logindex, logterm
func (rf *Raft) trimLogs(index int) (bool, int, int) {
	logarrayi := -1
	logindex, logterm := -1, -1
	for i, val := range rf.Logs {
		if val.Index == index {
			logindex = val.Index
			logterm = val.Term
			logarrayi = i
			break
		}
	}
	if -1 != logarrayi { // [1,2,3]  3   i: 2
		if logarrayi+1 < len(rf.Logs) {
			rf.Logs = rf.Logs[logarrayi+1:]
		} else {
			rf.Logs = rf.Logs[:0]
		}
		return true, logindex, logterm
	} else {
		rf.Logs = rf.Logs[:0]
	}
	return false, logindex, logterm
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible
// the tester calls Snapshot() periodically

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	go func() {
		// 这个锁还是得加，因为它会访问 rf.logs，但是直接加，会在tester的线程卡死，所以这里单开协程
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if index <= rf.Snapshotinfo.LastIncludedIndex ||
			(rf.Maxraftstate != -1 && rf.GetRaftStateSizee() <= rf.Maxraftstate) {
			// kvserver中 snapshot由用户触发，如果两个index 是相同的 可能会造成查找失败
			// 后面的 snapshot协程，可能会比早发起的协程要先执行。。。。。。防不胜防啊
			return
		}
		// snapshot 已经是所有的数据合集了，config.go 256行
		rf.MyPrintf(0, " call Snapshot index %d", index)

		bfind, loginx, logterm := rf.trimLogs(index)
		// snapshot 在有新的commit index的时候，才会执行， 所以肯定会有数据
		// 如果有新的snapshot install了，会情况日志，但是也要等继续有commit index时，才继续执行snapshot
		// 这时候 index 肯定也是大于 snapshot LastIncludedIndex, 所以这两个panic 不可能发生
		// 自己触发snapshot的时候，也要考虑到这一点，
		if bfind == false {
			panic(" can not happen")
		}
		rf.persistSnapshot(loginx, logterm, snapshot)
	}()
}
func (rf *Raft) persistSnapshot(loginx int, logterm int, snapshot []byte) {
	if loginx <= rf.Snapshotinfo.LastIncludedIndex {
		rf.MyPrintf(DEBUGLOGREPLICATION, "只增不减 index %d term  %d snapLastIncludedIndex: %d ", loginx, logterm, rf.Snapshotinfo.LastIncludedIndex)
		return
	}

	rf.Snapshotinfo.LastIncludedIndex = loginx
	rf.Snapshotinfo.LastIncludedTerm = logterm
	rf.persister.SaveStateAndSnapshot(rf.getpersistbytes(), snapshot)
}

// example RequestVote RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.MyTerm
	if args.Term < rf.MyTerm {
		return
	}
	if args.Term > rf.MyTerm {
		//If RPC request or response contains Term T > currentTerm: set currentTerm = T, convert to follower (§5.1
		rf.getBiggerTerm(args.Term)
		reply.Term = rf.MyTerm
	}
	// Save snapshot file, discard any existing or partial snapshot with a smaller index
	// 文件名应该以 leaderterm_lastindex_lastterm 命名， 删除掉小的， 比当前正在写的小，就不继续写了

	// Take care that these snapshots only advance the service's state, and don't cause it to move backwards.
	// 只能增加，不能减，不论什么异常情况
	rf.MyPrintf(0, "InstallSnapshot leaderterm %d leaderid %d  includedindex %d includeterm %d",
		args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	if args.LastIncludedIndex <= rf.Snapshotinfo.LastIncludedIndex {
		return
	}
	// 暂时都是一次发完
	if args.Done && args.Offset == 0 {
		rf.Print()
		//f existing log entry has same index and term as snapshot’s
		//last included entry, retain log entries following it and reply
		bfind, _, logterm := rf.trimLogs(args.LastIncludedIndex)
		if bfind && logterm != args.LastIncludedTerm {
			rf.Logs = rf.Logs[:0] //清空，杀干净
		}
		if len(rf.Logs) > 0 && rf.Logs[0].Index != args.LastIncludedIndex+1 {
			panic(" bad trim logs")
		}
		rf.applyChn <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data, //这里的snapshot，就是
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
		// applymsg 之后 lastapply 必须是和snapshot一致的
		rf.CommitIndex = args.LastIncludedIndex
		rf.LastAppliedIndex = args.LastIncludedIndex
		rf.persistSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
		rf.Print()
	} else {
		panic(" not support snapshot chunk")
	}
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
		rf.getBiggerTerm(args.Term)
		reply.Term = rf.MyTerm
	}
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		//If votedFor is null or candidateId, and candidate’s log is at
		//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		mylastinx, mylastterm := rf.lastEntryLogandSnapshot()
		if (args.LastLogTerm > mylastterm) ||
			(args.LastLogTerm == mylastterm && args.LastLogIndex >= mylastinx) {
			//If the logs have last entries with different terms, then the log with the later term is more up-to-date.
			//If the logs end with the same term, then whichever log is longer is more up-to-date.
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, timeout int64) bool {
	//ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//return ok
	retchn := make(chan bool)
	stopchn := make(chan int)
	defer func() {
		close(stopchn)
	}()

	go func() {
		start := time.Now()
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		timems := time.Now().UnixMilli() - start.UnixMilli()
		rf.MyPrintf(DEBUGLOGREPLICATION, "send to peer %d RequestVote rpc return %v speed %d ms isobselete %v granted %v replyterm %d when i am %d",
			server, ok, timems, timems > timeout, reply.VoteGranted, reply.Term, args.Term)
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
	}()

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

/*
1 term 1 2024-01-30 22:02:41.508 --  initNowIAmLeader

1 term 1 2024-01-30 22:02:41.509 --  start go routine sendlog 2
0 term 1 2024-01-30 22:02:41.508 --  initNowIAmLeader
这里有个状态差， 开始 election 的时候，可能已经选了别人 加锁
*/
func (rf *Raft) dealElectionTimeout() {
	rf.mu.Lock()
	lastloginx, lastlogterm := rf.lastEntryLogandSnapshot()
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
	type RetValue struct {
		ok    bool
		reply RequestVoteReply
	}
	// retchn := make(chan RetValue) 会造成 超时的 rpc routine 退不出去，或者在增加一个stop chan
	retchn := make(chan RetValue, len(rf.peers))

	for inx, _ := range rf.peers {
		if inx == rf.me {
			continue
		}
		// 如果网络不通，rpc 返回不及时，这个定时器会造成，协程发送过多
		go func(inx int) {
			req := RequestVoteArgs{
				Term:         rf.MyTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastloginx,
				LastLogTerm:  lastlogterm,
			}
			reply := RequestVoteReply{}
			ret := rf.sendRequestVote(inx, &req, &reply, 300)
			retchn <- RetValue{
				ok:    ret,
				reply: reply,
			}
		}(inx)
	}
	// 2 term 78 2024-02-01 19:02:13.622 -- send request to 4 vote spend 5737 ms ret false reply granted false replyterm 0 when i am 70
	// 为什么这个值放这么大呢，看看实际项目中的 5秒钟时间

	defer func() {
		rf.extendElectionTimeout()
	}()

	oknums := 1
	for x := 1; x < len(rf.peers); x++ {
		ret := <-retchn
		if ret.ok == false {
			continue
		}
		// 这里可能会有一些异常发生，在异步发送了投票请求之后，马上收到了 更高term的请求，使得自己的term更加了 同时变成 rolefellower 了
		// 所以这里判断一下，如果有变化，直接退出选举 防不胜防啊
		// 3 term 77 2024-02-05 20:18:18.249 -- send request to 0 vote spend 261 ms ret true reply granted true replyterm 76
		if rf.role != RoleCandidate && ret.reply.Term != rf.MyTerm {
			rf.MyPrintf(DEBUGLOGREPLICATION, "get bad term or role, i will exit election")
			return
		}
		if ret.reply.VoteGranted {
			oknums++
			if oknums >= len(rf.peers)/2+1 {
				rf.mu.Lock()
				rf.role = RoleLeader
				rf.MyPrintf(0, " become leader %d term %d\n", rf.me, rf.MyTerm)
				rf.Print0()
				rf.initNowIAmLeader()
				rf.mu.Unlock()
				return
			}
		} else if ret.reply.Term > rf.MyTerm {
			rf.mu.Lock()
			rf.getBiggerTerm(ret.reply.Term)
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) initNowIAmLeader() {
	rf.MyPrintf(DEBUGLOGREPLICATION, " initNowIAmLeader")
	rf.leaderTimeout = time.UnixMilli(0)
	rf.persistindex = 0
	logindex, _ := rf.lastEntryLogandSnapshot()
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
	if rf.killed() {
		return
	}

	reply.Success = false
	reply.Term = rf.MyTerm
	if args.Term < rf.MyTerm {
		return
	}

	if rf.role == RoleLeader && rf.MyTerm == args.Term {
		rf.Print()
		rf.MyPrintf(DEBUGLOGREPLICATION, "peer info %v", args)
		panic(" big problome, election error , now have  two leader !!!!!!!!!!!!!!!!!")
	}

	rf.MyPrintf(DEBUGLOGREPLICATION, "getappendentry from %d term %d previndex %d prevterm %d leadercommit %d entryies %d ",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))

	if args.Term >= rf.MyTerm {
		//If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
		//then the candidate recognizes the leader as legitimate and returns to follower state
		rf.getBiggerTerm(args.Term)
		reply.Term = rf.MyTerm
		rf.persist()
	}

	if len(args.Entries) == 0 {
		reply.Success = true
	} else {
		rf.Print()
		if args.PrevLogIndex == 0 {
			// 是不是要继续比较一下，后续的数据是否一致？ 不一致再清除
			rf.Logs = rf.Logs[:0]
			rf.Logs = append(rf.Logs, args.Entries...)
			// need clear snapshot ？？
			rf.Snapshotinfo.LastIncludedTerm = 0
			rf.Snapshotinfo.LastIncludedIndex = 0
		} else {
			//2. Reply false if log doesn’t contain an entry at prevLogIndex
			//whose term matches prevLogTerm (§5.3)
			previnlog := -1
			indexlog := -1
			{
				// 发送和接受时 PrevLogIndex 要特殊处理
				reply.ReplicaLogIndex = 0                                  // 有了snapshot 直接回归d到原点
				if args.PrevLogIndex < rf.Snapshotinfo.LastIncludedIndex { // 比snapshot还小，直接重新开始
					return
				} else if args.PrevLogIndex == rf.Snapshotinfo.LastIncludedIndex {
					if args.PrevLogTerm != rf.Snapshotinfo.LastIncludedTerm {
						return
					} else {
						indexlog = 0
					}
				} else {
					// prev  比 snapshotindex 大， 如果没有找到，先回到snapshot index + 1 ，不要一路撸到底
					mylogindex, mylogterm := 0, 0
					for i, val := range rf.Logs {
						if val.Term == args.PrevLogTerm && mylogindex == 0 { // 如果entries的第一个是 它term的第一个，则pervterm 肯定不是它term；
							mylogindex = val.Index
							mylogterm = val.Term
						}
						if val.Index == args.PrevLogIndex && val.Term == args.PrevLogTerm {
							indexlog = i + 1
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
				}
			}

			//3. If an existing entry conflicts with a new one (same index
			//but different terms), delete the existing entry and all that follow it (§5.3)
			i := indexlog // + 1
			j := 0
			for i < len(rf.Logs) && j < len(args.Entries) {
				if rf.Logs[i].Index != args.Entries[j].Index {
					panic(fmt.Sprintf(" rf log index %d is different from %d args.entries.index  in i %d j%d ", rf.Logs[i].Index, args.Entries[j].Index, i, j))
				}
				if rf.Logs[i].Term != args.Entries[j].Term {
					rf.Logs = rf.Logs[:i] // delete i and all follow it
					break
				}
				i++
				j++
			}
			//4. Append any new entries not already in the log
			// _, lastinx, _ := rf.lastLog() 可能刚好 snapshot了， rf.logs 为空了。。。。。。 有意思
			lastinx, _ := rf.lastEntryLogandSnapshot()
			for _, val := range args.Entries {
				if val.Index == lastinx+1 {
					rf.Logs = append(rf.Logs, val)
					lastinx += 1
				}
			}
			rf.persist()
		}
		rf.MyPrintf(0, "        replica logs: len: %v   first %v end %v ", len(rf.Logs), rf.Logs[0], rf.Logs[len(rf.Logs)-1])
		reply.Success = true
		rf.Print()
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
	lastlogindex, lastlogterm := rf.lastEntryLogandSnapshot()
	if lastlogterm == args.Term && rf.CommitIndex < args.LeaderCommit {
		if args.LeaderCommit > lastlogindex {
			rf.CommitIndex = lastlogindex
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
	}
}

func (rf *Raft) updateAppliedIndex() {
	if rf.killed() == false && rf.CommitIndex > rf.LastAppliedIndex {

		if rf.LastAppliedIndex < rf.Snapshotinfo.LastIncludedIndex {
			snap, snapdata := rf.readSnapshot(true)
			rf.applyChn <- ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      snapdata,
				SnapshotIndex: snap.LastIncludedIndex,
				SnapshotTerm:  snap.LastIncludedTerm,
			}
			rf.LastAppliedIndex = snap.LastIncludedIndex
			rf.MyPrintf(DEBUGLOGREPLICATION, " update applyiedindex to %d by applymsg snapshot ", rf.LastAppliedIndex)
		}

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

					rf.LastAppliedIndex = val.Index
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

		rf.MyPrintf(DEBUGLOGREPLICATION, " update applyiedindex to %d ", rf.LastAppliedIndex)
		// commit 更新，apply 一定会更新
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply, timeout int64) bool {
	retchn := make(chan bool)
	stopchn := make(chan int)
	defer func() {
		close(stopchn)
	}()

	go func() {
		start := time.Now()
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		timems := time.Now().UnixMilli() - start.UnixMilli()
		rf.MyPrintf(DEBUGLOGREPLICATION, "send to peer %d entries rpc return %v speed %d ms isobselete %v entries %d ",
			server, ok, timems, timems > timeout, len(args.Entries))
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
	}()

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

func (rf *Raft) dealHeartBeatTimeout() {
	if rf.role == RoleLeader {
		for inx, _ := range rf.peers {
			if inx == rf.me {
				continue
			}
			/*
					kvserver中，当网络不稳定，但是clerk 不断超时，快速发起请求的时候，会造成 raft中的log 残留很多
				    会有相同clerkid 和 serialnum， 如果此时 leader 不卸任，就会出错，
					或者在 kvserver这一级进行判断，但是这一级判断不了，除非保存一个 clerkid serialnum  --> index 的映射，这样可以吧之前的index 绑定到新的retchannel上


					学生提问：有没有可能出现极端的情况，导致单向的网络出现故障，进而使得Raft系统不能工作？


					Robert教授：我认为是有可能的。例如，如果当前Leader的网络单边出现故障，Leader可以发出心跳，但是又不能收到任何客户端请求。
					它发出的心跳被送达了，因为它的出方向网络是正常的，那么它的心跳会抑制其他服务器开始一次新的选举。
					但是它的入方向网络是故障的，这会阻止它接收或者执行任何客户端请求。这个场景是Raft并没有考虑的众多极端的网络故障场景之一。

					我认为这个问题是可修复的。我们可以通过一个双向的心跳来解决这里的问题。
					在这个双向的心跳中，Leader发出心跳，但是这时Followers需要以某种形式响应这个心跳。
					如果Leader一段时间没有收到自己发出心跳的响应，Leader会决定卸任，这样我认为可以解决这个特定的问题和一些其他的问题。
					网络中可能发生非常奇怪的事情，而Raft协议没有考虑到这些场景。
					查看响应是否过半数超时， 多次超时
			*/
			//lastloginx, lastlogterm := rf.lastEntryLogandSnapshot()
			go func(inx int) {
				rf.mu.Lock()
				args := AppendEntries{
					Term:         rf.MyTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.CommitIndex,
					//PrevLogIndex: lastloginx,
					//PrevLogTerm:  lastlogterm,
				}
				if rf.role != RoleLeader {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ret := rf.sendAppendEntries(inx, &args, &reply, 100)
				rf.mu.Lock()
				if ret && reply.Term > rf.MyTerm {
					rf.getBiggerTerm(reply.Term)
				}
				rf.mu.Unlock()
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
	index, term := rf.lastEntryLogandSnapshot()
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
	rf.MyPrintf(DEBUGLOGREPLICATION, "        logs: %v %v %v", len(rf.Logs), rf.Logs[0], rf.Logs[len(rf.Logs)-1])

	//rf.persist()
	// 这里肯定不能串行化啊，耗时太高了，影响了其他消息的发送
	// 可以在发送之前，进行串行化，这样有了发送间隔之后，串行化次数就会少很多，这里懒得优化了
	// 如果测试可以通过，就不着急
	return index, term, rf.role == RoleLeader
}

func (rf *Raft) indexInLog(index int) bool {
	loglen := len(rf.Logs)
	if loglen > 0 {
		return index >= rf.Logs[0].Index && index <= rf.Logs[loglen-1].Index
	}
	return false
}
func (rf *Raft) indexInSnapshot(index int) bool {
	if index <= 0 {
		panic("indexInSnapshot can not happened")
	}
	return index <= rf.Snapshotinfo.LastIncludedIndex
}

func (rf *Raft) dealLogReplication(topeer int, nowmyterm int) {
	rf.MyPrintf(DEBUGLOGREPLICATION, " start go routine sendlog %d ", topeer)
	errnums := 0
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
				PrevLogTerm:  -1,
			}

			rf.mu.Lock()

			//loglen, loglastindex, _ := rf.lastLog()
			////if loglen > 0 && loglastindex >= rf.nextIndex[topeer] {
			if rf.indexInLog(rf.nextIndex[topeer]) {
				rf.MyPrintf(DEBUGLOGREPLICATION, "send to peer %d entries fromindex  %d ", topeer, rf.nextIndex[topeer])
				//第一条数据时，PrevLogIndex 为0
				entries.PrevLogIndex = rf.nextIndex[topeer] - 1

				if entries.PrevLogIndex == rf.Snapshotinfo.LastIncludedIndex {
					entries.PrevLogTerm = rf.Snapshotinfo.LastIncludedTerm
				}
				for _, val := range rf.Logs {
					if val.Index == entries.PrevLogIndex {
						entries.PrevLogTerm = val.Term
					}
					MaxEntriesOneAppend := 1000
					if val.Index >= rf.nextIndex[topeer] && len(entries.Entries) < MaxEntriesOneAppend {
						entries.Entries = append(entries.Entries, val)
					}
				}
				if entries.Entries[len(entries.Entries)-1].Index > rf.persistindex {
					// 每次发送之前persist 这样能确保协商的数据肯定被persist，同时也不会太频繁
					rf.persistindex = entries.Entries[len(entries.Entries)-1].Index
					rf.persist()
				}
				rf.mu.Unlock()

				if len(entries.Entries) == 0 || entries.PrevLogTerm == -1 {
					panic(" len(entries.Entries) can not be 0 or term not be -1")
				}

				reply := AppendEntriesReply{}
				bok := rf.sendAppendEntries(topeer, &entries, &reply, 1000)
				if false == bok {
					errnums++
					if errnums >= 5 && (errnums%5) == 0 {
						rf.MyPrintf(0, "entries from %d len %d big send error nums : %d to peer %d -----------------",
							entries.PrevLogIndex, len(entries.Entries), errnums, topeer)
					}
					break // 失败之后，延迟再发，不着急
				} else {
					if errnums > 0 {
						rf.MyPrintf(0, "entries from %d len %d  big send error nums sucess: %d to peer %d -----------------",
							entries.PrevLogIndex, len(entries.Entries), errnums, topeer)
						errnums = 0
					}
					rf.mu.Lock()
					if reply.Success {
						// If successful: update nextIndex and matchIndex for follower
						rf.nextIndex[topeer] = entries.Entries[len(entries.Entries)-1].Index + 1
						rf.matchIndex[topeer] = entries.Entries[len(entries.Entries)-1].Index
						rf.updateLeaderCommitIndex()
						rf.updateAppliedIndex()
					} else {
						if reply.Term == rf.MyTerm {
							//rf.nextIndex[topeer]--
							rf.nextIndex[topeer] = reply.ReplicaLogIndex // - 1
							if rf.nextIndex[topeer] <= 0 {
								rf.nextIndex[topeer] = 1 // 限制一下，不能比0 更小
							}
							rf.mu.Unlock()
							rf.MyPrintf(DEBUGLOGREPLICATION, "send to peer %d entries not-success  againindex %d ", topeer, rf.nextIndex[topeer])
							continue
						} else if reply.Term > rf.MyTerm {
							rf.getBiggerTerm(reply.Term)
						}
					}
					rf.mu.Unlock()
				}
				// 发送处理结束
			} else if rf.indexInSnapshot(rf.nextIndex[topeer]) {
				info, snapshotdata := rf.readSnapshot(true)
				rf.mu.Unlock()

				rf.MyPrintf(DEBUGLOGREPLICATION, "send to peer %d InstallSnapshot fromindex  %d snapshot %v ", topeer, rf.nextIndex[topeer], info)
				args := InstallSnapshotArgs{
					Term:              rf.MyTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: info.LastIncludedIndex,
					LastIncludedTerm:  info.LastIncludedTerm,
					Offset:            0,
					Data:              snapshotdata,
					Done:              true,
				}
				reply := InstallSnapshotReply{}
				ok := rf.sendInstallSnapshot(topeer, &args, &reply, 1000)
				if ok {
					if errnums > 0 {
						rf.MyPrintf(0, "installsnapshot %d big send error nums sucess: %d to peer %d -----------------", args.LastIncludedIndex, errnums, topeer)
						errnums = 0
					}

					rf.mu.Lock()
					if reply.Term == rf.MyTerm {
						// If successful: update nextIndex and matchIndex for follower
						rf.nextIndex[topeer] = info.LastIncludedIndex + 1
						rf.matchIndex[topeer] = info.LastIncludedIndex
						rf.updateLeaderCommitIndex()
						rf.updateAppliedIndex()
					} else if reply.Term > rf.MyTerm {
						rf.getBiggerTerm(reply.Term)
					}
					rf.mu.Unlock()
				} else {
					errnums++
					if errnums >= 5 && (errnums%5 == 0) {
						rf.MyPrintf(0, " installsnapshot %d big send error nums : %d to peer %d -----------------", args.LastIncludedIndex, errnums, topeer)
					}
					break
				}
			} else {
				rf.mu.Unlock()
				break
			}

		}
	}
out:
	rf.MyPrintf(DEBUGLOGREPLICATION, " dealLogReplication go routine over %d ", topeer)
}

func (rf *Raft) findTermByLogIndex(index int) (bool, int) {
	if rf.Snapshotinfo.LastIncludedIndex == index {
		return true, rf.Snapshotinfo.LastIncludedTerm
	}
	for _, v := range rf.Logs {
		if v.Index == index {
			return true, v.Term
		}
	}
	return false, 0
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

	// kill 需要和 updateappliedid 进行同步，否则会出现 kill了，但是updateappliedid还在进行
	// 这里保障，只要kill，那边不会进行更新id，只要更新id中，kill暂时无法进入
	rf.mu.Lock()
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Unlock()

	rf.MyPrintf(0, "************************* i kill myself now over!!!!")
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
	x := rand.Intn(400) + 400
	rf.MyPrintf(DEBUGELECTION, "%d term %d election timeout add %d \n", rf.me, rf.MyTerm, x)
	rf.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(x))
}

func (rf *Raft) extendLeaderHeartbeatTimeout() {
	x := 100
	rf.leaderTimeout = time.Now().Add(time.Millisecond * time.Duration(x))
}

// var globaldebug = DEBUGELECTION
var globaldebug = 0 // DEBUGLOGREPLICATION //| DEBUGDETAIL

func (rf *Raft) MyPrintf(level int, format string, a ...interface{}) {
	if globaldebug&level == level {
		str := fmt.Sprintf(format, a...)
		format := "2006-01-02 15:04:05.000"
		fmt.Printf("%d term %d %s -- %s\n", rf.me, rf.MyTerm, time.Now().Format(format), str)
	}
}
func (rf *Raft) GetRaftStateSizee() int {
	return rf.persister.RaftStateSize()
}

// 多次成功
//x := rand.Intn(200) + 100
//x := 50

//x := rand.Intn(100) + 100
//x := 10

// x := rand.Intn(200) + 100
// x := 30
