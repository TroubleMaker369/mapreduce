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
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
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
type AppendEntriesArgs struct {
	Term int //Leader's term

	PrevLogIndex int        //index of log entry immediately preceding new ones (2B)
	PrevLogTerm  int        //term of PrevLogIndex entry (2B)
	Entries      []LogEntry //log entries to store (2B)
	LeaderCommit int        //leader's commitIndex (2B)
}
type AppendEntriesReply struct {
	Term    int //Leader's term
	Success bool

	ConflictIndex int //to optimization
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// role        string //Leader  candidate or follower
	// currentTerm int    //该server属于哪个term
	// voteFor     int    //代表所投的server的下标,初始化为-1
	// //表示本follower的选票还在，没有投票给其他人
	// votedThisTerm int //判断rf.votedFor是在哪一轮承认的leader
	// //防止一个节点同一轮投票给多个candidate
	// logEntries []Entries //保存执行的命令，下标从1开始  log

	// commitIndex int //最后一个提交的日志，下标从0开始
	// lastApplied int //最后一个应用到状态机的日志，下标从0开始

	// //leader独有，每一次election后重新初始化
	// nextIndex  []int //保存发给每个follower的下一条日志下标。初始为leader最后一条日志下标+1
	// matchIndex []int //对于每个follower，已知的最后一条与该follower同步的日志
	// //初始化为0。也相当于follower最后一条commit的日志

	// appendCh chan *AppendEntriesArgs //用于follower判断在
	// //election timeout时间内有没有收到心跳信号
	// voteCh chan *RequestVoteArgs

	// applyCh chan ApplyMsg //每commit一个log，就执行这个日志的命令，
	// //在实验中，执行命令=给applyCh发送信息

	// log bool //是否输出这一个raft实例的log,若为false则不输出，相当于这个实例“死了”

	// electionTimeout *time.Timer //选举时间定时器
	// heartBeat       int         //心跳时间定时器

	//2A
	state       int32 //Follower,Candidate,Leader
	currentTerm int   //服务器最后一次知道的任期号（初始化为 0，持续递增）
	voteFor     int   //当前获得选票的候选人的 Id
	//timerElectionChan 与 timerHeartbeatChan
	//是定时器用来发送消息的，当定时器超时后，会向对应的 Chan 中发送消息

	timerElectionChan  chan bool
	timerHeartbeatChan chan bool

	lastResetElectionTimer  int64
	lastResetHeartbeatTimer int64

	timeoutHeartbeat int64
	timeoutElection  int64

	//2B
	log []LogEntry //logentries
	//对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	nextIndex []int //for each server, index of the next log entry to send to that server (2B)
	////对于每一个服务器，已经复制给他的日志的最高索引值
	matchIndex  []int         //for each server, index of highest log entry known to be replicated on server (2B)
	commitIndex int           //index of highest log entry known to be commited (2B)
	lastApplied int           //index of highest log entry applied to state machine (2B)
	applyCh     chan ApplyMsg // channel to send commit (2B)

}

type LogEntry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) timerElection() {
	for {
		rf.mu.Lock()
		if rf.state != Leader { //lead don`t commit to begin election
			timeElapse := (time.Now().UnixNano() - rf.lastResetElectionTimer) / time.Hour.Milliseconds()
			if timeElapse > rf.timeoutElection {
				DPrintf("[timerElection] raft %d election timeout | current term:%d | current state :%d\n",
					rf.me, rf.currentTerm, rf.state)
				rf.timerElectionChan <- true
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 2)
	}
}
func (rf *Raft) timerHeartbeat() {
	for {
		rf.mu.Lock()
		if rf.state == Leader { //only leader can do  this
			timeElapse := (time.Now().UnixNano() - rf.lastResetHeartbeatTimer) / time.Hour.Milliseconds()
			if timeElapse > rf.timeoutHeartbeat {
				DPrintf("[timerHeartbeat] raft %d heartbeat timeout | current term: %d | current state: %d\n",
					rf.me, rf.currentTerm, rf.state)
				rf.timerHeartbeatChan <- true
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 2) //sendheartbea
	}
}
func (rf *Raft) resetTimerElection() {
	/*
		生成随机数有两种方式
		rander := rand.New(rand.NewSource(time.Now().UnixNano()))
		n1 := rander.Intn(100)

		rand.Seed(time.Now().UnixNano())
		n2 := rand.Intn(100)
	*/
	rand.Seed(time.Now().UnixNano())
	rf.timeoutElection = rf.timeoutHeartbeat*5 + rand.Int63n(150)
	rf.lastResetElectionTimer = time.Now().UnixNano()
}
func (rf *Raft) mainLoop() {
	for {
		select {
		case <-rf.timerElectionChan:
			rf.startElection()
		case <-rf.timerHeartbeatChan:
			rf.braodcastHeartbeat() //broadcastHeartbeat
		}
	}
}
func (rf *Raft) convertTo(state int32) {
	switch state {
	case Follower:
		rf.voteFor = -1
		rf.state = Follower
	case Candidate:
		rf.state = Candidate
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.resetTimerElection()
	case Leader:
		rf.state = Leader
		rf.resetTimerHeartbeat()
	}
}
func (rf *Raft) resetTimerHeartbeat() {
	rf.lastResetHeartbeatTimer = time.Now().UnixNano()
}

func (rf *Raft) braodcastHeartbeat() {
	rf.mu.Lock()
	if rf.state != Leader {
		DPrintf("[broadcastHeartbeat] raft %d lost leadership | current term: %d | current state: %d\n",
			rf.me, rf.currentTerm, rf.state)
		rf.mu.Lock()
		return
	}
	rf.resetTimerHeartbeat()
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(id int) {
		retry:

			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				PrevLogIndex: rf.nextIndex[id] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[id]-1].Term,
				Entries:      rf.log[rf.nextIndex[id]:],
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			var reply AppendEntriesReply
			if rf.sendAppendEntries(id, &args, &reply) {
				rf.mu.Lock()
				//defer rf.mu.Unlock()

				if rf.state != Leader {
					DPrintf("[broadcastHeartbeat] raft %d lost leadership | current term: %d | current state: %d\n",
						rf.me, rf.currentTerm, rf.state)
					rf.mu.Unlock()
					return
				}
				if rf.currentTerm != args.Term {
					DPrintf("[broadcastHeartbeat] raft %d term inconsistency | current term: %d | current state: %d | args term %d\n",
						rf.me, rf.currentTerm, rf.state, args.Term)
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					DPrintf("[appendEntriesAsync] raft %d append entries to %d accepted | current term: %d | current state: %d\n",
						rf.me, id, rf.currentTerm, rf.state)
					rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[id] = rf.matchIndex[id] + 1
					rf.checkN()
				} else {
					DPrintf("[appendEntriesAsync] raft %d append entries to %d rejected | current term: %d | current state: %d | reply term: %d\n",
						rf.me, id, rf.currentTerm, rf.state, reply.Term)
					if reply.Term > rf.currentTerm {
						rf.convertTo(Follower)
						rf.currentTerm = reply.Term
						rf.persist()
						rf.mu.Unlock()
						return
					}
					rf.nextIndex[id] = reply.ConflictIndex
					DPrintf("[appendEntriesAsync] raft %d append entries to %d rejected: decrement nextIndex and retry | nextIndex: %d\n",
						rf.me, id, rf.nextIndex[id])
					rf.mu.Unlock()
					goto retry
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("[appendEntriesAysnc] raft %d RPC to %d failed | current term: %d | current state: %d | reply term: %d\n",
					rf.me, id, rf.currentTerm, rf.state, reply.Term)
			}
		}(i)
	}
}
func (rf *Raft) checkN() {
	for N := len(rf.log) - 1; N > rf.commitIndex && rf.log[N].Term == rf.currentTerm; N-- {
		nReplicated := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
				nReplicated += 1
			}
			if nReplicated > len(rf.peers)/2 {
				rf.commitIndex = N
				go rf.applyEntries()
				break
			}
		}
	}
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.convertTo(Candidate)
	nVote := 1
	DPrintf("[startElection] raft %d start election | current term: %d | current state: %d \n",
		rf.me, rf.currentTerm, rf.state)
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(id int) {
			rf.mu.Lock()
			lastLogIndex := len(rf.log) - 1
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  rf.log[lastLogIndex].Term,
			}
			rf.mu.Unlock()

			var reply RequestVoteReply

			if rf.sendRequestVote(id, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm != args.Term { //id peer has been in other leader term
					return
				}
				if reply.VoteGranted {
					nVote += 1
					DPrintf("[requestVoteAsync] raft %d get accept vote from %d | current term: %d | current state: %d | reply term: %d | poll %d\n",
						rf.me, id, rf.currentTerm, rf.state, reply.Term, nVote)
					if nVote > len(rf.peers)/2 && rf.state == Candidate {
						rf.convertTo(Leader)
						DPrintf("[requestVoteAsync] raft %d convert to leader | current term: %d | current state: %d\n",
							rf.me, rf.currentTerm, rf.state)
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0 //对于每一个服务器，已经复制给他的日志的最高索引
						}
						rf.mu.Unlock()
						rf.braodcastHeartbeat()
						rf.mu.Lock() //?to slove defer rf.mu.Unlock()
					}
				} else {
					DPrintf("[requestVoteAsync] raft %d get reject vote from %d | current term: %d | current state: %d | reply term: %d | poll %d\n",
						rf.me, id, rf.currentTerm, rf.state, reply.Term, nVote)

					if rf.currentTerm < reply.Term {
						rf.convertTo(Follower)
						rf.currentTerm = reply.Term
						rf.persist()
					}
				}
			} else {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("[requestVoteAsync] raft %d RPC to %d failed | current term: %d | current state: %d | reply term: %d\n",
					rf.me, id, rf.currentTerm, rf.state, reply.Term)
			}
		}(i)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	// Your code here (2A).
	return term, isleader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	DPrintf("[persist] raft: %d || currentTerm: %d || votedFor: %d || log len: %d\n", rf.me, rf.currentTerm, rf.voteFor, len(rf.log))
}

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
	// if d.Decode(&xxx) != nil ||f
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("[readPersist] error\n")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = log
	}
	DPrintf("[readPersist] raft: %d || currentTerm: %d || votedFor: %d || log len: %d\n", rf.me, rf.currentTerm, rf.voteFor, len(log))
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int //发出选票的candidate的id，这里是server中的下标

	LastLogIndex int //index of candidate's last log entry (2B)
	LastLogTerm  int // term of candidate's last log entry (2B)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate recieved vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.convertTo(Follower)
		rf.currentTerm = args.Term
		rf.persist()
	}
	if args.Term < rf.currentTerm || (rf.voteFor != -1 && rf.voteFor != args.CandidateID) {
		DPrintf("[RequestVote] raft %d reject vote for %d | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, args.CandidateID, rf.currentTerm, rf.state, args.Term)

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateID { //default args.Term ==rf.currentTerm

		lastLogIndex := len(rf.log) - 1
		if rf.log[lastLogIndex].Term > args.LastLogTerm ||
			(rf.log[lastLogIndex].Term == args.LastLogTerm && args.LastLogIndex < lastLogIndex) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
		DPrintf("[RequestVote] raft %d accept vote for %d | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, args.CandidateID, rf.currentTerm, rf.state, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.voteFor = args.CandidateID
		rf.persist()
		rf.resetTimerElection()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[AppendEntries] raft %d reject append entries | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, rf.currentTerm, rf.state, args.Term)
		return
	}

	rf.resetTimerElection()
	if args.Term > rf.currentTerm {
		rf.convertTo(Follower)
		rf.currentTerm = args.Term
		rf.persist()
		DPrintf("[AppendEntries] raft %d update term | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, rf.currentTerm, rf.state, args.Term)
	} else if rf.state == Candidate {
		rf.state = Follower
		DPrintf("[AppendEntries] raft %d update state | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, rf.currentTerm, rf.state, args.Term)
	}

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[AppendEntries] raft %d reject append entry | log len: %d | args.PrevLogIndex: %d | args.prevLogTerm %d\n",
			rf.me, len(rf.log), args.PrevLogIndex, args.PrevLogTerm)
		reply.Term = rf.currentTerm
		reply.Success = false

		if len(rf.log) <= args.PrevLogIndex {
			reply.ConflictIndex = len(rf.log)
		} else {
			for i := args.PrevLogIndex; i > 0; i-- { //i should >0?
				if rf.log[i].Term != rf.log[i-1].Term {
					reply.ConflictIndex = i
					break
				}
			}
		}
	} else {
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it

		// If the follower has all the entries the leader sent, the follower MUST NOT truncate its log.
		// Any elements following the entries sent by the leader MUST be kept.
		isMatch := true
		nextIndex := args.PrevLogIndex + 1
		conflictIndex := 0
		logLen := len(rf.log)
		entLen := len(args.Entries)
		for i := 0; isMatch && i < entLen; i++ {
			if ((logLen - 1) < (nextIndex + i)) || rf.log[nextIndex+i].Term != args.Entries[i].Term {
				isMatch = false
				conflictIndex = i
				break
			}
		}
		if !isMatch {
			//rf.log = append(rf.log[:nextIndex+conflictIndex], args.Entries[conflictIndex:]...)
			//
			ex := make([]LogEntry, 0, len(rf.log[:nextIndex+conflictIndex]))
			ex = append(ex, rf.log[:nextIndex+conflictIndex]...)
			ex = append(rf.log[:nextIndex+conflictIndex], args.Entries[conflictIndex:]...)
			rf.log = ex
			rf.persist()
			DPrintf("[AppendEntries] raft %d appended entries from leader | log length: %d\n", rf.me, len(rf.log))
		}

		lastNewEntryIndex := args.PrevLogIndex + entLen
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
			go rf.applyEntries() // apply entries after update commitIndex
			//should release rf.mu.lock?
		}
		reply.Term = rf.currentTerm
		reply.Success = true
	}

}
func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if commitIndex > lastApplied:
	//increment lastApplied, apply log[lastApplied] to state machine
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
		rf.lastApplied += 1
		DPrintf("[applyEntries] raft %d applied entry | lastApplied: %d | commitIndex: %d\n",
			rf.me, rf.lastApplied, rf.commitIndex)
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if term, isLeader = rf.GetState(); isLeader {
		rf.mu.Lock()
		rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
		rf.persist()
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1 + len(rf.log)
		index = len(rf.log) - 1
		DPrintf("[Start] raft %d replicate command to log | current term: %d | current state: %d | log length: %d\n",
			rf.me, rf.currentTerm, rf.state, len(rf.log))
		rf.mu.Unlock()
	}

	return index, term, isLeader
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
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.timerHeartbeatChan = make(chan bool)
	rf.timerElectionChan = make(chan bool)
	rf.timeoutHeartbeat = 100 // ms
	rf.resetTimerElection()

	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("Starting raft %d\n", me)
	// start ticker goroutine to start elections
	//go rf.ticker()
	go rf.mainLoop()
	go rf.timerElection()
	go rf.timerHeartbeat()
	return rf
}
