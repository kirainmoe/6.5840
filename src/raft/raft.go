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

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// constants
const NOT_VOTED = -1

// server roles, can be leader/follower/candidate
const (
	ROLE_LEADER    = 1
	ROLE_FOLLOWER  = 2
	ROLE_CANDIDATE = 3
)

const (
	ELECTION_TIMEOUT_MIN_MS = 1000
	ELECTION_TIMEOUT_MAX_MS = 2000
	RPC_TIMEOUT_MS          = 100
)

// A Go object implementing a single Raft peer.
type LogEntry struct {
	Term    int64
	Payload any
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// server role (ROLE_*)
	role int32
	// election timeout, random by default
	electionTimeoutMs int64
	// timestamp of last AppendEntries RPC or
	lastReceiveRpcTimestamp int64

	// persistant state on all servers, should be update to stable storage before responding RPCs

	// latest term this server has been seen, default is 0
	currentTerm int64
	// candidateId that this server has voted for in current term, -1 if not voted
	votedFor int
	// log entries
	log []LogEntry

	// volatile state on all servers

	// index of highest log entry known to be committed (from leader)
	commitIndex int64
	// index of highest log entry known to be applied to state machine
	lastApplied int64

	// volatile state on leaders, reinitialized after election

	// for each followers, index of the next log entry to send
	nextIndex []int
	// for each server, index of highest log entry known to be repliated on server
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.currentTerm)
	isleader = rf.role == ROLE_LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	CandidateId  int
	Term         int64
	LastLogIndex int
	LastLogTerm  int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.info("received RequestVote args=%+v", args)
	rf.lastReceiveRpcTimestamp = time.Now().UnixMilli()

	if args.Term < rf.currentTerm {
		rf.info("candidate's term is behind follower's term candidate=%v incoming=%v current=%v",
			args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != NOT_VOTED {
		rf.info("already voted in current term candidate=%v term=%v voted=%v",
			args.CandidateId, args.Term, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm && rf.role != ROLE_FOLLOWER {
		rf.info("candidate term is ahead of current server, transform to follower incoming=%v current=%v",
			args.Term, rf.currentTerm)
		rf.role = ROLE_FOLLOWER
	}

	// TODO: checks candidate's log is at least as up-to-date receiver's log
	rf.currentTerm = args.Term // TODO: should update term here?
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.info("voted for the candidate in current term candidate=%v term=%v", args.CandidateId, args.Term)
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term      int64
	Success   bool
	LastIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debug("received AppendEntries from leader args=%+v", args)

	reply.LastIndex = -1

	// current role is leader, but meet a larger term
	if rf.role == ROLE_LEADER && rf.currentTerm < args.Term {
		rf.warn("leader meet a larger term of AppendEntries, converting to follower peer=%d", args.LeaderId)
		rf.updateTerm(args.Term)
		rf.convertToRole(ROLE_FOLLOWER)
	}

	// check AppendEntries call is from a valid leader
	// if leaders' term is behind of current follower, reject the request and return the known latest term
	reply.Term = max(args.Term, rf.currentTerm)
	if rf.currentTerm > args.Term {
		rf.info("source term is behind current term leaderId=%v sourceTerm=%v currentTerm=%v",
			args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		return
	}

	// if follower's log does not contain an entry at prevLogIndex
	currentLogIndex := len(rf.log) // +1
	if currentLogIndex < args.PrevLogIndex {
		rf.info("follower's log does not contain an entry at prevLogIndex leaderId=%v incoming=%v current=%v",
			args.LeaderId, args.PrevLogIndex, currentLogIndex)
		reply.Success = false
		reply.LastIndex = currentLogIndex
		return
	}

	// existing log entry conflicts with a new one (same index, different terms)
	if args.PrevLogIndex != 0 {
		// check logs has conflict
		hasConflict := false
		conflictIndex := -1

		for i, log := range args.Entries {
			logIndex := args.PrevLogIndex + i
			// has
			if log.Term != rf.log[logIndex].Term {
				rf.warn("conflict is detected index=%v incomingTerm=%v currentTerm=%v",
					logIndex, log.Term, rf.log[logIndex].Term)

				hasConflict = true
				conflictIndex = logIndex
				break
			}
		}

		// when conflict is detected, delete all existing entries after conflict index
		if hasConflict {
			to := len(rf.log)
			rf.log = rf.log[0:conflictIndex]
			rf.warn("removing entries from=%v to=%v nowLength=%v", conflictIndex, to, len(rf.log))
		}
	}

	// append new entries to log
	for _, log := range args.Entries {
		rf.log = append(rf.log, log)
	}
	rf.debug("append %d logs lastLogIndex=%v", len(args.Entries), len(rf.log))

	// update follower's term
	rf.currentTerm = max(rf.currentTerm, args.Term)

	// update last receive time
	rf.lastReceiveRpcTimestamp = time.Now().UnixMilli()

	// commit logs (TODO)
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return next role after vote
func (rf *Raft) requestVoteForAllServer() int32 {
	rf.info("sending RequestVote() to all servers peersCount=%v", len(rf.peers))

	var lock sync.Mutex
	var wg sync.WaitGroup

	votes := int64(1) // granted to self
	updateTerm := int64(-1)

	req := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	// concurrently request vote from all peers (except self)
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			res := RequestVoteReply{}

			rf.debug("send RequestVote to peer=%v", index)
			ok := RunInTimeLimit(RPC_TIMEOUT_MS, func() {
				rf.sendRequestVote(index, &req, &res)
			})
			if !ok {
				rf.error("RequestVote call failed or timeout peerIndex=%d", index)
				return
			}

			lock.Lock()
			defer lock.Unlock()

			// term in other follower is larger than current
			if res.Term > rf.currentTerm {
				rf.info("follower's term is larger than current candidate current=%v next=%v", rf.currentTerm, res.Term)

				updateTerm = max(updateTerm, res.Term)
				return
			}

			if res.VoteGranted {
				votes++
			}
		}()
	}

	// wait for all goroutines done
	wg.Wait()

	// follower's term > current term, update term and convert to follower
	if updateTerm > -1 {
		rf.updateTerm(updateTerm)
		return ROLE_FOLLOWER
	}

	rf.info("get votes number votes=%v atLeast=%v", votes, int64(math.Ceil(float64(len(rf.peers))/2)))

	if votes >= int64(math.Ceil(float64(len(rf.peers))/2)) {
		rf.info("server was elected as leader id=%v votes=%v", rf.me, votes)
		rf.initializeLeaderState()
		return ROLE_LEADER
	}

	return ROLE_FOLLOWER
}

func (rf *Raft) appendEntriesToAllServer(entries []LogEntry) {
	rf.debug("leader: send AppendEntries to all servers")

	var mu sync.Mutex
	var wg sync.WaitGroup

	prevLogIndex := len(rf.log)
	prevLogTerm := int64(-1)

	nextTerm := rf.currentTerm

	// first heartbeat
	if prevLogIndex != 0 {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}

	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()
			for {
				req := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      []LogEntry{}, // TODO: entries
					// TODO: leaderCommit
				}
				res := AppendEntriesReply{}

				rf.debug("AppendEntries to peer=%v", index)

				ok := RunInTimeLimit(RPC_TIMEOUT_MS, func() {
					rf.sendAppendEntries(index, &req, &res)
				})

				if !ok {
					rf.error("AppendEntries call failed peerIndex=%v", index)
					return
				}

				if res.Success {
					rf.debug("AppendEntries success peerIndex=%v", index)
					return
				}

				mu.Lock()

				// follower's term is ahead of leader
				if res.Term > nextTerm {
					nextTerm = res.Term
					mu.Unlock()
					return
				}

				// follower's log is beheind leader, append [lastIndex, latest] to follower
				// TODO
				if res.LastIndex >= 0 {
					rf.debug("retry due to lastIndex > 0")
					mu.Unlock()
					continue
				}

				mu.Unlock()
				break
			}

			rf.debug("AppendEntries successfully returns peer=%d", index)
		}()
	}

	wg.Wait()
	rf.debug("all followers have replied AppendEntries")

	// convert to follower if term behind happened
	if nextTerm > rf.currentTerm {
		rf.warn("leader identity is expired due to term behind incoming=%v current=%v", nextTerm, rf.currentTerm)
		rf.updateTerm(nextTerm)
		rf.convertToRole(ROLE_FOLLOWER)
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
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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

func (rf *Raft) ticker() {
	// first wait for a random timeout to prevent all servers go to candidate simultaneously
	time.Sleep(time.Duration(rf.electionTimeoutMs) * time.Millisecond)

	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()

		// Follower
		if role == ROLE_FOLLOWER {
			rf.mu.Lock()

			if !rf.checkShouldStartNewElection() {
				rf.mu.Unlock()
				goto nextTick
			}

			rf.info("starting new election")

			// start a new leader election if time is expired:
			// - increment current term by 1
			// - convert to candidate and vote for self
			// - reset election timer (TODO)
			// - send `RequestVote` rpc to all servers
			// 	- if gained majority of votes
			// 	- else convert to follower again
			rf.updateTerm()
			rf.convertToRole(ROLE_CANDIDATE)

			ok := RunInTimeLimit(rf.electionTimeoutMs, func() {
				nextRole := rf.requestVoteForAllServer()
				rf.convertToRole(nextRole)
			})

			if !ok {
				rf.warn("election timeout is exceeded tiemoutMs=%v", rf.electionTimeoutMs)
				rf.convertToRole(ROLE_FOLLOWER)
			}

			rf.mu.Unlock()
		}

		if role == ROLE_LEADER {
			// send heartbeat to all peers
			rf.mu.Lock()
			rf.appendEntriesToAllServer([]LogEntry{})
			rf.mu.Unlock()
		}

	nextTick:
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// #region internal methods
func (rf *Raft) convertToRole(role int32) {
	prevRole := rf.role
	rf.role = role

	if prevRole == role {
		return
	}

	if role == ROLE_FOLLOWER {
		rf.votedFor = NOT_VOTED
	}

	if role == ROLE_CANDIDATE {
		rf.votedFor = rf.me
	}

	rf.info("role convertion happened prevRole=%v nextRole=%v", rf.getRoleName(prevRole), rf.getRoleName(role))
}

func (rf *Raft) updateTerm(term ...int64) {
	prevTerm := rf.currentTerm

	if len(term) == 0 {
		rf.currentTerm++
	} else {
		rf.currentTerm = term[0]
	}

	rf.info("server term updated prevTerm=%d nextTerm=%d", prevTerm, atomic.LoadInt64(&rf.currentTerm))
}

// check that leader or candidate is dead and should start a new election
func (rf *Raft) checkShouldStartNewElection() bool {
	now := time.Now().UnixMilli()
	shouldStartElection := now-rf.lastReceiveRpcTimestamp > rf.electionTimeoutMs
	if shouldStartElection {
		rf.debug("should start an election now=%v lastReceive=%v diffMs=%v", now, rf.lastReceiveRpcTimestamp, now-rf.lastReceiveRpcTimestamp)
	}
	return shouldStartElection
}

func (rf *Raft) initializeLeaderState() {
	rf.debug("initialize leader volitle state")

	lastLogIndex := len(rf.log)
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range len(rf.peers) {
		rf.nextIndex[i] = lastLogIndex
	}

	rf.matchIndex = make([]int, len(rf.peers))
}

// #endregion

// #region log utils
func (rf *Raft) getRoleName(role int32) string {
	if role == ROLE_LEADER {
		return "leader"
	}

	if role == ROLE_CANDIDATE {
		return "candidate"
	}

	return "follower"
}

func (rf *Raft) getRole() string {
	role := atomic.LoadInt32(&rf.role)
	return rf.getRoleName(role)
}

func (rf *Raft) debug(format string, args ...any) {
	Log(LEVEL_DEBUG, rf.getRole(), rf.me, rf.currentTerm, format, args...)
}
func (rf *Raft) info(format string, args ...any) {
	Log(LEVEL_INFO, rf.getRole(), rf.me, rf.currentTerm, format, args...)
}
func (rf *Raft) warn(format string, args ...any) {
	Log(LEVEL_WARN, rf.getRole(), rf.me, rf.currentTerm, format, args...)
}
func (rf *Raft) error(format string, args ...any) {
	Log(LEVEL_ERROR, rf.getRole(), rf.me, rf.currentTerm, format, args...)
}

// #endregion log utils

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

	// Your initialization code here (3A, 3B, 3C).
	rf.electionTimeoutMs = randRange(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS)
	rf.role = ROLE_FOLLOWER

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
