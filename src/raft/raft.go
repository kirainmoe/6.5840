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
	RPC_TIMEOUT_MS          = 10
)

// A Go object implementing a single Raft peer.
type LogEntry struct {
	Index   int
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

	applyCh chan ApplyMsg

	// server role (ROLE_*)
	role int32
	// election timeout, random by default
	electionTimeoutMs int64
	// timestamp of last AppendEntries RPC or
	lastReceiveRpcTimestamp int64
	// timestamp of last received RequestVote and granted in follower role
	lastGrantVoteTimestamp int64

	// persistant state on all servers, should be update to stable storage before responding RPCs

	// latest term this server has been seen, default is 0
	currentTerm int64
	// candidateId that this server has voted for in current term, -1 if not voted
	votedFor int
	// log entries
	log []LogEntry

	// volatile state on all servers

	// index of highest log entry known to be committed (from leader)
	commitIndex int
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
	role := atomic.LoadInt32(&rf.role)
	isleader = role == ROLE_LEADER

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

// #region RequestVote
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
	rf.info("received RequestVote args=%+v", args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.checkConvertToFollower(args.Term, args.CandidateId)

	// candidate term behind
	if args.Term < rf.currentTerm {
		rf.info("candidate's term is behind follower's term candidate=%v incoming=%v current=%v",
			args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// already voted others in this term
	if args.Term == rf.currentTerm && rf.votedFor != NOT_VOTED {
		rf.info("already voted in current term candidate=%v term=%v voted=%v",
			args.CandidateId, args.Term, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	// candidate's term is ahead of current server
	if args.Term > rf.currentTerm && rf.role != ROLE_FOLLOWER {
		rf.info("candidate term is ahead of current server, transform to follower incoming=%v current=%v",
			args.Term, rf.currentTerm)
		rf.updateTerm(args.Term) // TODO: should update term here?
		rf.convertToRole(ROLE_FOLLOWER)
	}

	curLogLen := len(rf.log)

	// checks candidate's log is at least as up-to-date receiver's log
	// if args.LastLogIndex < curLogLen {
	// 	rf.info("candidate's log is behind current candidate=%v candidateLast=%v selfLast=%v",
	// 		args.CandidateId, args.LastLogIndex, curLogLen)
	// 	reply.VoteGranted = false
	// 	return
	// }

	// candidate's log term is conflict with current log (5.4.1)
	if args.LastLogIndex > 0 && curLogLen > 0 {
		lastTerm := rf.log[curLogLen-1].Term

		rf.debug("comparing whose log is newer: incomingTerm=%v curTerm=%v incomingLen=%v curLen=%v",
			args.LastLogTerm, lastTerm, args.LastLogIndex, curLogLen)

		// compare term, the later term is newer
		if args.LastLogTerm < lastTerm {
			rf.info(
				"candidate's log is outdated because term is behind incomingLen=%v incomingTerm=%v curLen=%v curTerm=%v",
				args.LastLogIndex, args.LastLogTerm, curLogLen, lastTerm,
			)
			reply.VoteGranted = false
			return
		}

		// if term is the same, the longer log is newer
		if args.LastLogTerm == lastTerm && args.LastLogIndex < curLogLen {
			rf.info("candidate's last log term is equal to current, but candidate's log is shorter incoming=%v current=%v",
				args.LastLogIndex, curLogLen)
			reply.VoteGranted = false
			return
		}
	}

	rf.updateLastGrantVoteTime()
	rf.updateTerm(args.Term) // TODO: should update term here?
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.info("voted for the candidate in current term candidate=%v term=%v", args.CandidateId, args.Term)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// return next role after vote
func (rf *Raft) requestVoteForAllServer() int32 {
	rf.info("candidate requesting vote from all servers")
	rf.info("time=%v", time.Now().UnixMilli())

	req := LockAndRun(rf, func() RequestVoteArgs {
		lastLogIndex := len(rf.log)
		request := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
		}
		if lastLogIndex > 0 {
			request.LastLogTerm = rf.log[lastLogIndex-1].Term
		}
		return request
	})

	rf.info("wake time=%v", time.Now().UnixMilli())

	// concurrently request vote from all peers (except self)
	result := SendRPCToAllPeersConcurrently(rf, "RequestVote", func(peerIndex int) *RequestVoteReply {
		res := RequestVoteReply{}
		rf.sendRequestVote(peerIndex, &req, &res)
		return &res
	})

	rf.info("RequestVote returned")

	votes := int64(1) // granted to self
	updateTerm := int64(-1)

	// summary votes and update known terms from peers
	ForEachPeers(rf, func(index int) {
		res := result[index]
		if !res.Ok {
			return
		}
		reply := result[index].Result.(*RequestVoteReply)
		updateTerm = max(updateTerm, reply.Term)
		if reply.VoteGranted {
			votes++
		}
	})

	// follower's term > current term, update term and convert to follower
	if updateTerm > rf.currentTerm {
		rf.info("follower's term is larger than current candidate current=%v next=%v", rf.currentTerm, updateTerm)
		rf.updateTerm(updateTerm)
		return ROLE_FOLLOWER
	}

	// if gained majority of votes, current server is elected as leader
	rf.info("get votes number votes=%v atLeast=%v peers=%v", votes, GetMajority(len(rf.peers)), len(rf.peers))

	if votes >= GetMajority(len(rf.peers)) {
		rf.success("server was elected as leader id=%v votes=%v", rf.me, votes)
		rf.initializeLeaderState()
		return ROLE_LEADER
	}

	rf.warn("election failed due to not gained enough votes, return to follower")
	rf.votedFor = NOT_VOTED

	return ROLE_FOLLOWER
}

// #endregion

// #region AppendEntries
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

	rf.checkConvertToFollower(args.Term, args.LeaderId)

	reply.LastIndex = -1

	if rf.currentTerm < args.Term {
		rf.warn("meet a larger term of AppendEntries, converting to follower peer=%d", args.LeaderId)
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
	if args.PrevLogIndex != 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		thisTerm := rf.log[args.PrevLogIndex-1].Term
		rf.info("follower's log has prevLogIndex, but term is different index=%v incoming=%v expecting=%v",
			args.PrevLogIndex, args.PrevLogTerm, thisTerm)
		// find an index of this term
		index := 0
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if thisTerm != rf.log[i].Term {
				index = i
				break
			}
		}
		reply.Success = false
		reply.LastIndex = index + 1
		rf.info("the first entry that has term=%v index=%v", thisTerm, reply.LastIndex)
		return
	}

	// slice rf.log
	if args.PrevLogIndex != 0 {
		rf.debug("slice rf.log to [0,%v)", args.PrevLogIndex)
		rf.log = rf.log[0:args.PrevLogIndex]
	}

	// append new entries to log
	rf.appendLogEntries(&args.Entries)
	rf.debug("append %d logs, lastLogIndex=%v", len(args.Entries), len(rf.log))

	// update follower's term
	rf.updateTerm(max(rf.currentTerm, args.Term))

	// update last receive time
	rf.updateLastReceiveRpcTime()

	// commit logs
	if args.LeaderCommit > int64(rf.commitIndex) && args.LeaderCommit <= int64(len(rf.log)) {
		rf.commitLogs(rf.commitIndex+1, int(args.LeaderCommit))
	}

	// response RPCs
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) appendEntriesToAllServer(entries *[]LogEntry) {
	rf.debug("leader: send AppendEntries to all servers length=%v", len(*entries))

	var nextTerm int64
	var maxCommitInThisRequest int

	LockAndRun(rf, func() bool {
		nextTerm = rf.currentTerm

		// append logs to leader
		rf.appendLogEntries(entries)
		maxCommitInThisRequest = len(rf.log)

		return true
	})

	result := SendRPCToAllPeersConcurrently(rf, "AppendEntries", func(peerIndex int) *AppendEntriesReply {
		return rf.sendAppendEntriesToSpecificPeer(peerIndex)
	})

	successPeers := int64(0)
	ForEachPeers(rf, func(index int) {
		ok := result[index].Ok
		payload := result[index].Result.(*AppendEntriesReply)

		ok, maybeNextTerm := rf.handleAppendEntriesResult(index, ok, payload)
		if ok {
			successPeers++
			return
		}
		if maybeNextTerm > 0 {
			nextTerm = max(nextTerm, maybeNextTerm)
		}
	})

	rf.debug("followers have replied AppendEntries num=%v", successPeers)

	LockAndRun(rf, func() bool {
		// convert to follower if term behind happened
		if nextTerm > rf.currentTerm {
			rf.warn("leader identity is expired due to term behind incoming=%v current=%v", nextTerm, rf.currentTerm)
			rf.updateTerm(nextTerm)
			rf.convertToRole(ROLE_FOLLOWER)
		}

		// commit log if majority is returned
		// +1: self
		if successPeers+1 >= GetMajority(len(rf.peers)) {
			rf.success("get majority server AppendEntries success, we can commit now succ=%v peers=%v",
				successPeers+1, len(rf.peers))
			prevCommited := rf.commitIndex
			if prevCommited+1 <= maxCommitInThisRequest {
				rf.commitLogs(prevCommited+1, maxCommitInThisRequest)
			}
		}

		return true
	})
}

func (rf *Raft) handleAppendEntriesResult(
	index int,
	ok bool,
	payload *AppendEntriesReply,
) (bool, int64) {
	rf.debug("peer returns inspect: index=%v return=%v", index, payload)

	if !ok {
		rf.error("AppendEntries call failed peerIndex=%v", index)
		return false, -1
	}

	if payload.Success {
		rf.debug("AppendEntries success peerIndex=%v", index)

		// update nextIndex
		LockAndRun(rf, func() bool {
			rf.updateNextIndex(index, -1)
			return true
		})

		return true, -1
	}

	// follower's term is ahead of leader
	if payload.Term > rf.getTerm() {
		rf.debug("Follower term is larger thean leader incoming=%v currentCache=%v", payload.Term, rf.getTerm())

		return false, payload.Term
	}

	// follower's log is beheind leader, append [lastIndex, latest] to follower
	if payload.LastIndex > 0 {
		rf.warn("retry due to lastIndex > 0, update lastIndex=%v", payload.LastIndex)

		LockAndRun(rf, func() bool {
			rf.updateNextIndex(index, payload.LastIndex)
			return true
		})

		go rf.retryAppendEntries(index) // retry in goroutine
		return false, -1
	}

	return false, -1
}

func (rf *Raft) sendAppendEntriesToSpecificPeer(peerIndex int) *AppendEntriesReply {
	var req AppendEntriesArgs
	LockAndRun(rf, func() bool {
		// get previous sent log index and its term for peer i
		prevLogIndex := rf.nextIndex[peerIndex]
		prevLogTerm := int64(-1)

		if prevLogIndex > 0 { // already sent log
			prevLogTerm = rf.log[prevLogIndex-1].Term
		}

		// construct entries to send
		logEntries := make([]LogEntry, len(rf.log)-prevLogIndex)
		copy(logEntries, rf.log[prevLogIndex:len(rf.log)]) // [prevLogIndex+1, len(rf.log))

		rf.info("send AppendEntries to peer=%v from=%v to=%v prevLogIndex=%v prevTerm=%v",
			peerIndex, prevLogIndex, len(rf.log), prevLogIndex, prevLogTerm)

		req = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      logEntries,
			LeaderCommit: int64(rf.commitIndex), // follower can commit from: [rfFollower.commitIndex, leaderCommit+1)
		}
		return true
	})

	res := AppendEntriesReply{}

	// send AppendEntries infinitely if fails
	for {
		ok := rf.sendAppendEntries(peerIndex, &req, &res)
		if ok {
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	return &res
}

func (rf *Raft) retryAppendEntries(peerIndex int) {
	result := rf.sendAppendEntriesToSpecificPeer(peerIndex)
	rf.handleAppendEntriesResult(peerIndex, true, result)
}

// #endregion

// #region Log Commit

// Commit logs from [fromIndex, toIndex] to state machine
// should subtract 1, since index is stored from 1 in Raft's definition
func (rf *Raft) commitLogs(fromIndex int, toIndex int) {
	start := fromIndex - 1
	end := toIndex
	entries := rf.log[start:end]

	rf.info("commit logs fromTo=[%v, %v] fromToInGo=[%v, %v)", fromIndex, toIndex, start, end)

	for index, entry := range entries {
		rf.debug("current entry=%v", entry)
		currentLogIndex := fromIndex + index // in Raft's presentation

		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Payload,
			CommandIndex: currentLogIndex,
		}

		rf.applyCh <- msg

		rf.debug("commit log index=%v payload=%v complete", currentLogIndex, entry.Payload)

		rf.commitIndex = currentLogIndex // in Raft's presentation
	}
}

// #endregion

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
	// rf.mu.Lock()
	role := atomic.LoadInt32(&rf.role)
	currentTerm := atomic.LoadInt64(&rf.currentTerm)
	term = int(currentTerm)
	isLeader = role == ROLE_LEADER
	// rf.mu.Unlock()

	// if current role is not leader, just return and redirect the request to leader
	// otherwise we can append this log and start an entry
	if isLeader {
		rf.info("receive client request payload=%v", command)

		// metric1 := time.Now().UnixMilli()

		logEntries := []LogEntry{
			{
				Term:    currentTerm,
				Payload: command,
			},
		}

		rf.appendEntriesToAllServer(&logEntries)

		currentLogIndex := logEntries[0].Index
		// commited := rf.commitIndex >= currentLogIndex
		// // if !commited {
		// // 	rf.debug("log is not committed, should not start agreement with this index=%v commit=%v",
		// // 		currentLogIndex, rf.commitIndex)
		// // 	currentLogIndex = -1
		// // }

		// metric2 := time.Now().UnixMilli()

		// log.Println("start agreement in ", metric2-metric1, "ms")

		rf.debug("start agreement with logIndex=%v term=%v isLeader=%v command=%v",
			currentLogIndex, term, isLeader, command)

		return currentLogIndex, term, isLeader
	}

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

	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		role := atomic.LoadInt32(&rf.role)

		// Follower
		if role == ROLE_FOLLOWER {
			if !rf.checkShouldStartNewElection() {
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

			ok, _ := RunInTimeLimit(rf.electionTimeoutMs, func() bool {
				nextRole := rf.requestVoteForAllServer()
				rf.convertToRole(nextRole)
				return true
			})

			if !ok {
				rf.warn("election timeout is exceeded tiemoutMs=%v", rf.electionTimeoutMs)
				rf.convertToRole(ROLE_FOLLOWER)
			}

			rf.debug("follower next role=%v", rf.getRoleName(rf.getRole()))
		}

		if role == ROLE_LEADER {
			// send heartbeat to all peers
			rf.debug("sending heartbeats to all peers")
			rf.appendEntriesToAllServer(&[]LogEntry{})
		}

	nextTick:
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

	rf.warn("serveer has been killed peer=%v", rf.me)
}

// #region internal methods
func (rf *Raft) convertToRole(role int32) {
	prevRole := rf.role
	atomic.StoreInt32(&rf.role, role)

	if prevRole == role {
		return
	}

	if role == ROLE_CANDIDATE {
		rf.votedFor = rf.me
	}

	rf.info("role convertion happened prevRole=%v nextRole=%v", rf.getRoleName(prevRole), rf.getRoleName(role))
}

func (rf *Raft) updateTerm(term ...int64) {
	if rf.mu.TryLock() {
		defer rf.mu.Unlock()
	}

	prevTerm := atomic.LoadInt64(&rf.currentTerm)

	if len(term) == 0 {
		atomic.AddInt64(&rf.currentTerm, 1)
	} else {
		atomic.StoreInt64(&rf.currentTerm, term[0])
	}

	curTerm := atomic.LoadInt64(&rf.currentTerm)
	if prevTerm != curTerm {
		rf.info("server term updated prevTerm=%d nextTerm=%d", prevTerm, curTerm)
	}

	if prevTerm < curTerm {
		rf.votedFor = NOT_VOTED
	}
}

// Refresh timestamp of last receiving RPC. This method is lock-free.
func (rf *Raft) updateLastReceiveRpcTime() {
	now := time.Now().UnixMilli()
	atomic.StoreInt64(&rf.lastReceiveRpcTimestamp, now)
}

func (rf *Raft) updateLastGrantVoteTime() {
	now := time.Now().UnixMilli()
	atomic.StoreInt64(&rf.lastGrantVoteTimestamp, now)
}

// check that leader or candidate is dead and should start a new election
func (rf *Raft) checkShouldStartNewElection() bool {
	if rf.mu.TryLock() {
		defer rf.mu.Unlock()
	}

	now := time.Now().UnixMilli()

	receiveAppendEntriesTimeout := (now-rf.lastReceiveRpcTimestamp > rf.electionTimeoutMs)
	voteTimeout := now-rf.lastGrantVoteTimestamp > rf.electionTimeoutMs

	shouldStartElection := receiveAppendEntriesTimeout && voteTimeout
	if shouldStartElection {
		rf.debug("should start an election rpcTimeout=%v voteTimeout=%v",
			receiveAppendEntriesTimeout, voteTimeout)
	} else {
		rf.debug("no need to restart an election rpcTimeout=%v diff=%v<=%v voteTimeout=%v",
			receiveAppendEntriesTimeout, now-rf.lastReceiveRpcTimestamp, rf.electionTimeoutMs, voteTimeout)
	}

	return shouldStartElection
}

func (rf *Raft) checkConvertToFollower(term int64, peer int) {
	currentTerm := atomic.LoadInt64(&rf.currentTerm)
	if currentTerm >= term {
		return
	}

	rf.warn("received RPC from other peers that term larger than self, convert to follower peer=%v incoming=%v current=%v", peer, term, currentTerm)
	rf.updateTerm(term)
	rf.convertToRole(ROLE_FOLLOWER)
}

func (rf *Raft) initializeLeaderState() {
	rf.debug("initialize leader volitle state")

	lastLogIndex := len(rf.log)
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range len(rf.peers) {
		rf.nextIndex[i] = lastLogIndex
	}

	rf.matchIndex = make([]int, len(rf.peers))
	rf.votedFor = rf.me
}

func (rf *Raft) appendLogEntries(entries *[]LogEntry) {
	rf.debug("appendLogEntries to self logs len=%v entries=%v", len(*entries), *entries)

	if rf.mu.TryLock() {
		defer rf.mu.Unlock()
	}

	curLen := len(rf.log)

	for i := range *entries {
		entry := &((*entries)[i])
		// if entry has no index, current is leader's first append, mark an index
		if entry.Index == 0 {
			(*entries)[i].Index = curLen + i + 1
		}

		// otherwise, the entry is received from leaders
		if entry.Index <= curLen {
			rf.debug("override log entry at index=%v", entry.Index)
			rf.log[entry.Index-1] = *entry // override log items
		} else {
			rf.log = append(rf.log, *entry)
		}
	}

	rf.debug("append %v log entries to self logs currentLogs=%v", len(*entries), len(rf.log))
}

func (rf *Raft) updateNextIndex(peerIndex int, value int) {
	var nextValue int
	if value == -1 {
		nextValue = len(rf.log)
	} else {
		nextValue = min(value, len(rf.log))
	}
	rf.nextIndex[peerIndex] = nextValue

	rf.debug("update nextIndex peer=%v nextIndex=%v", peerIndex, nextValue)
}

// #endregion

// #region Log Utils
func (rf *Raft) getRoleName(role int32) string {
	if role == ROLE_LEADER {
		return "leader"
	}

	if role == ROLE_CANDIDATE {
		return "candidate"
	}

	return "follower"
}

func (rf *Raft) getRole() int32 {
	role := atomic.LoadInt32(&rf.role)
	return role
}

func (rf *Raft) getTerm() int64 {
	term := atomic.LoadInt64(&rf.currentTerm)
	return term
}

func (rf *Raft) debug(format string, args ...any) {
	Log(LEVEL_DEBUG, rf.getRoleName(rf.getRole()), rf.me, rf.getTerm(), format, args...)
}
func (rf *Raft) info(format string, args ...any) {
	Log(LEVEL_INFO, rf.getRoleName(rf.getRole()), rf.me, rf.getTerm(), format, args...)
}
func (rf *Raft) warn(format string, args ...any) {
	Log(LEVEL_WARN, rf.getRoleName(rf.getRole()), rf.me, rf.getTerm(), format, args...)
}
func (rf *Raft) error(format string, args ...any) {
	Log(LEVEL_ERROR, rf.getRoleName(rf.getRole()), rf.me, rf.getTerm(), format, args...)
}
func (rf *Raft) fatal(format string, args ...any) {
	Log(LEVEL_FATAL, rf.getRoleName(rf.getRole()), rf.me, rf.getTerm(), format, args...)
}
func (rf *Raft) success(format string, args ...any) {
	Log(LEVEL_SUCCESS, rf.getRoleName(rf.getRole()), rf.me, rf.getTerm(), format, args...)

}

// #endregion

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
	rf.electionTimeoutMs = int64(rf.me)*200 + 1000 // randRange(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS)
	rf.role = ROLE_FOLLOWER
	rf.applyCh = applyCh
	rf.votedFor = NOT_VOTED

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
