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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// var RVcount, AEcount, HBcount, RetryCount int64
// var LastRVcount, LastAEcount, LastTimestamp int64

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

// timeouts
const (
	ELECTION_TIMEOUT_MIN_MS = 800
	ELECTION_TIMEOUT_MAX_MS = 2000
	RPC_TIMEOUT_MS          = 30
	RPC_RETRY_MS            = 500
)

// LogEntry is the presentation of single entry item type structure in Raft
type LogEntry struct {
	Index   int
	Term    int64
	Payload any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	commitCh chan LogCommitRequest

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	// server role (ROLE_*)
	role int32
	// election timeout, random by default
	electionTimeoutMs int64
	// timestamp of last AppendEntries RPC
	lastReceiveRpcTimestamp int64
	// timestamp of last received RequestVote and granted in follower role
	lastGrantVoteTimestamp int64

	// persistant state on all servers, should be update to stable storage before responding RPCs

	// (persist) latest term this server has been seen, default is 0
	currentTerm int64
	// (persist) candidateId that this server has voted for in current term, -1 if not voted
	votedFor int32
	// (persist) log entries
	log []LogEntry

	// (persist) the index of last compacted log
	compactedLogIndex int
	// (persist) the term of last compacted log
	compactedLogTerm int64

	// volatile state on all servers
	// `lastApplied` is not implemented

	// index of highest log entry known to be committed (from leader)
	commitIndex int

	// volatile state on leaders, reinitialized after election
	// `matchIndex` is not implemented

	// for each followers, index of the next log entry to send
	nextIndex []int

	snapshot []byte

	// internal
	logID int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = int(rf.getTerm())
	role := atomic.LoadInt32(&rf.role)
	isleader = role == ROLE_LEADER

	return term, isleader
}

// #region Persist

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(ctx context.Context) {
	// Your code here (3C).
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	// encode persist states
	encoder.Encode(rf.getTerm())
	encoder.Encode(atomic.LoadInt32(&rf.votedFor))
	encoder.Encode(rf.log)

	// snapshot related
	encoder.Encode(rf.compactedLogIndex)
	encoder.Encode(rf.compactedLogTerm)

	rf.persister.Save(buffer.Bytes(), rf.snapshot)

	rf.success(ctx, DevLog{
		"message":  "saving persist state",
		"term":     rf.getTerm(),
		"votedFor": rf.getVotedFor(),
		"logLen":   rf.getCurrentLogLength(),
		"snapLen":  len(rf.snapshot),
	})
}

// restore previously persisted state.
func (rf *Raft) readPersist(ctx context.Context, data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	// base persist states
	TryDecode(ctx, rf, decoder, "rf.currentTerm", &rf.currentTerm)

	var votedFor int32
	TryDecode(ctx, rf, decoder, "rf.votedFor", &votedFor)
	rf.votedFor = votedFor

	TryDecode(ctx, rf, decoder, "rf.log", &rf.log)

	// decode snapshot related
	TryDecode(ctx, rf, decoder, "rf.compactedLogIndex", &rf.compactedLogIndex)
	TryDecode(ctx, rf, decoder, "rf.compactedLogTerm", &rf.compactedLogTerm)

	rf.success(ctx, DevLog{
		"message":   "read persist state successful",
		"term":      rf.getTerm(),
		"votedFor":  rf.getVotedFor(),
		"logLen":    rf.getCurrentLogLength(),
		"snapIndex": rf.compactedLogIndex,
		"snapTerm":  rf.compactedLogTerm,
	})
}

// #endregion

// #region Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	ctx := context.Background()

	rf.info(ctx, DevLog{
		"message": "received snapshot ready request, compacting logs",
		"index":   index,
	})

	rf.mu.Lock()
	defer rf.mu.Unlock()

	ok, term := rf.compactLogsToIndex(ctx, index)
	if !ok {
		return
	}

	// persist logs and commit snapshot
	rf.snapshot = snapshot
	rf.persist(ctx)

	rf.success(ctx, DevLog{
		"message": "successfully created snapshot",
		"index":   index,
		"term":    term,
	})
}

type InstallSnapshotArgs struct {
	Term              int64
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int64
	Data              []byte
	// offset and chunk mechanisms are not implemented.

	// internal
	LogID int64
}

type InstallSnapshotReply struct {
	Term int64

	// internal
	LogID int64
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ctx := context.WithValue(context.Background(), ContextKeyLogID, args.LogID)
	reply.LogID = args.LogID

	rf.debug(ctx, DevLog{
		"message":          "received InstallSnapshot request",
		"leader":           args.LeaderId,
		"lastIncludeIndex": args.LastIncludedIndex,
		"lastIncludeTerm":  args.LastIncludedTerm,
		"length":           len(args.Data),
		"incomingTerm":     args.Term,
	})

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.checkConvertToFollower(ctx, args.Term, args.LeaderId)

	// leader's term is behind current's term, reject snapshot
	if args.Term < rf.getTerm() {
		rf.warn(ctx, DevLog{
			"message":  "leader term is behind current server, rejected install snapshot",
			"incoming": args.Term,
			"current":  rf.getTerm(),
		})

		reply.Term = rf.getTerm()
		return
	}

	rf.updateLastReceiveRpcTime()

	// apply snapshot
	rf.info(ctx, DevLog{
		"message": "apply snapshot to state machine",
		"term":    args.LastIncludedTerm,
		"index":   args.LastIncludedIndex,
		"len":     len(args.Data),
	})

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		SnapshotTerm:  int(args.LastIncludedTerm),
		SnapshotIndex: args.LastIncludedIndex,
		Snapshot:      args.Data,
	}
	rf.applyCh <- applyMsg

	rf.commitIndex = args.LastIncludedIndex
	rf.compactedLogIndex = args.LastIncludedIndex
	rf.compactedLogTerm = args.LastIncludedTerm
	rf.snapshot = args.Data

	// discard logs
	rf.discardLogsUntil(ctx, args.LastIncludedIndex)

	rf.success(ctx, DevLog{
		"message":        "follower InstallSnapshot success",
		"compactedIndex": rf.compactedLogIndex,
		"bufferLen":      len(rf.log),
	})
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshotToPeer(ctx context.Context, peerIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logID := ctx.Value(ContextKeyLogID).(int64)

	if rf.getRole() != ROLE_LEADER {
		rf.warn(ctx, DevLog{"message": "leader identity has changed, stop InstallSnapshot"})
		return
	}

	rf.info(ctx, DevLog{"message": "send InstallSnapshot to peer", "peer": peerIndex})

	installSnapshotArgs := InstallSnapshotArgs{
		Term:              rf.getTerm(),
		LeaderId:          rf.me,
		LastIncludedIndex: rf.compactedLogIndex,
		LastIncludedTerm:  rf.compactedLogTerm,
		Data:              rf.snapshot,
		LogID:             logID,
	}
	installSnapshotReply := InstallSnapshotReply{}

	done, ok := RunInTimeLimit(RPC_TIMEOUT_MS, func() bool {
		return rf.sendInstallSnapshot(peerIndex, &installSnapshotArgs, &installSnapshotReply)
	})

	if !done || !ok {
		rf.error(ctx, DevLog{
			"message": "send InstallSnapshot request failed or timeout",
			"timeout": done,
			"ok":      ok,
		})
		return
	}

	rf.checkConvertToFollower(ctx, installSnapshotReply.Term, peerIndex)
	rf.updateNextIndex(ctx, peerIndex, rf.compactedLogIndex)

	rf.success(ctx, DevLog{"message": "successfully installed snapshot to peer", "peer": peerIndex})
}

// #endregion

// #region RequestVote
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	CandidateId  int
	Term         int64
	LastLogIndex int
	LastLogTerm  int64

	// internal
	LogID int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64
	VoteGranted bool

	// internal
	LogID int64
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	ctx := context.WithValue(context.Background(), ContextKeyLogID, args.LogID)
	reply.LogID = args.LogID

	// Your code here (3A, 3B).
	rf.info(ctx, DevLog{
		"message": "receive RequestVote RPC",
		"args":    args,
	})

	reply.LogID = args.LogID

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.checkConvertToFollower(ctx, args.Term, args.CandidateId)

	// candidate term behind
	if args.Term < rf.getTerm() {
		rf.warn(ctx, DevLog{
			"event":       "VOTE_NOT_GRANTED",
			"message":     "candidate's term is behind follower's term",
			"candidateID": args.CandidateId,
			"incoming":    args.Term,
			"current":     rf.getTerm(),
		})

		reply.VoteGranted = false
		reply.Term = rf.getTerm()
		return
	}

	// already voted others in this term
	if args.Term == rf.getTerm() && rf.getVotedFor() != NOT_VOTED {
		rf.warn(ctx, DevLog{
			"event":       "VOTE_NOT_GRANTED",
			"message":     "already voted in current term",
			"candidateID": args.CandidateId,
			"term":        rf.getTerm(),
			"votedFor":    rf.getVotedFor(),
		})

		reply.VoteGranted = false
		return
	}

	curLogLen := rf.getCurrentLogLength()

	// candidate's log term is conflict with current log (5.4.1)
	if args.LastLogIndex >= 0 && curLogLen > 0 {
		lastTerm := rf.getTermOfLogIndex(ctx, curLogLen)

		// compare term, the later term is newer
		if args.LastLogTerm < lastTerm {
			rf.warn(ctx, DevLog{
				"event":          "VOTE_NOT_GRANTED",
				"message":        "candidate's log is outdated because term is behind",
				"candidateID":    args.CandidateId,
				"incomingLength": args.LastLogIndex,
				"incomingTerm":   args.LastLogTerm,
				"currentLength":  curLogLen,
				"currentTerm":    lastTerm,
			})

			reply.VoteGranted = false
			return
		}

		// if term is the same, the longer log is newer
		if args.LastLogTerm == lastTerm && args.LastLogIndex < curLogLen {
			rf.warn(ctx, DevLog{
				"event":          "VOTE_NOT_GRANTED",
				"message":        "candidate's last log term is equal to current, but candidate's log is shorter",
				"incomingLength": args.LastLogIndex,
				"currentLength":  curLogLen,
			})

			reply.VoteGranted = false
			return
		}
	}

	rf.updateLastGrantVoteTime()
	rf.updateTerm(ctx, args.Term) // TODO: should update term here?
	rf.updateVotedFor(ctx, int32(args.CandidateId))
	reply.VoteGranted = true

	rf.info(ctx, DevLog{
		"event":     "VOTE_GRANTED",
		"message":   "voted for the candidate in current term",
		"candidate": args.CandidateId,
		"term":      args.Term,
	})
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// return next role after vote
func (rf *Raft) requestVoteForAllServer(ctx context.Context) int32 {
	rf.info(ctx, DevLog{"message": "candidate requesting vote from all servers"})

	req := LockAndRun(rf, func() RequestVoteArgs {
		lastLogIndex := rf.getCurrentLogLength()
		logID := rf.GetLogID()
		request := RequestVoteArgs{
			Term:         rf.getTerm(),
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  rf.getTermOfLogIndex(ctx, rf.getCurrentLogLength()),
			LogID:        logID,
		}

		return request
	})

	// concurrently send RequestVote rpc to all peers (except self)
	result := SendRPCToAllPeersConcurrently(rf, "RequestVote", func(peerIndex int) *RequestVoteReply {
		res := RequestVoteReply{}
		rf.sendRequestVote(peerIndex, &req, &res)
		return &res
	})

	rf.debug(ctx, DevLog{"message": "RequestVote returned"})

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
	if updateTerm > rf.getTerm() {
		rf.warn(ctx, DevLog{
			"message":  "follower's term is larger than current candidate",
			"current":  rf.getTerm(),
			"incoming": updateTerm,
		})

		rf.updateTerm(ctx, updateTerm)
		return ROLE_FOLLOWER
	}

	// if gained majority of votes, current server is elected as leader
	rf.info(ctx, DevLog{
		"message": "get votes summary",
		"votes":   votes,
		"least":   GetMajority(len(rf.peers)),
		"peers":   len(rf.peers),
	})

	if votes >= GetMajority(len(rf.peers)) {
		rf.success(ctx, DevLog{
			"message": "server was elected as leader",
			"id":      rf.me,
			"votes":   votes,
		})

		rf.initializeLeaderState(ctx)
		return ROLE_LEADER
	}

	rf.warn(ctx, DevLog{"message": "election failed due to not gained enough votes, return to follower"})

	rf.mu.Lock()
	rf.updateVotedFor(ctx, NOT_VOTED)
	rf.mu.Unlock()

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

	// internal
	LogID int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool

	// index of first entry that has conflict term
	LastIndex int

	// internal
	NextIndex int
	LogID     int64
}

type AppendEntriesCallResult struct {
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ctx := context.WithValue(context.Background(), ContextKeyLogID, args.LogID)
	reply.LogID = args.LogID

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debug(ctx, DevLog{
		"message": "received AppendEntries from leader",
		"args":    args,
	})

	rf.checkConvertToFollower(ctx, args.Term, args.LeaderId)

	reply.LastIndex = -1

	currentTerm := rf.getTerm()
	currentLogIndex := rf.getCurrentLogLength()
	compactedLogIndex := rf.compactedLogIndex

	// check AppendEntries call is from a valid leader
	// if leaders' term is behind of current follower, reject the request and return the known latest term
	reply.Term = max(args.Term, currentTerm)
	if currentTerm > args.Term {
		rf.warn(ctx, DevLog{
			"event":    "REJECT_APPEND_ENTRIES",
			"message":  "source term is behind current term",
			"leader":   args.LeaderId,
			"incoming": args.Term,
			"current":  currentTerm,
		})

		reply.Success = false
		return
	}

	// if follower's log does not contain an entry at prevLogIndex
	if currentLogIndex < args.PrevLogIndex {
		rf.warn(ctx, DevLog{
			"event":    "REJECT_APPEND_ENTRIES",
			"message":  "follower's log does not contain an entry at prevLogIndex",
			"leader":   args.LeaderId,
			"incoming": args.PrevLogIndex,
			"current":  currentLogIndex,
		})

		reply.Success = false
		reply.LastIndex = currentLogIndex
		return
	}

	// leader's prevLogIndex is compacted in follower
	if args.PrevLogIndex > 0 && args.PrevLogIndex < compactedLogIndex {
		rf.warn(ctx, DevLog{
			"event":     "IGNORE_APPEND_ENTRIES",
			"message":   "received a compacted log index, ignoring",
			"index":     args.PrevLogIndex,
			"compacted": compactedLogIndex,
		})

		reply.Success = false
		reply.LastIndex = compactedLogIndex
		return
	}

	// existing log entry conflicts with a new one (same index, different terms)
	if args.PrevLogIndex != 0 && rf.getTermOfLogIndex(ctx, args.PrevLogIndex) != args.PrevLogTerm {
		thisTerm := rf.getTermOfLogIndex(ctx, args.PrevLogIndex)
		rf.warn(ctx, DevLog{
			"event":     "REJECT_APPEND_ENTRIES",
			"message":   "follower's log has an entry at prevLogIndex, but term is conflict",
			"index":     args.PrevLogIndex,
			"incoming":  args.PrevLogTerm,
			"expecting": thisTerm,
		})

		// find the first entry of `thisTerm` (conflict term)
		// so that follower can resend from the first conflict term
		// TODO: can be optimized with binary search

		index := 0
		for i := args.PrevLogIndex; i >= rf.compactedLogIndex && i > 1; i-- {
			if rf.getTermOfLogIndex(ctx, i) >= thisTerm {
				index = i - 1
				continue
			}

			break
		}

		reply.Success = false
		reply.LastIndex = index

		rf.info(ctx, DevLog{
			"event":     "REJECT_APPEND_ENTRIES",
			"message":   "first entry has term of current log records",
			"term":      thisTerm,
			"index":     reply.LastIndex,
			"compacted": rf.compactedLogIndex,
		})

		return
	}

	// slice log array if later logs should be discarded
	// note that we should remove previous logs if log is compacted
	if args.PrevLogIndex != 0 && args.PrevLogIndex < rf.getCurrentLogLength() {
		end := args.PrevLogIndex - rf.compactedLogIndex

		if end > 0 {
			rf.warn(ctx, DevLog{
				"message": "discarding log after prevLogIndex",
				"index":   args.PrevLogIndex,
				"end":     end,
			})

			rf.log = rf.log[0:end]
		} else {
			rf.log = []LogEntry{}
		}
	}

	// append new entries to log
	rf.appendLogEntries(ctx, &args.Entries)
	rf.success(ctx, DevLog{
		"message":      "append log entries success",
		"len":          len(args.Entries),
		"lastLogIndex": rf.getCurrentLogLength(),
	})

	// update follower's term
	rf.updateTerm(ctx, max(rf.getTerm(), args.Term))

	// update last receive time
	rf.updateLastReceiveRpcTime()

	// commit logs
	if args.LeaderCommit > int64(rf.commitIndex) && args.LeaderCommit <= int64(rf.getCurrentLogLength()) {
		rf.submitToCommitQueue(ctx, rf.commitIndex+1, int(args.LeaderCommit))
	}

	// response RPCs
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// called by leader to append commands to leader's log, then send AppendEntries to followers
func (rf *Raft) appendEntriesToAllServer(
	ctx context.Context,
	entries *[]LogEntry,
) {
	rf.debug(ctx, DevLog{
		"message": "leader: send AppendEntries to all servers",
		"length":  len(*entries),
	})

	var nextTerm int64
	var maxCommitInThisRequest int

	LockAndRun(rf, func() bool {
		nextTerm = rf.getTerm()

		// if AppendEntries is success, leader can only commit logs up to current length
		maxCommitInThisRequest = rf.getCurrentLogLength()

		return true
	})

	result := SendRPCToAllPeersConcurrently(rf, "AppendEntries", func(peerIndex int) *AppendEntriesCallResult {
		res := AppendEntriesCallResult{}
		res.args, res.reply = rf.sendAppendEntriesToSpecificPeer(peerIndex)
		return &res
	})

	successPeers := int64(0)
	ForEachPeers(rf, func(index int) {
		ok := result[index].Ok
		payload := result[index].Result.(*AppendEntriesCallResult)

		if !ok {
			rf.warn(ctx, DevLog{
				"message": "send AppendEntries failed or timeout before handle result",
				"peer":    index,
			})
			return
		}

		ok, maybeNextTerm := rf.handleAppendEntriesResult(
			ctx,
			index,
			payload.args,
			payload.reply,
		)

		if ok {
			successPeers++
			return
		}

		if maybeNextTerm > 0 {
			nextTerm = max(nextTerm, maybeNextTerm)
		}
	})

	rf.debug(ctx, DevLog{
		"message": "count followers have replied AppendEntries",
		"num":     successPeers,
	})

	if rf.getRole() != ROLE_LEADER {
		rf.warn(ctx, DevLog{"message": "leader identity is expired, not commiting logs"})
		return
	}

	LockAndRun(rf, func() bool {
		// convert to follower if term behind happened
		if nextTerm > rf.getTerm() {
			rf.warn(ctx, DevLog{
				"message":  "leader identity is expired due to term behind",
				"incoming": nextTerm,
				"current":  rf.getTerm(),
			})

			rf.convertToRole(ctx, ROLE_FOLLOWER)
			rf.updateTerm(ctx, nextTerm)
		}

		// commit log if majority is returned
		// +1: self
		if successPeers+1 >= GetMajority(len(rf.peers)) {
			rf.success(ctx, DevLog{
				"message":  "get majority server AppendEntries success, we can commit now",
				"success":  successPeers + 1,
				"peers":    len(rf.peers),
				"logIndex": maxCommitInThisRequest,
			})

			prevCommited := rf.commitIndex
			if prevCommited < rf.getCurrentLogLength() {
				rf.submitToCommitQueue(ctx, prevCommited+1, maxCommitInThisRequest)
			}
		}

		return true
	})
}

func (rf *Raft) handleAppendEntriesResult(
	ctx context.Context,
	index int,
	arg *AppendEntriesArgs,
	payload *AppendEntriesReply,
) (bool, int64) {
	ctx = context.WithValue(ctx, ContextKeyLogID, (func() int64 {
		if arg == nil {
			return -1
		}
		return arg.LogID
	})())

	if payload == nil {
		rf.error(ctx, DevLog{
			"message":   "AppendEntries call failed",
			"peerIndex": index,
		})

		return false, -1
	}

	if payload.Success {
		// update nextIndex
		rf.mu.Lock()
		rf.updateNextIndex(ctx, index, payload.NextIndex)
		rf.mu.Unlock()

		rf.success(ctx, DevLog{
			"message": "AppendEntries success",
			"index":   index,
		})

		return true, -1
	}

	// follower's term is ahead of leader
	if payload.Term > rf.getTerm() {
		rf.debug(ctx, DevLog{
			"message":  "follower term is larger thean leader",
			"incoming": payload.Term,
			"current":  rf.getTerm(),
		})

		return false, payload.Term
	}

	// follower's log is beheind leader, append [lastIndex, latest] to follower
	if payload.LastIndex >= 0 {
		rf.warn(ctx, DevLog{
			"event":     "RETRY_APPEND_ENTRIES",
			"message":   "retry due to lastIndex >= 0, update lastIndex",
			"nextIndex": payload.LastIndex,
			"peer":      index,
		})

		rf.mu.Lock()
		rf.updateNextIndex(ctx, index, payload.LastIndex)
		rf.mu.Unlock()

		go rf.retryAppendEntries(ctx, index, arg.Term) // retry in goroutine
		return false, -1
	}

	return false, -1
}

func (rf *Raft) sendAppendEntriesToSpecificPeer(peerIndex int) (*AppendEntriesArgs, *AppendEntriesReply) {
	var req AppendEntriesArgs
	var prevLogIndex int
	var nextLogIndex int

	logID := rf.GetLogID()
	ctx := context.WithValue(context.Background(), ContextKeyLogID, logID)

	if rf.getRole() != ROLE_LEADER {
		rf.warn(context.Background(), DevLog{
			"message": "leader identity has changed, stop AppendEntries",
		})

		return nil, nil
	}

	rf.mu.Lock()

	// handle prevLogIndex <= rf.compactedIndex, then InstallSnapshot is needed
	prevLogIndex = min(rf.nextIndex[peerIndex], rf.getCurrentLogLength())
	shouldInstallSnapshot := prevLogIndex < rf.compactedLogIndex

	if shouldInstallSnapshot {
		rf.mu.Unlock()
		rf.warn(ctx, DevLog{
			"message":   "meet nextIndex < compactedLogIndex, sending InstallSnapshot to peer",
			"peer":      peerIndex,
			"nextIndex": prevLogIndex,
			"compacted": rf.compactedLogIndex,
		})

		go rf.sendInstallSnapshotToPeer(ctx, peerIndex)
		return &AppendEntriesArgs{LogID: logID}, nil
	}

	// get previous sent log index and its term for peer i
	prevLogTerm := int64(-1)
	if prevLogIndex > 0 { // already sent log
		prevLogTerm = rf.getTermOfLogIndex(ctx, prevLogIndex)
	}

	// construct entries to send
	nextLogIndex = rf.getCurrentLogLength()
	logEntries := make([]LogEntry, rf.getCurrentLogLength()-prevLogIndex)
	copy(logEntries, rf.getLogEntriesAtRange(ctx, prevLogIndex+1, nextLogIndex)) // [prevLogIndex+1, len(log))

	req = AppendEntriesArgs{
		Term:         rf.getTerm(),
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logEntries,
		LeaderCommit: int64(rf.commitIndex), // follower can commit from: [rfFollower.commitIndex, leaderCommit+1)
		LogID:        logID,
	}

	res := AppendEntriesReply{}

	rf.info(ctx, DevLog{
		"event":        "SEND_APPEND_ENTRIES",
		"message":      "send AppendEntries to peer",
		"peer":         peerIndex,
		"prevLogIndex": prevLogIndex,
		"prevLogTerm":  req.PrevLogTerm,
		"from":         prevLogIndex,
		"to": (func() int {
			if len(req.Entries) == 0 {
				return prevLogIndex
			}
			return req.Entries[len(req.Entries)-1].Index
		})(),
	})

	rf.mu.Unlock()

	// send AppendEntries infinitely if fails
	done, ok := RunInTimeLimit(RPC_TIMEOUT_MS, func() bool {
		return rf.sendAppendEntries(peerIndex, &req, &res)
	})

	if !done || !ok {
		return &req, nil
	}

	res.NextIndex = nextLogIndex

	return &req, &res
}

func (rf *Raft) retryAppendEntries(ctx context.Context, peerIndex int, term int64) {
	time.Sleep(time.Millisecond * RPC_RETRY_MS * 2)

	if rf.getTerm() != term || rf.getRole() != ROLE_LEADER {
		rf.warn(ctx, DevLog{
			"message":  "leader identity expired or term has changed, discard retyr task",
			"taskTerm": term,
		})
		return
	}

	arg, reply := rf.sendAppendEntriesToSpecificPeer(peerIndex)

	rf.handleAppendEntriesResult(ctx, peerIndex, arg, reply)
}

// #endregion

// #region Log Commit

// Commit logs from [fromIndex, toIndex] to state machine
// should subtract 1, since index is stored from 1 in Raft's definition
func (rf *Raft) submitToCommitQueue(ctx context.Context, fromIndex int, toIndex int) {
	if fromIndex > rf.getCurrentLogLength() || toIndex > rf.getCurrentLogLength() {
		rf.fatal(ctx, DevLog{
			"message":          "cannot submit a commit task that out of bound",
			"fromIndex":        fromIndex,
			"toIndex":          toIndex,
			"currentLogLength": rf.getCurrentLogLength(),
		})
		return
	}

	rf.debug(ctx, DevLog{
		"message": "create commit task to commit queue",
		"from":    fromIndex,
		"to":      toIndex,
	})

	rf.commitCh <- LogCommitRequest{
		FromIndex: fromIndex,
		ToIndex:   toIndex,
	}
}

type LogCommitRequest struct {
	FromIndex int
	ToIndex   int
}

func (rf *Raft) committer(ctx context.Context) {
	for task := range rf.commitCh {
		if rf.killed() {
			break
		}

		// get log entries in range [from, to]
		rf.mu.Lock()
		currentCommited := rf.commitIndex
		from := max(currentCommited+1, task.FromIndex)
		to := task.ToIndex

		rf.debug(ctx, DevLog{
			"message":  "received log commit request",
			"from":     task.FromIndex,
			"to":       task.ToIndex,
			"commited": currentCommited,
			"realFrom": from,
			"realTo":   to,
		})

		if from <= rf.compactedLogIndex {
			rf.warn(context.Background(), DevLog{
				"message": "`from` is compacted, need to commit snapshot before continue",
			})

			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				SnapshotTerm:  int(rf.compactedLogTerm),
				SnapshotIndex: rf.compactedLogIndex,
				Snapshot:      rf.snapshot,
			}

			rf.commitIndex = rf.compactedLogIndex
			from = rf.commitIndex + 1
		}

		if to < from {
			rf.mu.Unlock()
			continue
		}

		if from > rf.getCurrentLogLength() || to > rf.getCurrentLogLength() {
			rf.warn(ctx, DevLog{
				"message": "commit task may be outdated",
				"from":    from,
				"to":      to,
				"length":  rf.getCurrentLogLength(),
			})

			rf.mu.Unlock()
			continue
		}

		entries := rf.getLogEntriesAtRange(ctx, from, to)

		rf.info(ctx, DevLog{
			"message":   "commit logs summary",
			"range":     fmt.Sprintf("[%v, %v]", from, to),
			"goRange":   fmt.Sprintf("[%v, %v)", from-1, to),
			"commitLen": len(entries),
			"bufferLen": len(rf.log),
		})

		for index, entry := range entries {
			rf.debug(ctx, DevLog{"message": "current commiting entry", "entry": entry})
			currentLogIndex := from + index // in Raft's presentation

			rf.mu.Unlock()

			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Payload,
				CommandIndex: currentLogIndex,
			}

			rf.applyCh <- msg

			rf.mu.Lock()

			rf.commitIndex = currentLogIndex // in Raft's presentation

			rf.debug(ctx, DevLog{
				"message": "commit log item complete",
				"index":   currentLogIndex,
				"payload": entry.Payload,
			})
		}

		rf.mu.Unlock()
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
	// this is atomic opeartions so acquring mutex lock is not required
	role := atomic.LoadInt32(&rf.role)
	currentTerm := atomic.LoadInt64(&rf.currentTerm)
	term = int(currentTerm)
	isLeader = role == ROLE_LEADER

	// if current role is not leader, just return and redirect the request to leader
	// otherwise we can append this log and start an entry
	if isLeader {
		ctx := context.Background()

		rf.info(ctx, DevLog{
			"message": "receive client request",
			"payload": command,
		})

		metricStart := time.Now().UnixMilli()

		logEntries := []LogEntry{
			{
				Term:    currentTerm,
				Payload: command,
			},
		}

		rf.mu.Lock()
		rf.appendLogEntries(ctx, &logEntries)
		rf.mu.Unlock()

		go rf.appendEntriesToAllServer(ctx, &logEntries)

		currentLogIndex := logEntries[0].Index

		metricEnd := time.Now().UnixMilli()

		rf.success(ctx, DevLog{
			"message":  "start agreement complete",
			"logIndex": currentLogIndex,
			"term":     term,
			"command":  command,
			"metricMs": metricEnd - metricStart,
		})

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

func (rf *Raft) ticker(ctx context.Context) {
	// first wait for a random timeout to prevent all servers go to candidate simultaneously
	time.Sleep(time.Duration(rf.me) * 200 * time.Millisecond)

	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		role := atomic.LoadInt32(&rf.role)

		// Follower
		if role == ROLE_FOLLOWER {
			if !rf.checkShouldStartNewElection(ctx) {
				goto nextTick
			}

			rf.info(ctx, DevLog{"message": "starting new election"})

			// start a new leader election if time is expired:
			// - increment current term by 1
			// - convert to candidate and vote for self
			// - reset election timer (TODO)
			// - send `RequestVote` rpc to all servers
			// 	- if gained majority of votes
			// 	- else convert to follower again
			rf.updateTerm(ctx)
			rf.convertToRole(ctx, ROLE_CANDIDATE)

			ok, _ := RunInTimeLimit(rf.electionTimeoutMs, func() bool {
				nextRole := rf.requestVoteForAllServer(ctx)
				rf.convertToRole(ctx, nextRole)
				return true
			})

			if !ok {
				rf.warn(ctx, DevLog{
					"message":   "election timeout is exceeded, sleep for next election",
					"timeoutMs": rf.electionTimeoutMs,
				})

				rf.convertToRole(ctx, ROLE_FOLLOWER)
			}
		}

		if role == ROLE_LEADER {
			// send heartbeat to all peers
			rf.debug(ctx, DevLog{"message": "sending heartbeats to all peers"})
			rf.appendEntriesToAllServer(ctx, &[]LogEntry{})
		}

	nextTick:
		ms := rand.Int31()%200 + 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

	rf.warn(ctx, DevLog{
		"message": "serveer has been killed",
		"peer":    rf.me,
	})
}

// #region internal methods
func (rf *Raft) convertToRole(ctx context.Context, role int32) {
	prevRole := rf.role
	atomic.StoreInt32(&rf.role, role)

	if prevRole == role {
		return
	}

	if role == ROLE_CANDIDATE {
		rf.updateVotedFor(ctx, int32(rf.me))
	}

	rf.info(ctx, DevLog{
		"message":  "role convertion happened",
		"prevRole": rf.getRoleName(prevRole),
		"nextRole": rf.getRoleName(role),
	})
}

func (rf *Raft) updateTerm(ctx context.Context, term ...int64) {
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
		rf.info(ctx, DevLog{
			"message":  "server term updated",
			"prevTerm": prevTerm,
			"curTerm":  curTerm,
		})
	}

	if prevTerm < curTerm {
		rf.updateVotedFor(ctx, NOT_VOTED)
	}

	rf.persist(ctx)
}

func (rf *Raft) updateVotedFor(ctx context.Context, nextVoteFor int32) {
	atomic.StoreInt32(&rf.votedFor, nextVoteFor)
	rf.persist(ctx)
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
func (rf *Raft) checkShouldStartNewElection(ctx context.Context) bool {
	if rf.mu.TryLock() {
		defer rf.mu.Unlock()
	}

	now := time.Now().UnixMilli()
	lastReceiveRpcTimestamp := atomic.LoadInt64(&rf.lastReceiveRpcTimestamp)
	lastGrantVoteTimestamp := atomic.LoadInt64(&rf.lastGrantVoteTimestamp)
	receiveAppendEntriesTimeout := (now-lastReceiveRpcTimestamp > rf.electionTimeoutMs)
	voteTimeout := now-lastGrantVoteTimestamp > rf.electionTimeoutMs

	shouldStartElection := receiveAppendEntriesTimeout && voteTimeout
	if shouldStartElection {
		rf.debug(ctx, DevLog{
			"message":     "should start an election",
			"rpcTimeout":  receiveAppendEntriesTimeout,
			"voteTimeout": voteTimeout,
		})
	} else {
		rf.debug(ctx, DevLog{
			"message":     "no need to restart an election",
			"rpcTimeout":  receiveAppendEntriesTimeout,
			"diff":        now - lastReceiveRpcTimestamp,
			"threshold":   rf.electionTimeoutMs,
			"voteTimeout": voteTimeout,
		})
	}

	return shouldStartElection
}

func (rf *Raft) checkConvertToFollower(ctx context.Context, term int64, peer int) {
	currentTerm := atomic.LoadInt64(&rf.currentTerm)
	if currentTerm >= term {
		return
	}

	rf.warn(ctx, DevLog{
		"message":  "received RPC from other peers that term larger than self, convert to follower",
		"peer":     peer,
		"incoming": term,
		"current":  currentTerm,
	})

	rf.updateTerm(ctx, term)
	rf.convertToRole(ctx, ROLE_FOLLOWER)
}

func (rf *Raft) initializeLeaderState(ctx context.Context) {
	rf.mu.Lock()

	// initialize nextIndex of all pers to self log length
	lastLogIndex := rf.getCurrentLogLength()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range len(rf.peers) {
		rf.nextIndex[i] = lastLogIndex
	}

	// voted for self in this term
	rf.updateVotedFor(ctx, int32(rf.me))

	rf.mu.Unlock()
}

func (rf *Raft) appendLogEntries(ctx context.Context, entries *[]LogEntry) {
	rf.debug(ctx, DevLog{
		"message": "appendLogEntries to self logs",
		"len":     len(*entries),
		"entries": *entries,
	})

	curLen := rf.getCurrentLogLength()

	for i := range *entries {
		entry := &((*entries)[i])
		// if entry has no index, current is leader's first append, mark an index
		if entry.Index == 0 {
			entry.Index = curLen + i + 1
		}

		// discard logs in snapshot
		if entry.Index <= rf.compactedLogIndex {
			rf.warn(ctx, DevLog{
				"message":   "iscard log entry that is compacted",
				"incoming":  entry.Index,
				"compacted": rf.compactedLogIndex,
			})

			continue
		}

		// otherwise, the entry is received from leaders
		if entry.Index <= curLen {
			rf.debug(ctx, DevLog{"message": "override log entry at index", "index": entry.Index})
			rf.log[entry.Index-1] = *entry // override log items
		} else {
			rf.log = append(rf.log, *entry)
		}
	}

	rf.debug(ctx, DevLog{
		"message": fmt.Sprintf("append %v log entries to self logs", len(*entries)),
		"length":  rf.getCurrentLogLength(),
	})

	rf.persist(ctx)
}

func (rf *Raft) updateNextIndex(ctx context.Context, peerIndex int, value int) {
	var nextValue int
	if value == -1 {
		nextValue = rf.getCurrentLogLength()
	} else {
		nextValue = min(value, rf.getCurrentLogLength())
	}
	rf.nextIndex[peerIndex] = nextValue

	rf.debug(ctx, DevLog{
		"message":   "update nextIndex",
		"peer":      peerIndex,
		"nextIndex": nextValue,
	})
}

// get current length of compacted / stored logs
func (rf *Raft) getCurrentLogLength() int {
	return rf.compactedLogIndex + len(rf.log)
}

// return a pointer of log entry in given index
func (rf *Raft) getLogEntryAt(ctx context.Context, index int) *LogEntry {
	// compaction
	if index <= rf.compactedLogIndex {
		rf.fatal(ctx, DevLog{
			"message":   "requested a compacted log entry is not supported",
			"index":     index,
			"compacted": rf.compactedLogIndex,
		})

		return nil
	}
	return &rf.log[index-rf.compactedLogIndex-1]
}

func (rf *Raft) getLogEntriesAtRange(ctx context.Context, fromIndex int, toIndex int) []LogEntry {
	if fromIndex > toIndex {
		return []LogEntry{}
	}

	if fromIndex <= rf.compactedLogIndex || toIndex <= rf.compactedLogIndex {
		rf.fatal(ctx, DevLog{
			"message":   "requested log entries contains compacted entries is not supported",
			"from":      fromIndex,
			"to":        toIndex,
			"compacted": rf.compactedLogIndex,
		})

		return nil
	}

	from := fromIndex - rf.compactedLogIndex
	to := toIndex - rf.compactedLogIndex

	if to > len(rf.log) {
		rf.fatal(ctx, DevLog{
			"message":        "requested log out of bound",
			"from":           from,
			"to":             to,
			"len":            len(rf.log),
			"compactedIndex": rf.compactedLogIndex,
		})
	}

	return rf.log[from-1 : to]
}

// return the discarded length of term
func (rf *Raft) discardLogsUntil(ctx context.Context, index int) int64 {
	rf.info(ctx, DevLog{
		"message": "discarding logs",
		"from":    0,
		"to":      index,
	})
	// find the entry in log array that has this index
	// find out its index in array and its term
	fromIndex := 0
	term := int64(-1)

	// TODO: can be optimized via binary search
	for ; fromIndex < len(rf.log) && rf.log[fromIndex].Index <= index; fromIndex++ {
		if rf.log[fromIndex].Index == index {
			term = rf.log[fromIndex].Term
		}
	}

	// discard entries before `fromIndex`
	rf.log = rf.log[fromIndex:len(rf.log)]
	return term
}

func (rf *Raft) compactLogsToIndex(ctx context.Context, index int) (bool, int64) {
	if index <= rf.compactedLogIndex {
		rf.warn(ctx, DevLog{
			"message":  "new snapshot index is behind current compacted index",
			"incoming": index,
			"current":  rf.compactedLogIndex,
		})
		return false, -1
	}

	term := rf.discardLogsUntil(ctx, index)

	// update compacted index and term
	rf.commitIndex = index
	rf.compactedLogIndex = index
	rf.compactedLogTerm = term

	rf.success(ctx, DevLog{
		"message": "log compacted done",
		"index":   index,
		"term":    term,
	})

	return true, term
}

func (rf *Raft) getTermOfLogIndex(ctx context.Context, index int) int64 {
	if index == 0 {
		return -1
	}
	if index <= rf.compactedLogIndex {
		return rf.compactedLogTerm
	}
	return rf.getLogEntryAt(ctx, index).Term
}

// #endregion

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

func (rf *Raft) getVotedFor() int32 {
	votedFor := atomic.LoadInt32(&rf.votedFor)
	return votedFor
}

// #region Log Utils
var lastLogCallTime int64

func (rf *Raft) logBase(ctx context.Context, level int, payload DevLog) {
	caller := GetCaller(3)

	now := time.Now().UnixMilli()

	finalPayload := DevLog{
		"_id":     rf.me,
		"_role":   rf.getRoleName(rf.getRole()),
		"_term":   rf.getTerm(),
		"_method": caller,
		"_cost":   fmt.Sprintf("%vms", now-atomic.LoadInt64(&lastLogCallTime)),
	}

	for k, v := range payload {
		finalPayload[k] = v
	}

	if logID := ctx.Value(ContextKeyLogID); logID != nil {
		finalPayload["_logID"] = logID
	}

	Log(level, finalPayload)

	atomic.StoreInt64(&lastLogCallTime, now)
}

func (rf *Raft) debug(ctx context.Context, payload DevLog) {
	rf.logBase(ctx, LEVEL_DEBUG, payload)
}
func (rf *Raft) success(ctx context.Context, payload DevLog) {
	rf.logBase(ctx, LEVEL_SUCCESS, payload)
}
func (rf *Raft) info(ctx context.Context, payload DevLog) {
	rf.logBase(ctx, LEVEL_INFO, payload)
}
func (rf *Raft) warn(ctx context.Context, payload DevLog) {
	rf.logBase(ctx, LEVEL_WARN, payload)
}
func (rf *Raft) error(ctx context.Context, payload DevLog) {
	rf.logBase(ctx, LEVEL_ERROR, payload)
}
func (rf *Raft) fatal(ctx context.Context, payload DevLog) {
	rf.logBase(ctx, LEVEL_FATAL, payload)
	panic("fatal detected")
}

func (rf *Raft) GetLogID() int64 {
	index := atomic.LoadInt64(&rf.logID)
	atomic.AddInt64(&rf.logID, 1)
	return 1e10 + int64(rf.me)*1e8 + index
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
	ctx := context.WithValue(context.Background(), ContextKeySelfID, me)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.electionTimeoutMs = int64(rf.me)*200 + ELECTION_TIMEOUT_MIN_MS
	rf.role = ROLE_FOLLOWER
	rf.applyCh = applyCh
	rf.votedFor = NOT_VOTED
	rf.commitCh = make(chan LogCommitRequest, 50)

	// initialize from state persisted before a crash
	rf.readPersist(context.Background(), persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.committer(ctx)
	go rf.ticker(ctx)

	return rf
}
