package raft

import (
	"sync"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
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
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("id = %d RequestVote Lock", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d rece %d", rf.me, args.CandidateId)

	if rf.currentTerm < args.Term {
		rf.setNewTerm(args.Term)
	}
	if rf.currentTerm > args.Term || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		DPrintf("%d no vote to %d\n", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	lastLog := rf.logs.lastLog()
	newerThanMe := (args.LastLogTerm > lastLog.Term) ||
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
	if newerThanMe {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		DPrintf("%d vote to %d\n", rf.me, args.CandidateId)
		rf.resetElectionTime()
	} else {
		DPrintf("no newerThanMe %d no vote to %d\n", rf.me, args.CandidateId)
	}

}

func (rf *Raft) callRequestVote(i int, args *RequestVoteArgs, getVoted *int, becomeLeader *sync.Once) {

	reply := RequestVoteReply{}

	rf.sendRequestVote(i, args, &reply)

	DPrintf("id = %d callRequestVote Lock", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	if !reply.VoteGranted {
		return
	}

	*getVoted++
	//DPrintf("%d get %d", rf.me, *getVoted)
	if *getVoted > rf.peersNum/2 && rf.curState == Candidate {
		becomeLeader.Do(rf.becomeLeader)
	}

}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
