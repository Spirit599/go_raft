package raft

import (
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) setNewTerm(term int) {

	DPrintf("id = %d become follower setNewTerm %d\n", rf.me, term)
	rf.curState = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetElectionTime()
}

func (rf *Raft) becomeLeader() {
	DPrintf("im %d leader", rf.me)
	rf.curState = Leader
	lastLogIndex := rf.logs.lastLog().Index
	for i := 0; i < rf.peersNum; i++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.Broadcast(true)
}

func (rf *Raft) resetElectionTime() {
	now := time.Now()
	timeout := time.Duration(rf.electionTimeOutDownlimit+rand.Int()%rf.electionTimeOutdiff) * time.Millisecond
	rf.electionTime = now.Add(timeout)
	//fmt.Println(rf.electionTime, rf.me)
}

func (rf *Raft) voteForMe() {

	DPrintf("id = %d voteforme\n", rf.me)

	rf.currentTerm++
	rf.curState = Candidate
	rf.votedFor = rf.me
	rf.resetElectionTime()

	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.logs.lastLog().Index
	args.LastLogTerm = rf.logs.lastLog().Term

	getVoted := 1

	var becomeLeader sync.Once
	for serverID := 0; serverID < rf.peersNum; serverID++ {
		if serverID != rf.me {

			go rf.callRequestVote(serverID, &args, &getVoted, &becomeLeader)
		}

	}

}
