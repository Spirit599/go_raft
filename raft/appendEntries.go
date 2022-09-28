package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) Broadcast(isHeartBeat bool) {

	lastLog := rf.logs.lastLog()
	for i := 0; i < rf.peersNum; i++ {
		if i != rf.me {
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			if isHeartBeat || rf.nextIndex[i] <= lastLog.Index {
				nextIndex := rf.nextIndex[i]
				prevLog := rf.logs.at(nextIndex - 1)
				args.PrevLogIndex = prevLog.Index
				args.PrevLogTerm = prevLog.Term
				args.LeaderCommit = rf.commitIndex
				args.Entries = make([]Log, lastLog.Index-nextIndex+1)
				copy(args.Entries, rf.logs.slice(nextIndex))
			}
			go rf.callAppendEntries(i, &args)
		}
	}
}

func (rf *Raft) callAppendEntries(i int, args *AppendEntriesArgs) {

	reply := AppendEntriesReply{}
	rf.sendAppendEntries(i, args, &reply)

	//DPrintf("id = %d callAppendEntries Lock", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	if reply.Term < rf.currentTerm {
		return
	}
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		next := match + 1
		rf.matchIndex[i] = max(match, rf.matchIndex[i])
		rf.nextIndex[i] = max(next, rf.nextIndex[i])
	} else if rf.nextIndex[i] > 1 {
		rf.nextIndex[i]--
	}
	rf.leaderCommitLogs()
}

func (rf *Raft) leaderCommitLogs() {
	if rf.curState != Leader {
		return
	}
	lastLogIndex := rf.logs.lastLog().Index
	for toBeCommit := rf.commitIndex + 1; toBeCommit <= lastLogIndex; toBeCommit++ {
		cnt := 1
		for i := 0; i < rf.peersNum; i++ {
			if i != rf.me && rf.matchIndex[i] >= toBeCommit {
				cnt++
			}
			if cnt > rf.peersNum/2 {
				rf.commitIndex = toBeCommit
				rf.apply()
				break
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	//DPrintf("id = %d AppendEntries Lock", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}
	if rf.currentTerm < args.Term {
		rf.setNewTerm(args.Term)
		return
	}

	//DDPrintf("id %d get heartbeat from %d", rf.me, args.LeaderId)
	rf.resetElectionTime()
	if rf.curState == Candidate {
		rf.curState = Follower
	}

	if rf.logs.lastLog().Index < args.PrevLogIndex {
		return
	}
	if rf.logs.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		return
	}
	for index, log := range args.Entries {

		if log.Index <= rf.logs.lastLog().Index && rf.logs.at(log.Index).Term != log.Term {
			rf.logs.truncate(log.Index)
		}
		if log.Index > rf.logs.lastLog().Index {
			rf.logs.append(args.Entries[index:]...)
			DPrintf("[%d]: follower append [%v]", rf.me, args.Entries[index:])
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logs.lastLog().Index)
		rf.apply()
	}
	reply.Success = true

}
