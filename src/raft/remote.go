package raft

import (
	"time"
)

type AppendReq struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLog
	LeaderCommit int
}

type AppendReply struct {
	Term    int
	Success bool
	Xterm   int
	XIndex  int
	Len     int
	Old     bool
}
type InstallSnapReq struct {
	Term            int
	Snap            []byte
	LastInclude     int
	LastIncludeTerm int
}

type InstallSnapReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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
	curTerm := rf.currentTerm
	if args.Term < curTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.role = roleFollower
		rf.currentTerm = args.Term
		rf.votedFor = nil
	}
	if (rf.votedFor == nil || *rf.votedFor == args.CandidateID) && rf.logIsUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = &args.CandidateID
		rf.fullFollowerFlag()
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) logIsUpToDate(term, index int) bool {
	if term > rf.getLog(rf.getVirtualEndLogIndex()).Term {
		return true
	}
	if term == rf.getLog(rf.getVirtualEndLogIndex()).Term && index >= rf.getVirtualEndLogIndex() {
		return true
	}
	return false

}
func (rf *Raft) seenHigherTerm(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term <= rf.currentTerm {
		return false
	}
	rf.role = roleFollower
	rf.currentTerm = term
	rf.votedFor = nil
	return true
}
func (rf *Raft) AppendEntries(args *AppendReq, reply *AppendReply) {
	// Your code here (2A, 2B).
	var shouldPersis bool = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	curTerm := rf.currentTerm
	if args.Term < curTerm {
		reply.Term = curTerm
		reply.Success = false
		return
	}
	rf.fullFollowerFlag()
	//看到了更高的term或者，当前是竞选中，看到了别的领导发的心跳。
	if (args.Term == rf.currentTerm && rf.role == roleCandidate) || args.Term > rf.currentTerm {
		rf.role = roleFollower
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = nil
			shouldPersis = true
		}
	}
	if args.PrevLogIndex > rf.getVirtualEndLogIndex() {
		reply.Term = curTerm
		reply.Len = rf.getVirtualEndLogIndex() + 1
		reply.Success = false
		if shouldPersis {
			rf.persist()
		}
		return
	}
	if args.PrevLogIndex < rf.getVirtualBeginLogIndex()-1 {
		//旧的请求
		reply.Term = curTerm
		reply.Old = true
		return
	}
	if rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		for i := rf.getVirtualBeginLogIndex(); i <= rf.getVirtualEndLogIndex(); i++ {
			if rf.getLog(i).Term == rf.getLog(args.PrevLogIndex).Term {
				reply.XIndex = i
				break
			}
		}
		reply.Xterm = rf.getLog(args.PrevLogIndex).Term
		reply.Term = curTerm
		reply.Success = false
		if shouldPersis {
			rf.persist()
		}
		return
	}
	if len(args.Entries) != 0 && !rf.alreadyAppend(args.PrevLogIndex+1, args.Entries) {
		rf.superLogs = append(rf.superLogs[0:rf.real(args.PrevLogIndex+1)], args.Entries...)
		rf.persist()
		shouldPersis = false
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getVirtualEndLogIndex())
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	if shouldPersis {
		rf.persist()
	}
	return
}
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
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
//

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) int {
	okChan := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		okChan <- ok
	}()
	select {
	case <-time.After(rpcTimeout):
		return NoReply //超时 要重试
	case ok := <-okChan:
		if !ok {
			return NoReply //没响应要重试
		}
		seen := rf.seenHigherTerm(reply.Term)
		if seen {
			return SeeLeader //看到大哥 不用重试
		}
		return RpcSuccess
	}

}
func (rf *Raft) sendAppendEntries(server int, args *AppendReq, reply *AppendReply) int {
	okChan := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		okChan <- ok
	}()
	select {
	case <-time.After(rpcTimeout):
		return NoReply //超时
	case ok := <-okChan:
		if !ok {
			return NoReply //没响应
		}
		seen := rf.seenHigherTerm(reply.Term)
		if seen {
			return SeeLeader //看到大哥 不用重试
		}
		return RpcSuccess
	}
}

//TODO 重复装载
func (rf *Raft) InstallSnap(args *InstallSnapReq, reply *InstallSnapReply) {
	rf.mu.Lock()
	curTerm := rf.currentTerm
	if args.Term < curTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.fullFollowerFlag()
	//TODO persist?
	if args.Term > rf.currentTerm {
		rf.role = roleFollower
		rf.currentTerm = args.Term
		rf.votedFor = nil
	}
	if args.LastInclude <= rf.lastLogInclude {
		reply.Success = true
		reply.Term = curTerm
		rf.mu.Unlock()
		return
	}
	if args.LastInclude == rf.HistoryInstall {
		//上次的未完成的重复调用！
		if !(time.Now().Unix()-rf.installTimeStamp > retryInstallInterval) {
			rf.mu.Unlock()
			reply.Success = false
			reply.Term = curTerm
			return
		}
	}
	rf.HistoryInstall = args.LastInclude
	rf.installTimeStamp = time.Now().Unix()
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snap,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastInclude,
	}
	rf.mu.Unlock()
	go func() {
		select {
		case <-time.After(retryInstallInterval * time.Second):
			return
		case rf.applyChan <- msg:
			return
		}
	}()
	reply.Success = false
	reply.Term = curTerm
	return
}

func (rf *Raft) sendInstallSnap(server int, args *InstallSnapReq, reply *InstallSnapReply) int {
	okChan := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnap", args, reply)
		okChan <- ok
	}()
	select {
	case <-time.After(rpcTimeout):
		return NoReply //超时
	case ok := <-okChan:
		if !ok {
			return NoReply //没响应
		}
		seen := rf.seenHigherTerm(reply.Term)
		if seen {
			return SeeLeader //看到大哥 不用重试
		}
		return RpcSuccess
	}
}
