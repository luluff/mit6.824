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
	"6.824/labgob"
	"bytes"
	"context"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	role         int
	followerFlag int32
	rand         *rand.Rand
	//所有服务器上的持久性状态
	currentTerm        int
	votedFor           *int
	superLogs          []RaftLog //第0位不放东西，从1开始
	lastLogInclude     int
	lastLogIncludeTerm int
	snap               []byte
	//leader上的易失性状态
	nextIndex  []int //逻辑地址
	matchIndex []int //逻辑地址
	//所有服务器上的易失性状态
	commitIndex int //初始化为lastIncludingIndex
	lastApplied int //初始化为lastIncludingIndex  检查所有index的初始化
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	taskChan  chan bool
	applyChan chan ApplyMsg
	//用来防止相同snapInstall操作频繁顶向kv
	installTimeStamp int64
	HistoryInstall   int
}
type RaftLog struct {
	Term    int
	Command interface{}
}

func (rf *Raft) appendLog(value RaftLog) {
	rf.setLog(rf.getVirtualEndLogIndex()+1, value)
}
func (rf *Raft) setLog(virtualAddr int, value RaftLog) {
	if virtualAddr-rf.lastLogInclude <= 0 {
		panic("setLog to snapshot!")
	}
	rf.superLogs[rf.real(virtualAddr)] = value
}
func (rf *Raft) getVirtualEndLogIndex() int {
	return rf.lastLogInclude + len(rf.superLogs) - 1
}
func (rf *Raft) real(virtual int) int {
	return virtual - rf.lastLogInclude
}
func (rf *Raft) getLog(virtual int) RaftLog {
	return rf.superLogs[rf.real(virtual)]
}
func (rf *Raft) getVirtualBeginLogIndex() int {
	return rf.lastLogInclude + 1
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
	rf.superLogs = append(rf.superLogs, RaftLog{
		Term:    -1,
		Command: "empty",
	})
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())
	//rf.rand = rand.New(rand.NewSource(int64(me)))
	rf.rand = rand.New(rand.NewSource(makeSeed()))
	// start follower goroutine to start elections
	rf.role = roleFollower
	rf.applyChan = applyCh
	go rf.follower()
	go rf.doApply()
	return rf
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	//Your code here (2C).
	//Example:
	rf.persister.SaveRaftState(rf.getNormalPersistData())
}
func (rf *Raft) getNormalPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	var vf int
	switch rf.votedFor {
	case nil:
		vf = -1
	default:
		vf = *rf.votedFor
	}
	err := e.Encode(vf)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.currentTerm)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.superLogs)
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (rf *Raft) persisSnapAndLog(data []byte, lastIncluding, lastTerm int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(lastIncluding)
	if err != nil {
		panic(err)
	}
	err = e.Encode(lastTerm)
	if err != nil {
		panic(err)
	}
	err = e.Encode(data)
	if err != nil {
		panic(err)
	}
	snapData := w.Bytes()
	rf.persister.SaveStateAndSnapshot(rf.getNormalPersistData(), snapData)
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		lastLogInclude     int
		lastLogIncludeTerm int
		snap               []byte
	)
	if d.Decode(&lastLogInclude) != nil || d.Decode(&lastLogIncludeTerm) != nil || d.Decode(&snap) != nil {
		panic("readSnapshot")
	}
	rf.lastLogInclude = lastLogInclude
	rf.lastLogIncludeTerm = lastLogIncludeTerm
	rf.lastApplied = lastLogInclude
	rf.commitIndex = lastLogInclude
	rf.snap = snap

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		voteFor int
		curTerm int
		logs    []RaftLog
	)
	if d.Decode(&voteFor) != nil || d.Decode(&curTerm) != nil || d.Decode(&logs) != nil {
		panic("readPersist")
	}
	switch voteFor {
	case -1:
		rf.votedFor = nil
	default:
		rf.votedFor = &voteFor
	}
	rf.superLogs = logs
	rf.currentTerm = curTerm
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex < rf.lastApplied {
		return false
	}
	if rf.getVirtualEndLogIndex() <= lastIncludedIndex {
		rf.superLogs = []RaftLog{{
			Term:    lastIncludedTerm,
			Command: "empty"}}
	} else {
		lg := rf.getLog(lastIncludedIndex)
		if lg.Term != lastIncludedTerm {
			panic("CondInstallSnapshot")
		}
		rf.superLogs = append([]RaftLog{{
			Term:    lg.Term,
			Command: lg.Command}}, rf.superLogs[rf.real(lastIncludedIndex+1):]...)
	}
	rf.lastLogInclude = lastIncludedIndex
	rf.lastLogIncludeTerm = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.snap = snapshot
	rf.persisSnapAndLog(snapshot, lastIncludedIndex, lastIncludedTerm)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//旧的不行
	if index <= rf.lastLogInclude {
		return
	}
	lg := rf.getLog(index)
	rf.superLogs = append([]RaftLog{{
		Term:    lg.Term,
		Command: lg.Command,
	}}, rf.superLogs[rf.real(index+1):]...)
	rf.lastLogInclude = index
	rf.lastLogIncludeTerm = lg.Term
	rf.snap = snapshot
	rf.persisSnapAndLog(snapshot, index, lg.Term)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != roleLeader {
		return -1, rf.currentTerm, false
	}
	rf.superLogs = append(rf.superLogs, RaftLog{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.persist()
	select {
	case rf.taskChan <- true:
	default:

	}
	return rf.getVirtualEndLogIndex(), rf.currentTerm, true
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

// The follower go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) follower() {
	//fmt.Printf("peer %d is follower on term %d time[%d]\n ", rf.me, rf.currentTerm, time.Now().Unix())
	for !rf.killed() {
		rf.clearFollowerFlag()
		time.Sleep(rf.getRandomElectionTimeout())
		if !rf.isFollowerFlagFull() {
			rf.mu.Lock()
			rf.role = roleCandidate
			rf.mu.Unlock()
			go rf.candidate()
			return
		}
	}
}

func (rf *Raft) candidate() {
	//fmt.Printf("peer %d is candidate on term %d time[%d]\n ", rf.me, rf.currentTerm, time.Now().Unix())
	for !rf.killed() {
		//fmt.Printf("peer %d is candidate on term %d time[%d]\n ", rf.me, rf.currentTerm, time.Now().Unix())
		rf.mu.Lock()
		if rf.role == roleFollower {
			rf.mu.Unlock()
			go rf.follower()
			return
		}
		rf.currentTerm++
		rf.votedFor = &rf.me
		okChan := make(chan int, 1)
		ctx, cancel := context.WithCancel(context.Background())
		go rf.qiTao(ctx, okChan, rf.currentTerm, rf.me, rf.getVirtualEndLogIndex(), rf.getLog(rf.getVirtualEndLogIndex()).Term)
		rf.mu.Unlock()
		select {
		//见到leader了
		case <-ctx.Done():
			{
				time.Sleep(rf.getRandomElectionTimeout())
			}
		case ok := <-okChan:
			if ok == NoMajorVote {
				time.Sleep(rf.getRandomElectionTimeout())
				continue
			}
			if ok == MajorVote {
				//获得大多数的选票了
				cancel()
				go rf.leader()
				return
			}
			panic("aaaa")
		//超时
		case <-time.After(rf.getRandomElectionTimeout()):
			cancel()
		}
	}

}

func (rf *Raft) leader() {
	rf.mu.Lock()
	if rf.role == roleFollower {
		rf.mu.Unlock()
		go rf.follower()
		return
	}
	rf.role = roleLeader
	//fmt.Printf("peer %d is leader on term %d time[%d]\n ", rf.me, rf.currentTerm, time.Now().Unix())
	rf.nextIndex = make([]int, len(rf.peers))
	//初始化为领导者的最后一个日志索引+1
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getVirtualEndLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers)) //初始化为0
	rf.taskChan = make(chan bool, 1)
	heartBeatCtx, cancelHeartBeat := context.WithCancel(context.Background())
	go rf.heartBeatAll(heartBeatCtx, rf.taskChan, cancelHeartBeat)
	rf.mu.Unlock()
	for !rf.killed() {
		rf.mu.Lock()
		switch rf.role {
		case roleFollower:
			cancelHeartBeat()
			rf.mu.Unlock()
			go rf.follower()
			return
		case roleLeader:
			rf.mu.Unlock()
			time.Sleep(leaderInterval) //多久检查一下自己的身份
		default:
			panic("leader")
		}

	}
	cancelHeartBeat()
}
func (rf *Raft) updateCommitIndex() {
	for N := rf.getVirtualEndLogIndex(); N >= rf.getVirtualBeginLogIndex(); N-- {
		var num int = 1
		if rf.getLog(N).Term != rf.currentTerm {
			continue
		}
		for _, tmp := range rf.matchIndex {
			if tmp >= N {
				num++
			}
		}

		if rf.isMajor(num) {
			if N < rf.commitIndex {
				panic("commitIndex")
			}
			rf.commitIndex = N
			//找到了
			return
		}
	}
}
func (rf *Raft) heartBeatAll(ctx context.Context, ch chan bool, cancel func()) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.heartBeatOne(ctx, i, ch, cancel)
	}
}
func (rf *Raft) heartBeatOne(ctx context.Context, followerIndex int, ch chan bool, cancel func()) {
	var still bool
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(electionTimeout + time.Duration(followerIndex)*10*time.Millisecond):
			still = true
			for still {
				still = rf.doBeat(ctx, followerIndex, cancel)
			}
		case <-ch:
			still = true
			for still {
				still = rf.doBeat(ctx, followerIndex, cancel)
			}
		}

	}
}

//返回值：是否要继续
func (rf *Raft) doBeat(ctx context.Context, followerIndex int, cancel func()) bool {
	var ok int
	reply := &AppendReply{}
	rf.mu.Lock()
	select {
	case <-ctx.Done():
		rf.mu.Unlock()
		return false
	default:
	}
	if rf.role != roleLeader {
		cancel()
		rf.mu.Unlock()
		return false
	}
	rf.updateCommitIndex()
	if rf.nextIndex[followerIndex] <= rf.lastLogInclude {
		return rf.doSendInstallSnap(followerIndex, ctx)
	}
	req := &AppendReq{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.nextIndex[followerIndex] - 1,
		PrevLogTerm:  rf.getLog(rf.nextIndex[followerIndex] - 1).Term,
		Entries:      rf.superLogs[rf.real(rf.nextIndex[followerIndex]):],
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	if ok = rf.sendAppendEntries(followerIndex, req, reply); ok == RpcSuccess {
		if len(req.Entries) == 0 {
			//心跳 略
			return false
		}
		rf.mu.Lock()
		select {
		case <-ctx.Done():
			rf.mu.Unlock()
			return false
		default:
			if rf.role != roleLeader {
				cancel()
				rf.mu.Unlock()
				return false
			}
			still := rf.dealReply(followerIndex, req, reply)
			rf.mu.Unlock()
			return still
		}
	}
	//根据appendenry返回值
	if ok == SeeLeader {
		return false
	}
	//超时了 睡一下吧
	if ok == NoReply {
		time.Sleep(appendRpcTimeoutRetryInterval)
	} else {
		panic(ok)
	}
	return true
}

// 返回值：是否还要继续append？
func (rf *Raft) dealReply(followerIndex int, req *AppendReq, reply *AppendReply) bool {
	if reply.Old {
		panic("valid")
	}
	if reply.Success {
		rf.matchIndex[followerIndex] = req.PrevLogIndex + len(req.Entries)
		rf.nextIndex[followerIndex] = rf.matchIndex[followerIndex] + 1
		return false
	} else {
		if reply.Len != 0 {
			rf.nextIndex[followerIndex] = reply.Len
		} else {
			kdex := -1
			for k := rf.getVirtualEndLogIndex(); k >= rf.getVirtualBeginLogIndex(); k-- {
				if rf.getLog(k).Term == reply.Xterm {
					kdex = k
					break
				}
			}
			//存在
			if kdex != -1 {
				rf.nextIndex[followerIndex] = kdex + 1
			} else {
				rf.nextIndex[followerIndex] = reply.XIndex
			}
		}
	}
	return true
}
func (rf *Raft) getRandomElectionTimeout() time.Duration {
	//200-350
	return 2*electionTimeout + time.Duration(rf.rand.Intn(150))*time.Millisecond
	//return 2*electionTimeout + time.Duration(rf.rand.Intn(len(rf.peers)))*electionRandomGap
}

func (rf *Raft) fullFollowerFlag() {
	atomic.StoreInt32(&rf.followerFlag, 1)
}

func (rf *Raft) clearFollowerFlag() {
	atomic.StoreInt32(&rf.followerFlag, 0)
}

func (rf *Raft) isFollowerFlagFull() bool {
	return atomic.LoadInt32(&rf.followerFlag) == 1
}

func (rf *Raft) isMajor(num int) bool {
	return num > len(rf.peers)/2
}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	cur := rf.currentTerm
	isLeader := rf.role == roleLeader
	return int(cur), isLeader
}
func (rf *Raft) GetRole() int {

	// Your code here (2A).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	result := rf.role
	return result
}

func (rf *Raft) qiTao(ctx context.Context, okChan chan int, currentTerm int, candidateID, lastLogIndex int, lastLogTerm int) {
	var getVotesNum int32 = 1
	wg := &sync.WaitGroup{}
	dk := make(chan bool, 1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(followerIndex int, ctx context.Context) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					reply := &RequestVoteReply{}
					val := rf.sendRequestVote(followerIndex, &RequestVoteArgs{
						Term:         currentTerm,
						CandidateID:  candidateID,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					}, reply)
					if val == NoReply {
						time.Sleep(reVoteRandomTime)
						continue
					}
					if val == SeeLeader {
						ctx.Done() //这一次所有的索票协程都可以结束了
						return
					}
					if val == RpcSuccess {
						if reply.VoteGranted {
							atomic.AddInt32(&getVotesNum, 1)
						}
						return
					}
					panic("qiTao")
				}
			}
		}(i, ctx)
	}
	go func() {
		wg.Wait()
		dk <- true
	}()
	majorChan := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(checkMajorVoteInterVal)
				if rf.isMajor(int(atomic.LoadInt32(&getVotesNum))) {
					majorChan <- true
					return
				}
			}
		}
	}()
	select {
	case <-ctx.Done():
		return
	case <-majorChan:
		okChan <- MajorVote
		return
	case <-dk:
		if rf.isMajor(int(atomic.LoadInt32(&getVotesNum))) {
			okChan <- MajorVote
			return
		}
		okChan <- NoMajorVote
	}

}

func (rf *Raft) alreadyAppend(i int, entries []RaftLog) bool {
	for _, log := range entries {
		if i > rf.getVirtualEndLogIndex() {
			return false
		}
		if rf.getLog(i) != log {
			return false
		}
		i++
	}
	return true
}

func (rf *Raft) doSendInstallSnap(follower int, ctx context.Context) bool {
	req := &InstallSnapReq{
		Term:            rf.currentTerm,
		Snap:            rf.snap,
		LastInclude:     rf.lastLogInclude,
		LastIncludeTerm: rf.lastLogIncludeTerm,
	}
	reply := &InstallSnapReply{}
	rf.mu.Unlock()
	ok := rf.sendInstallSnap(follower, req, reply)
	if ok == RpcSuccess {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		default:
			if rf.role != roleLeader {
				return false
			}
			if reply.Success {
				rf.matchIndex[follower] = req.LastInclude
				rf.nextIndex[follower] = req.LastInclude + 1
			}
			//给一个心跳的时间让follower装载snapshot
			return false
		}
	}
	if ok == NoReply {
		time.Sleep(appendRpcTimeoutRetryInterval)
		return true
	}
	return false
}

func (rf *Raft) doApply() {
	var do bool = false
	for !rf.killed() {
		rf.mu.Lock()
		if do {
			rf.lastApplied++
		}
		if rf.commitIndex > rf.lastApplied {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.getLog(rf.lastApplied + 1).Command,
				CommandIndex: rf.lastApplied + 1,
			}
			rf.mu.Unlock()
			do = true
			rf.applyChan <- msg
		} else {
			do = false
			rf.mu.Unlock()
			time.Sleep(fuckApplyInterval)
		}
	}
}
