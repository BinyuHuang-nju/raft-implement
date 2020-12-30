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
	"sync"
)
import "labrpc"

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"
)

// interface{}: empty interface, interface{} 类型是没有方法的接口,所有类型都实现了空接口
//              这里我认为是command可标识为read/write，值与寄存器的类型也可变，所以不用具体类型表示，(或者可以用string做字符串解析？)
/*	names := []string{"stanley", "david", "oscar"}
    vals := make([]interface{}, len(names))
    for i, v := range names {
        vals[i] = v
    }*/


/*
第一次测试: 卡在TestReElection，错误原因：server2 断连后，server0与server1循环选举，并判断到对方的term更大而转为follower，从此往复
	找到原因：在StartElection中，if reply.Term > rf.currentTerm 写成了 rf.me
第二次测试：卡在TestBasicAgree，错误原因：刚开始各自选主发送heartbeat等正常，后来变成server1与server3进入candidate且死循环，从此往复
	很明显，server0、2、4在这里没有crash或延迟，是进入了死锁状态，而这三个node恰好都在成为leader后卡死，说明我在leader操作中有死锁或者死循环问题，
	把LeaderCommitLogEntries移到lock外面后跑通但报错,测试后发现server2成为leader后发heartbeat+AppendEntries+heartbeat后，
	在这个heartbeat处卡死.....
	最终锁定问题：leader在发AppendEntries后sleep一段时间内下一个heartbeat触发leader执行heartbeat，此时还没经历AppendEntries的
	LeaderCommitLogEntries，所以发出去的prevLogIndex、prevLogIndex是过期的，后来使nextIndex=matchIndex=0，且该代码处理不了乱序情况
第三次测试：依然卡在TestBasicAgree，错误原因：commitIndex与matchIndex问题，follower端的commitIndex和LastApplied迟迟不动
	找到原因：Index是从1开始的，所有与日志项有关的包括index、commitIndex等,现在开始给所有需要偏移1的变量进行偏移
第四次测试：依然卡在TestBasicAgree，后来仔细寻找，找到MakeAppendEntriesArgs中写了rf.nextIndex[target] >= len(rf.log)，应该为 '>'，
	以及AppendEntries中rf.log[args.PrevLogIndex].Term、rf.log[targetIndex].Term的[]中均要加上-1
第五次测试：卡在TestFailAgree，LeaderCommit: 5 , server  0  commitIndex: 5  applyIndex: 1 , len(log): 1，显然commitIndex应该<= len
	是因为在AppendEntries中，只要contain&&leaderCommit>rf.commitIndex就更新rf.commitIndex这样是不妥的，因为可能这个AppendEntries不带日志项，
	导致log长度反而小于rf.commitIndex，导致后续apply的时候发现数组越界问题
第六次测试：卡在TestFailAgree，且两个follower循环在LeaderCommit: 5 , server  1  commitIndex: 4  applyIndex: 4 , len(log): 5，
	这是因为我在leader端每次发送AppendEntries或heartbeat都会自动更新apply，而follower端则要收到且非heartbeat才会更新
	修改了AppendEntries中	rf.commitIndex = Min(args.LeaderCommit, len(rf.log))，即使是heartbeat也能更新rf.commitIndex
	修改完这个bug后，2A、2B的BasicAgree FailAgree FailNoAgree ConcurrentStarts Rejoin Backup Count 目前都已经通过
	Persist1通过 Persist2、Persist3有问题 后面的只能过Figure8
第七次测试： Persist2大部分时候不过，偶尔能过，有一次报错原因：panic: send on closed channel，这是传输超时 replyCh <- reply，所以考虑什么情况
	下需要把停止发送,故在ReadyToSendAppendEntries中也加入了定时器，到达一定时间停止发送，防止出现panic。
第八次测试： Persist3:runtime error: index out of range [-1] 位置357行：rf.log = append(rf.log, args.Entries[i-begin])
	args.PrevLogIndex: 2，LeaderCommit: 0 , server  1  commitIndex: 1  applyIndex: 1 , len(log): 1
	可以看出，这是在server2作为leader restart后，它本地的commitIndex初始为0，导致出现了LeaderCommit<rf.commitIndex的情况
	在persist和readPersist加入commitIndex后仍然为原问题，可能是我的persist写在锁外面单独调用一次锁，导致非原子性在中间直接crash等情况
第九次测试：在AppendEntries修补了follower日志空洞导致的数组下标越界问题，但发现测试中leaderCommit始终为0且落后于follower的commitIndex
	修改了Make中readPersist的方式，使得持久化数据能被正常读入了，并加入了保存commitIndex，随后Persist3稳定通过，Persist2也通过率上升到90%左右

	但是这显然是不加commitIndex的持久化也应该对的，后来找到在LeaderSendAppendEntries中
	if reply.Contain == true && reply.NextTestIndex > args.PrevLogIndex+1 { 把&&后半删去即可通过

目前分布式系统大作业要求的必做+选做基本无问题，在后续的测试中，Figure8,TestUnreliableAgree,TestReliableChurn,TestUnreliableChurn通过，
大部分都出在panic: send on closed channel，说明sendAppendEntries模块依然需要对信道和接收做优化。

优化完成，目前情况：除了Persist2与TestFigure8Unreliable仅有一半几率过以外，其他用例全部通过。
*/

/* optimization
1: In AppendEntries, nextIndex can decrease more which could only decrement
2: In LeaderSendAppendEntries, increase commitIndex faster
*/

const FOLLOWER_STATE int = 0
const CANDIDATE_STATE int = 1
const LEADER_STATE int = 2
const HeartbeatPeriod = 50
const ElectionTimeBase = 250
const Interval = 25
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type logEntry struct{
	Index 	int
	Term 	int
	Command interface{}
}
//
// A Go object implementing a single Raft peer.
//
/*type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}*/
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state  	int   // the state of server (follower/candidate/leader)

	// persistent state on all servers , Updated on stable storage before responding to RPCs
	currentTerm		int   // latest term server has seen (initialized to -1)
	votedFor		int	  // candidateId that received vote in current term(initialized to -1)
	log				[]logEntry

	// volatile state on all servers
	commitIndex		int   // index of highest log entry known to be committed (initialized to -1)
	lastApplied		int   // index of highest log entry applied to state machine (initialized to -1)
	//voteNum			int

	// volatile state on leaders , reinitialized after election
	nextIndex		[]int // index of the next log entry to send to some server (initialized to leader last log index+1)
	matchIndex		[]int // index of highest log entry known to be replicated on server
	heartbeatTimer	*time.Timer

	// volatile state to guarantee consistency when rpc is out of order
	leaderSendNum	int	 // leader
	maxSeenNum		int  // follower
	consistent		bool

	// timers of heartbeat and election timeout
	heartbeatTimeOut	int
	electionTimeOut		int
	electionTimer 		*time.Timer

	applyChannel chan ApplyMsg //a channel on which the tester or service expects Raft to send ApplyMsg messages.
	killed	     chan int      // if alive then 0, killed than 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER_STATE
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// because there exists lock in RequestVote, there is no need to lock
	// But I change the location to move persist() out of mu.lock()
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.electionTimeOut)
	e.Encode(rf.log)
	//e.Encode(rf.commitIndex) // new add
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//fmt.Println("======================================================")
	//fmt.Println("Server ",rf.me," have saved persistent data.len(log):",len(rf.log),",commitIndex:",rf.commitIndex)
	//fmt.Println("======================================================")
}

//
// restore previously persisted state.
// 恢复机制,很显然需要和persist()有相同的存储和读取方式保证对应数据一致
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.electionTimeOut)
	d.Decode(&rf.log)
	//d.Decode(&rf.commitIndex)
}

type RequestVoteArgs struct {
	// Your data here.
	Term			int  // candidate's term
	CandidateId 	int	 // candidate requesting vote
	LastLogIndex	int  // index of candidate's last log entry
	LastLogTerm		int	 // term of candidate's last log entry
}

type RequestVoteReply struct {
	// Your data here.
	Term			int  // currentTerm, for candidate to update itself
	VoteGranted		bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term			int  // leader's term
	LeaderId		int	 // so follower can redirect clients
	PrevLogIndex	int	 // index of log entry immediately preceding new ones
	PrevLogTerm		int  // term of prevLogIndex entry
	Entries 		[]logEntry  // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit	int  // leader's commitIndex
	LeaderSendNum	int
}

type AppendEntriesReply struct {
	Term			int  // currentTerm, for leader to update itself
	Success			bool // false if term stale
	Contain			bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	NextTestIndex	int
	Id 				int  // server ID
	Newest			bool // to guarantee the rpc is not out of order
	PrevLogIndex	int
}

func Min(x, y int) int {
	if x<y {
		return x
	}
	return y
}
func Max(x,y int) int {
	if x>y {
		return x
	}
	return y
}

func (rf *Raft) ResetElectionTimer(){
	//rf.mu.Lock() // one operation is atomic and does not need to add lock
	//defer rf.mu.Unlock()
	rf.electionTimer.Reset(time.Duration(rf.electionTimeOut)*time.Millisecond)
}
func (rf *Raft) ResetHeartbeatTimer(){
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.heartbeatTimer.Reset(time.Duration(rf.heartbeatTimeOut)*time.Millisecond)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	//defer rf.persist()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm{
		// change state to become a follower
		rf.CanLeaBecomeFollower(args.Term)
		reply.Term = args.Term
		reply.VoteGranted = true
	} else{
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		if (rf.votedFor != -1 && rf.votedFor != args.CandidateId) || rf.state != FOLLOWER_STATE {
			reply.VoteGranted = false
		}
	}
	if reply.VoteGranted == true {
		// the log of candidate should be more up-to-date
		var last = len(rf.log)-1
		if last >= 0 {
			if args.LastLogTerm > rf.log[last].Term ||
				(args.LastLogTerm == rf.log[last].Term && args.LastLogIndex >= rf.log[last].Index) {
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		}
	}
	if reply.VoteGranted == true {
		rf.votedFor = args.CandidateId
		rf.ResetElectionTimer() // new term, reset time of election
	}
	rf.persist()
	//fmt.Println("======================================================")
	//fmt.Println("Server ",rf.me,": Receive RequestVote from server ",args.CandidateId)
	//fmt.Println("before this, votedFor:",rf.votedFor," ,currentTerm:",rf.currentTerm," so result:",reply.VoteGranted)
	//fmt.Println("======================================================")
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply){  //ID not considered
	rf.mu.Lock()
	reply.Id = rf.me
	reply.PrevLogIndex = args.PrevLogIndex
	//reply.PrevLogTerm = args.PrevLogTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.persist()
		rf.mu.Unlock()
		return
	}
	reply.Success = true
	reply.Term = args.Term
	rf.ResetElectionTimer()
	if args.Term > rf.currentTerm {
		rf.CanLeaBecomeFollower(args.Term)
	} else {
		if rf.state == LEADER_STATE {
			fmt.Println("It is impossible!!! There are two leaders in the same term!! ")
		} else if rf.state == CANDIDATE_STATE {
			rf.state = FOLLOWER_STATE
		}
	}
	//fmt.Println("======================================================")
	//fmt.Println("args.PrevLogIndex:",args.PrevLogIndex," ,len(args.Entries):",len(args.Entries),",rf.consistent =",rf.consistent)
	//fmt.Println("args.LeaderSendNum = ",args.LeaderSendNum,", rf.maxSeenNum = ",rf.maxSeenNum)
	//fmt.Println("LeaderCommit:",args.LeaderCommit,", server ",rf.me," commitIndex:",rf.commitIndex," applyIndex:",rf.lastApplied,", len(log):",len(rf.log))
	if rf.votedFor != args.LeaderId { //为了考虑领导者crash替换了leader，但follower不知情，这时可能log和新leader不匹配
		rf.consistent = false
		rf.maxSeenNum = -1
		rf.votedFor = args.LeaderId
	}
	if args.LeaderSendNum < rf.maxSeenNum {
		reply.Newest = false
	} else {
		reply.Newest = true
		rf.maxSeenNum = args.LeaderSendNum
	}
	if rf.consistent == true {
		reply.Contain = true
		if !reply.Newest {
			rf.mu.Unlock()
			return
		}
		if len(args.Entries) == 0 {
			reply.NextTestIndex = args.PrevLogIndex+1
		} else {
			begin := args.Entries[0].Index-1
			if begin != args.PrevLogIndex {
				fmt.Println("It is impossible!!! args.PrevLogIndex+1 != rf.indexNext[target]")
			}
			leaderLen := args.Entries[len(args.Entries)-1].Index
			//fmt.Println("begin:",begin," leaderLen:",leaderLen,", len(rf.log):",len(rf.log))
			if begin > len(rf.log) {   // contain, but there exists gap between leader's Entries and follower's log
				reply.NextTestIndex = len(rf.log) + 1
			} else {
				for i := begin; i < len(rf.log) && i < leaderLen; i++ {
					if rf.log[i].Term != args.Entries[i-begin].Term {
						rf.log[i] = args.Entries[i-begin]
					}
				}
				if len(rf.log) < leaderLen {
					for i := len(rf.log); i < leaderLen; i++ {
						rf.log = append(rf.log, args.Entries[i-begin])
						// Persist3: a bug reaches here, begin: 2  leaderLen: 3 , len(rf.log): 1,len(args.Entries): 1
					}
				} else if len(rf.log) > leaderLen {
					rf.log = rf.log[:leaderLen]
				}
				reply.NextTestIndex = leaderLen + 1
			}
		}
	} else {
		if args.PrevLogIndex <= 0 || args.PrevLogIndex < rf.commitIndex || args.LeaderCommit < rf.commitIndex{
			reply.Contain = true
			reply.NextTestIndex = rf.commitIndex + 1
		} else if len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Contain = false                   //optimization1
			var targetIndex int = Min(args.PrevLogIndex-1,len(rf.log))   // improve efficiency from decrement to decrease more
			for targetIndex > 0 && rf.log[targetIndex-1].Term != args.PrevLogTerm { // initially '>', now change to '!='
				targetIndex--
			}
			reply.NextTestIndex = targetIndex
		} else {
			reply.Contain = true
			reply.NextTestIndex = args.PrevLogIndex + 1
		}
		if len(args.Entries) == 0 {  // heartbeat
			// have reset election timer

		} else if reply.Contain && reply.Newest {                    // normal AppendEntries RPC
			begin := args.Entries[0].Index-1
			if begin != args.PrevLogIndex {
				fmt.Println("It is impossible!!! args.PrevLogIndex+1 != rf.indexNext[target]")
			}
			leaderLen := args.Entries[len(args.Entries)-1].Index
			for i:= begin; i < len(rf.log) && i < leaderLen;i++ {
				if rf.log[i].Term != args.Entries[i-begin].Term {
					rf.log[i] = args.Entries[i-begin]
				}
			}
			if len(rf.log) < leaderLen {
				for i:=len(rf.log); i< leaderLen; i++ {
					rf.log = append(rf.log, args.Entries[i-begin])
				}
			} else if len(rf.log) > leaderLen {
				rf.log = rf.log[:leaderLen]
			}
			reply.NextTestIndex = leaderLen+1
		}
	}
	if reply.Contain {
		rf.consistent = true
	}
	if reply.Contain && args.LeaderCommit > rf.commitIndex && reply.Newest {
		//rf.commitIndex = Min(args.LeaderCommit, len(rf.log))
		if args.LeaderCommit > len(rf.log) {
			fmt.Println("It is impossible!!! Now that Contain is true, then len(rf.log)==len(leader's log)")
		}
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log))
	}
	/*fmt.Println("Server ",rf.me,": Receive AppendEntries from server(leader) ",args.LeaderId)
	if len(args.Entries) == 0 {
		fmt.Println("The AppendEntries RPC is heartbeat")
	}
	fmt.Println("LeaderCommit:",args.LeaderCommit,", server ",rf.me," commitIndex:",rf.commitIndex," applyIndex:",rf.lastApplied,"reply.nextTestIndex:",reply.NextTestIndex,", len(log):",len(rf.log))
	fmt.Println("======================================================")*/
	rf.persist()
	rf.mu.Unlock()

	//rf.persist()
	rf.AllServerApplyLogEntries()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// 客户端的一次日志请求操作触发
// 1)Leader将该请求记录到自己的日志之中;
// 2)Leader将请求的日志以并发的形式,发送AppendEntries RCPs给所有的服务器;
// 3)Leader等待获取多数服务器的成功回应之后(如果总共5台,那么只要收到另外两台回应),
// 将该请求的命令应用到状态机(也就是提交),更新自己的commitIndex 和 lastApplied值;
// 4)Leader在与Follower的下一个AppendEntries RPCs通讯中,
// 就会使用更新后的commitIndex,Follower使用该值更新自己的commitIndex;
// 5)Follower发现自己的 commitIndex > lastApplied
// 则将日志commitIndex的条目应用到自己的状态机(这里就是Follower提交条目的时机)
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := len(rf.log)+1
	term := rf.currentTerm
	isLeader := rf.state == LEADER_STATE
	if isLeader == false {
		rf.mu.Unlock()
		return index, term, false
	}
	entry := logEntry{}
	entry.Index = index
	entry.Term = rf.currentTerm
	entry.Command = command
	rf.log = append(rf.log, entry)
	rf.persist()
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	// the code to send AppendEntries RPC immediately and reset heartbeatTimeout
	rf.mu.Unlock()
	//rf.LeaderSendAppendEntries()
	rf.LeaderSendAppendEntries2()

	//rf.persist()	// because there exists lock in persist(), so it should not be lock()-> lock()-> unlock()-> unlock()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.killed <- 1
}

func (rf *Raft)ReadyToSendRequestVote(target int, args RequestVoteArgs, replyCh chan *RequestVoteReply) {
	reply := &RequestVoteReply{}
	timeout := time.NewTimer(time.Duration(rf.electionTimeOut - rf.heartbeatTimeOut)*time.Millisecond)
	var received = false
	for received == false {
		select {
		case <-timeout.C:
			return
		default:
			received = rf.sendRequestVote(target, args, reply)
		}
	}
	if received == true {
		replyCh <- reply
	}
	/*fmt.Println("======================================================")
	fmt.Println("Server ",rf.me,": Send RequestVote to server ",target, " currentTerm:",args.Term )
	if received == true {
		fmt.Println("The RequestVote RPC has been sent successfully.")
	}
	fmt.Println("======================================================")*/
}

func (rf *Raft)ReadyToSendAppendEntries(target int, args AppendEntriesArgs, replyCh chan *AppendEntriesReply,timeout bool) {
	reply := &AppendEntriesReply{}
	//timeout := time.NewTimer(time.Duration(rf.heartbeatTimeOut-2*Interval)*time.Millisecond)
	//mutex := sync.Mutex{}
	var received = false

	if rf.sendAppendEntries(target, args, reply) {
		if !timeout {
			received = true
			replyCh <- reply
		}
	}
	/*for received == false {
		select {
		case <-timeout.C:
			return
		default:
			received = rf.sendAppendEntries(target, args, reply)
		}
	}
	if received == true {
		replyCh <- reply
	}*/

	fmt.Println("======================================================")
	fmt.Println("Server ",rf.me,": Send AppendEntries to server ",target)
	if len(args.Entries) ==0 {
		fmt.Println("This is a heartbeat.")
	}
	if received == true {
		fmt.Println("The AppendEntries RPC has been sent successfully.")
	}
	fmt.Println("======================================================")
}

func (rf *Raft)MakeRequestVoteArgs(lli, llt int) RequestVoteArgs {
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = lli
	args.LastLogTerm = llt
	return args
}


func (rf *Raft)MakeAppendEntriesArgs(target int,isHeartbeat bool) AppendEntriesArgs {
	rf.mu.Lock()
	args := AppendEntriesArgs{}
	if rf.state != LEADER_STATE {
		rf.mu.Unlock()
		return args
	}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = 0
	args.PrevLogTerm = 0
	//rf.leaderSendNum = rf.leaderSendNum + 1
	args.LeaderSendNum = rf.leaderSendNum
	if rf.nextIndex[target] > 1 {
		args.PrevLogIndex = rf.log[rf.nextIndex[target]-2].Index
		args.PrevLogTerm = rf.log[rf.nextIndex[target]-2].Term
	}
	if isHeartbeat || rf.nextIndex[target] > len(rf.log) {
		args.Entries = []logEntry{}
	} else { // just normal AppendEntries RPC, which should contain log entries from index nextIndex[target] to end
		entries := make ([]logEntry, len(rf.log)-rf.nextIndex[target]+1)
		for i:=0; i < len(entries); i++ {
			entries[i] = rf.log[rf.nextIndex[target]+i-1]
		}
		args.Entries = entries
	}
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()
	return args
}

func (rf *Raft)FolCanStartElection() {
	// times out, starts election
	rf.mu.Lock()
	rf.state = CANDIDATE_STATE
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.ResetElectionTimer()
	var lli, llt, le int = -1, -1, len(rf.log)
	if le > 0 {
		lli = rf.log[le-1].Index
		llt = rf.log[le-1].Term
	}
	args := rf.MakeRequestVoteArgs(lli, llt)
	rf.persist()
	rf.mu.Unlock()

	mutex := sync.Mutex{}
	var gate = false
	replyCh := make(chan *RequestVoteReply, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// there is no lock, so the server may receive RequestVote or AppendEntries anytime and may turn to follower
		if rf.state == FOLLOWER_STATE {
			break
		}
		//go rf.ReadyToSendRequestVote(i, args, replyCh)
		go func(target int, replyCh chan *RequestVoteReply) {
			reply := &RequestVoteReply{}
			timeout := time.NewTimer(time.Duration(rf.electionTimeOut - rf.heartbeatTimeOut)*time.Millisecond)
			var received = false
			for received == false {
				select {
				case <-timeout.C:
					return
				default:
					received = rf.sendRequestVote(target, args, reply)
				}
			}
			if received == true {
				mutex.Lock()
				if !gate {
					replyCh <- reply
				}
				mutex.Unlock()
			}
		}(i,replyCh)
	}
	//理论上在timeout后这里应该在下一个candidate里处理，这里保留后续可做优化
	time.Sleep(time.Duration(rf.electionTimeOut-rf.heartbeatTimeOut+Interval) * time.Millisecond)
	mutex.Lock()
	gate = true
	mutex.Unlock()
	close(replyCh)

	if rf.state == FOLLOWER_STATE {
		rf.ResetElectionTimer()
		rf.persist()
		return // if not func, change to 'break'
	}

	rf.mu.Lock()
	var voteNum, total = 1, len(rf.peers)
	for reply := range replyCh {
		if reply.Term > rf.currentTerm { // because the decision if rf.state == FOLLOWER_STATE before, here rf.currentTerm == args.Term
			rf.CanLeaBecomeFollower(reply.Term)
		} else {
			if reply.VoteGranted == true {
				voteNum = voteNum + 1
			}
		}
	}
	//fmt.Println("======================================================")
	//fmt.Println("Server ",rf.me," state:",rf.state,", voteNum:",voteNum,", total:",total)
	if rf.state == FOLLOWER_STATE {
		rf.ResetElectionTimer()
		//fmt.Println("Turn to follower.")
		//fmt.Println("======================================================")
	} else if voteNum >= total/2+1 {
		rf.state = LEADER_STATE
		// rf.BecomeLeader()
	} else { // times out, new election
		rf.state = CANDIDATE_STATE
		rf.electionTimeOut = rand.Int()%ElectionTimeBase + ElectionTimeBase
		rf.ResetElectionTimer()
		//fmt.Println("Server ",rf.me," times out, starts new election, new electionThreld:",rf.electionTimeOut)
		//fmt.Println("======================================================")
	}
	rf.persist()
	rf.mu.Unlock()
	//rf.persist()
}

func (rf *Raft)LeaderCommitLogEntries() {
	rf.mu.Lock()
	if rf.commitIndex == len(rf.log) || rf.state != LEADER_STATE {
		rf.mu.Unlock()
		return
	}
	/*
	for i:= rf.commitIndex+1; i < len(rf.log); i++ {
		matchNum := 1
		for j:=0; j<len(rf.peers);j++ {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				matchNum = matchNum + 1
			}
		}
	}*/	// This method's efficiency is not good, optimization2
	oldCommitIndex, majority := rf.commitIndex, len(rf.peers)/2 + 1
	statistic := make([]int, len(rf.log)-oldCommitIndex)
	//fmt.Println("Leader ",rf.me," len:",len(rf.log)," ,oldCommitIndex:",oldCommitIndex," ,majority:",majority)
	for i:=0; i<len(statistic);i++ {
		statistic[i] = 1
	}
	for i:=0; i<len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] > oldCommitIndex {
			for j:=oldCommitIndex+1; j<=rf.matchIndex[i]; j++ {
				statistic[j-oldCommitIndex-1] = statistic[j-oldCommitIndex-1]+1
			}
		}
	}
	i := 0
	for ; i<len(statistic); i++ {
		if statistic[i] < majority {
			rf.commitIndex = oldCommitIndex+ i  // (oldCommitIndex+1)+(i-1)
			break
		}
	}
	if i == len(statistic) {
		rf.commitIndex = len(rf.log)
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft)AllServerApplyLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.lastApplied {
		for i:= rf.lastApplied+1; i<= rf.commitIndex; i++ {
			var msg ApplyMsg
			msg.Index = i
			msg.Command = rf.log[i-1].Command
			rf.applyChannel <- msg
		}
		rf.lastApplied = rf.commitIndex
		//fmt.Println("Server ",rf.me," has applied log entries, lastApplied:",rf.lastApplied)
	}
}

func (rf *Raft)CandidateBecomeLeader() {
	rf.mu.Lock()
	rf.state = LEADER_STATE
	rf.votedFor = rf.me
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)+1
		rf.matchIndex[i] = 0
	}
	rf.ResetElectionTimer()
	rf.persist()
	//fmt.Println("======================================================")
	//fmt.Println("Server ",rf.me," has become a leader.")
	//fmt.Println("======================================================")
	rf.mu.Unlock()
}

func (rf *Raft)CanLeaBecomeFollower(term int) {
	//rf.mu.Lock()
	rf.state = FOLLOWER_STATE
	rf.currentTerm = term
	rf.votedFor = -1
	rf.maxSeenNum = -1
	//rf.voteNum = 0
	rf.consistent = false
	rf.ResetElectionTimer()
	//rf.mu.Unlock()
}

func (rf *Raft)LeaderSendHeartbeat() {
	/*rf.mu.Lock()
	// need to modify like LeaderSendAppendEntries()
	var args AppendEntriesArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = -1
	args.PrevLogTerm = -1
	if len(rf.log) > 0 {
		args.PrevLogIndex = rf.log[len(rf.log)-1].Index
		args.PrevLogTerm = rf.log[len(rf.log)-1].Term
	}
	args.Entries = []logEntry{}
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock() */
	rf.mu.Lock()
	rf.leaderSendNum = rf.leaderSendNum + 1 // one word may add lock to ensure +1 everytime not + a number > 1
	rf.mu.Unlock()
	replyCh := make(chan *AppendEntriesReply, len(rf.peers))
	var timeout = false
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// there is no lock, so the server may receive RequestVote or AppendEntries anytime and may turn to follower
		if rf.state == FOLLOWER_STATE {
			break
		}
		args := rf.MakeAppendEntriesArgs(i, true)
		go rf.ReadyToSendAppendEntries(i, args, replyCh, timeout)
	}
	time.Sleep(time.Duration(rf.heartbeatTimeOut-Interval) * time.Millisecond)
	timeout = true
	close(replyCh)
	if rf.state == FOLLOWER_STATE {
		//fmt.Println("Server ",rf.me," changes to follower.")
		rf.ResetElectionTimer()
		return // if not func, change to 'break'
	}

	// there should be code to handle replys from followers
	rf.mu.Lock()
	rf.ResetHeartbeatTimer()
	//fmt.Println("======================================================")
	//fmt.Println("Leader ",rf.me,",")
	for reply := range replyCh {
		if reply.Success == false && reply.Term > rf.currentTerm {
			rf.CanLeaBecomeFollower(reply.Term)
			break
		} else {
			if !reply.Newest {
				continue
			}
			if reply.Contain == true && reply.NextTestIndex > reply.PrevLogIndex+1 {
				rf.nextIndex[reply.Id] = reply.NextTestIndex
				rf.matchIndex[reply.Id] = Max(reply.NextTestIndex-1,rf.matchIndex[reply.Id])// in case of misleading order
			} else if reply.Contain == false {
				rf.nextIndex[reply.Id] = reply.NextTestIndex+1    // if !contain, there need to be +1
				// when false , it should not change matchIndex
			}
		}
		fmt.Println("From server ",reply.Id,"nextIndex is:",rf.nextIndex[reply.Id],",matchIndex is:",rf.matchIndex[reply.Id])
	}
	fmt.Println("======================================================")
	rf.persist()
	rf.mu.Unlock()
	rf.LeaderCommitLogEntries()
	rf.AllServerApplyLogEntries()
}

func (rf *Raft)LeaderSendAppendEntries() {
	fmt.Println("======================================================")
	fmt.Println("Leader ",rf.me," begins to send AppendEntries RPC, the leaderLen = ",len(rf.log))
	fmt.Println("======================================================")
	rf.mu.Lock()
	rf.leaderSendNum = rf.leaderSendNum + 1
	rf.mu.Unlock()
	replyCh := make(chan *AppendEntriesReply, len(rf.peers))
	var timeout = false
	for i:=0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.state == FOLLOWER_STATE {
			break
		}
		args := rf.MakeAppendEntriesArgs(i,false)
		go rf.ReadyToSendAppendEntries(i, args, replyCh, timeout)
	}
	time.Sleep(time.Duration(rf.heartbeatTimeOut-Interval) * time.Millisecond)
	timeout = true
	close(replyCh)  // close will produce dead loop
	if rf.state == FOLLOWER_STATE {
		rf.ResetElectionTimer()
		return // if not func, change to 'break'
	}
	// 这儿使用统一发、统一收处理不了超时问题，试想如果超时到>rf.heartbeatTimeout-Interval而<rf.electionTimeout，
	// 则leader一直收不到reply，而followers都认为leader有效而不进行election，陷入了无法推进的状态(尽管几乎没这种可能性
	// 所以这里应该改成func1统一发+func2信道监听陆续收，RequestVote因为要对收到的rpc做统一处理可以不改，后续有空要改
	rf.mu.Lock()
	rf.ResetHeartbeatTimer()
	fmt.Println("======================================================")
	fmt.Println("Leader ",rf.me,",")
	for reply := range replyCh {
		if reply.Success == false && reply.Term > rf.currentTerm {
			rf.CanLeaBecomeFollower(reply.Term)
			break
		} else {
			if !reply.Newest {
				continue
			}
			if reply.Contain == true && reply.NextTestIndex > reply.PrevLogIndex+1 {
				rf.nextIndex[reply.Id] = reply.NextTestIndex
				rf.matchIndex[reply.Id] = Max(reply.NextTestIndex-1,rf.matchIndex[reply.Id])// in case of misleading order
			} else if reply.Contain == false {
				rf.nextIndex[reply.Id] = reply.NextTestIndex+1    // if !contain, there need to be +1
				// when false , it should not change matchIndex
			}
		}
		fmt.Println("From server ",reply.Id,"nextIndex is:",rf.nextIndex[reply.Id],",matchIndex is:",rf.matchIndex[reply.Id])
	}
	fmt.Println("======================================================")
	rf.persist()
	rf.mu.Unlock()
	rf.LeaderCommitLogEntries()
	rf.AllServerApplyLogEntries()
}
/*
func (rf *Raft)FolCanStartElection2() {
	rf.mu.Lock()
	rf.state = CANDIDATE_STATE
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.voteNum = 1
	rf.ResetElectionTimer()
	var lli, llt, le int = -1, -1, len(rf.log)
	if le > 0 {
		lli = rf.log[le-1].Index
		llt = rf.log[le-1].Term
	}
	args := rf.MakeRequestVoteArgs(lli, llt)
	rf.persist()
	rf.mu.Unlock()

	for i :=0; i<len(rf.peers); i++ {
		if
	}
}
*/
func (rf *Raft)SendAndRevAppendEntries(target int) {
	args := rf.MakeAppendEntriesArgs(target, false)
	reply := &AppendEntriesReply{}
	var received = rf.sendAppendEntries(target, args, reply)
	rf.mu.Lock()
	if received == true {
		if reply.Success == false && reply.Term > rf.currentTerm {
			rf.CanLeaBecomeFollower(reply.Term)
			rf.mu.Unlock()
			return
		} else {
			if !reply.Newest {
				rf.mu.Unlock()
				return
			}
			//fmt.Println("reply.Contain=",reply.Contain,",reply.NextTestIndex=",reply.NextTestIndex,"args.PrevLogIndex=",args.PrevLogIndex)
			//if reply.Contain == true && reply.NextTestIndex > args.PrevLogIndex+1 {
			if reply.Contain == true {
				rf.nextIndex[target] = reply.NextTestIndex
				rf.matchIndex[target] = Max(reply.NextTestIndex-1, rf.matchIndex[target])
			} else {
				rf.nextIndex[target] = reply.NextTestIndex + 1
			}
		}
		//fmt.Println("Leader ",rf.me,"currentTerm:",rf.currentTerm,"has log entries:",len(rf.log),"leaderCommit:",rf.commitIndex," From server ",target,"nextIndex is:",rf.nextIndex[target],",matchIndex is:",rf.matchIndex[target])
	}
	rf.mu.Unlock()
}

func (rf *Raft)LeaderSendAppendEntries2() {
	// 对LeaderSendHeartbeat和LeaderSendAppendEntries做统一优化处理
	rf.mu.Lock()
	rf.leaderSendNum = rf.leaderSendNum + 1
	rf.ResetHeartbeatTimer()
	rf.mu.Unlock()
	for i:= 0; i< len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.state == FOLLOWER_STATE {
			break
		}
		//args := rf.MakeAppendEntriesArgs(i, false)
		go rf.SendAndRevAppendEntries(i)
	}
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
	rf.LeaderCommitLogEntries()
	rf.AllServerApplyLogEntries()
}

func (rf *Raft) Loop() {
	rf.ResetElectionTimer()
	rf.ResetHeartbeatTimer()
	for {
		select {
		case <-rf.electionTimer.C:
			rf.ResetElectionTimer()
			var ifLeader_NewLeader bool = false
			if rf.state == FOLLOWER_STATE {
				// times out, starts election
				ifLeader_NewLeader = true
				rf.FolCanStartElection()
			}else if rf.state == CANDIDATE_STATE { // there should not be 'else if',because rf may times out and start new election
				ifLeader_NewLeader = true
				rf.FolCanStartElection()
			}
			if ifLeader_NewLeader == true && rf.state == LEADER_STATE {
				rf.CandidateBecomeLeader()
				//rf.LeaderSendHeartbeat()
				go rf.LeaderSendAppendEntries2()
			}
		case <-rf.heartbeatTimer.C:
			switch rf.state {
			case FOLLOWER_STATE:
				rf.ResetHeartbeatTimer()
			case CANDIDATE_STATE:
				rf.ResetHeartbeatTimer()
			case LEADER_STATE:
				go rf.LeaderSendAppendEntries2()
			}
		case <-rf.killed:
			//fmt.Println("Server ",rf.me," has crashed.")
			return
		default:
			rf.AllServerApplyLogEntries()
		}
	}
}

func (rf *Raft) Loop2() {
	for {
		switch rf.state {
		case FOLLOWER_STATE:
		case CANDIDATE_STATE:
		case LEADER_STATE:
		}
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
	// step1. initialize each variables of server node
	// step2. create a go routine to judge whether start election
	// step3. if step2 passes, then become a leader and send heartbeat and AppendEntriesRPC
	// step4. if crash, end; if lose leadership, goto 2
	// Your initialization code here.
	rf.state = FOLLOWER_STATE

	// initialize from state persisted before a crash
	/*if persister.ReadRaftState() != nil {
		rf.readPersist(persister.ReadRaftState())
	} else {
		rf.currentTerm = -1
		rf.votedFor = -1
		rf.log = []logEntry{}
		rf.electionTimeOut = rand.Int()%ElectionTimeBase + ElectionTimeBase
		rf.commitIndex = 0
	}*/
	rf.currentTerm = -1
	rf.votedFor = -1
	rf.log = []logEntry{}
	rf.electionTimeOut = rand.Int()%ElectionTimeBase + ElectionTimeBase
	rf.commitIndex = 0
	rf.readPersist(persister.ReadRaftState())
	//fmt.Println("Server ",rf.me," restarts, the commitIndex is:",rf.commitIndex,"len(log)=",len(rf.log))

	rf.lastApplied = 0
	rf.heartbeatTimeOut = HeartbeatPeriod
	rf.electionTimer = time.NewTimer(time.Duration(rf.electionTimeOut)*time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(time.Duration(rf.heartbeatTimeOut)*time.Millisecond)
	rf.leaderSendNum = -1
	rf.maxSeenNum = -1
	rf.consistent = false
	rf.applyChannel = applyCh
	rf.killed = make(chan int)

	go rf.Loop()

	return rf
}
