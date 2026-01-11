package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"raft/labgob"
	"raft/labrpc"
	"raft/raftapi"
	tester "raft/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm             int
	votedFor                int
	log                     []LogEntry
	electionTimeoutInterval time.Duration
	state                   State
	applyCh                 chan raftapi.ApplyMsg

	// Volatile state on all servers
	commitIndex   int
	lastApplied   int
	voteCount     int
	lastHeartbeat time.Time

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

type LogEntry struct {
	TermReceived int
	Command      interface{}
}

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}

	rf.snapshot = rf.persister.ReadSnapshot()
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Don't snapshot if this index is already included
	if index <= rf.lastIncludedIndex {
		return
	}

	// Don't snapshot past end of log
	if index > rf.getLastLogIndex() {
		return
	}

	relativeIndex := rf.getRelativeLogIndex(index)

	// ensure index is present in log slice
	if relativeIndex < 0 || relativeIndex >= len(rf.log) {
		return
	}

	// Update snapshot boundary
	rf.lastIncludedTerm = rf.log[relativeIndex].TermReceived
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot

	// Keep entries after index
	rf.log = rf.log[relativeIndex+1:]

	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	if rf.commitIndex < index {
		rf.commitIndex = index
	}

	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	candidateTerm := args.Term
	candidateId := args.CandidateId
	candidateLastLogIndex := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm

	// If candidate's term is less than current term, reject vote
	if candidateTerm < rf.currentTerm {
		return
	}

	if candidateTerm > rf.currentTerm {
		rf.currentTerm = candidateTerm
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		reply.Term = rf.currentTerm
	}

	myLastLogIndex := rf.getLastLogIndex()
	myLastLogTerm := rf.getLastLogTerm()
	// If candidate's log is not up to date, reject vote
	if candidateLastLogTerm < myLastLogTerm || (candidateLastLogTerm == myLastLogTerm && candidateLastLogIndex < myLastLogIndex) {
		return
	}

	if rf.votedFor == -1 || rf.votedFor == candidateId {
		rf.votedFor = candidateId
		rf.persist()
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now()
	}
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	LogSize int
	XTerm   int
	XIndex  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	leadersTerm := args.Term

	if leadersTerm < rf.currentTerm {
		return
	}

	rf.lastHeartbeat = time.Now()
	rf.state = Follower

	if leadersTerm > rf.currentTerm {
		rf.currentTerm = leadersTerm
		rf.votedFor = -1
		rf.persist()
		reply.Term = rf.currentTerm
	}

	leaderPrevIndex := args.PrevLogIndex

	reply.LogSize = -1
	reply.XIndex = -1
	reply.XTerm = -1

	// If follower's log is shorter than leader's prev index, tell leader size of log
	if rf.getLastLogIndex() < leaderPrevIndex {
		reply.LogSize = rf.getLastLogIndex()
		return
	}

	// If prev entry is before our snapshot, tell leader our snapshot index
	if leaderPrevIndex < rf.lastIncludedIndex {
		reply.LogSize = rf.lastIncludedIndex
		return
	}

	expectedTerm := rf.getLogTerm(leaderPrevIndex)

	if expectedTerm != args.PrevLogTerm {
		reply.XTerm = expectedTerm
		reply.XIndex = leaderPrevIndex

		for reply.XIndex > rf.lastIncludedIndex {
			entryTerm := rf.getLogTerm(reply.XIndex - 1)
			if entryTerm != reply.XTerm {
				break
			}
			reply.XIndex--
		}
		return
	}

	reply.Success = true

	incorrectEntriesIdx := leaderPrevIndex + 1
	entryIndex := 0

	// Find where logs diverge
	for entryIndex < len(args.Entries) && incorrectEntriesIdx <= rf.getLastLogIndex() {
		entry := rf.getAbsoluteLogEntry(incorrectEntriesIdx)
		if entry == nil || entry.TermReceived != args.Entries[entryIndex].TermReceived {
			// Delete conflicting entry and all after
			relativeIndex := rf.getRelativeLogIndex(incorrectEntriesIdx)
			if relativeIndex >= 0 {
				rf.log = rf.log[:relativeIndex]
			}
			break
		}
		incorrectEntriesIdx++
		entryIndex++
	}

	// Append any remaining new entries
	if entryIndex < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[entryIndex:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		lastIndex := rf.getLastLogIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}

	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.lastHeartbeat = time.Now()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		reply.Term = rf.currentTerm
	}

	rf.state = Follower

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	applyMsg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	// Check if we have any log entries that come after the snapshot
	newLog := make([]LogEntry, 0)

	if rf.getLastLogIndex() > args.LastIncludedIndex {
		relativeIndex := rf.getRelativeLogIndex(args.LastIncludedIndex)

		// If the entry exists in our log and has matching term, keep entries after it
		if relativeIndex >= 0 && relativeIndex < len(rf.log) {
			if rf.log[relativeIndex].TermReceived == args.LastIncludedTerm {
				newLog = append(newLog, rf.log[relativeIndex+1:]...)
			}
		}
	}

	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.persist()
	rf.mu.Unlock()

	rf.applyCh <- applyMsg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})

	rf.persist()

	for server := range rf.peers {
		if server != rf.me {
			go rf.replicateLog(server)
		}
	}

	return index, term, true
}

func (rf *Raft) replicateLog(server int) {
	retryTimeout := 10 * time.Millisecond
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		nIdx := rf.nextIndex[server]
		termWhenCallingRPC := rf.currentTerm

		// Check if follower needs a snapshot
		if nIdx <= rf.lastIncludedIndex {
			// Follower is behind our snapshot, send InstallSnapshot.
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.snapshot,
			}
			reply := &InstallSnapshotReply{}
			rf.mu.Unlock()

			ok := rf.sendInstallSnapshot(server, args, reply)

			if ok {
				rf.mu.Lock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					rf.lastHeartbeat = time.Now()
					rf.mu.Unlock()
					return
				}

				if rf.state == Leader && rf.currentTerm == termWhenCallingRPC {
					rf.matchIndex[server] = args.LastIncludedIndex
					rf.nextIndex[server] = args.LastIncludedIndex + 1
				}
				rf.mu.Unlock()
			} else {
				// RPC failed, retry later
				time.Sleep(retryTimeout)
			}
			continue
		}

		prevIdx := nIdx - 1
		prevTerm := rf.getLogTerm(prevIdx)
		var entries []LogEntry

		relativeIndex := rf.getRelativeLogIndex(nIdx)
		if relativeIndex >= 0 && relativeIndex < len(rf.log) {
			entries = make([]LogEntry, len(rf.log[relativeIndex:]))
			copy(entries, rf.log[relativeIndex:])
		} else {
			entries = nil
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		reply := &AppendEntriesReply{}

		rf.mu.Unlock()
		ok := rf.sendAppendEntries(server, args, reply)

		if !ok {
			time.Sleep(retryTimeout)
			continue
		}

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			rf.lastHeartbeat = time.Now()
			rf.mu.Unlock()
			return
		}

		// Ignore stale replies
		if reply.Term != termWhenCallingRPC || rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if newMatchIndex > rf.matchIndex[server] {
				rf.matchIndex[server] = newMatchIndex
				rf.nextIndex[server] = newMatchIndex + 1
			}
			rf.mu.Unlock()
			return
		}
		// Handle failure case
		newNextIdx := 1
		if reply.LogSize >= 0 {
			// Follower's log is too short or needs snapshot
			newNextIdx = reply.LogSize + 1
		} else {
			lastIndexOfXTerm := -1
			lastLog := rf.getLastLogIndex()
			for i := lastLog; i > rf.lastIncludedIndex; i-- {
				entry := rf.getAbsoluteLogEntry(i)
				if entry == nil {
					continue
				}
				if entry.TermReceived == reply.XTerm {
					lastIndexOfXTerm = i
					break
				}
				if entry.TermReceived < reply.XTerm {
					break
				}
			}

			if lastIndexOfXTerm != -1 {
				newNextIdx = lastIndexOfXTerm + 1
			} else {
				newNextIdx = reply.XIndex
			}
		}

		if newNextIdx <= rf.lastIncludedIndex {
			// Need to send snapshot on next iteration
			newNextIdx = rf.lastIncludedIndex
		}

		if newNextIdx > rf.getLastLogIndex()+1 {
			newNextIdx = rf.getLastLogIndex() + 1
		}

		if newNextIdx < 1 {
			newNextIdx = 1
		}

		rf.nextIndex[server] = newNextIdx
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyCommittedCommands() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			nextApply := rf.lastApplied + 1

			// nextApply is covered by a snapshot prefix
			if nextApply <= rf.lastIncludedIndex {
				// advance lastApplied to snapshot boundary and send snapshot
				rf.lastApplied = rf.lastIncludedIndex
				newMsg := raftapi.ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.snapshot,
					SnapshotTerm:  rf.lastIncludedTerm,
					SnapshotIndex: rf.lastIncludedIndex,
				}
				rf.mu.Unlock()
				rf.applyCh <- newMsg
				continue
			}
			// nextApply is in the in-memory log
			relIdx := rf.getRelativeLogIndex(nextApply)
			if relIdx >= 0 && relIdx < len(rf.log) {
				entry := rf.log[relIdx]
				rf.lastApplied = nextApply
				newMsg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: nextApply,
				}
				rf.mu.Unlock()
				rf.applyCh <- newMsg
				continue
			}
			rf.mu.Unlock()
			time.Sleep(1 * time.Millisecond)
			continue
		}
		rf.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	rf.mu.Lock()
	for !rf.killed() && rf.state == Leader {
		tempMatchIdx := make([]int, len(rf.matchIndex))
		copy(tempMatchIdx, rf.matchIndex)
		tempMatchIdx[rf.me] = rf.getLastLogIndex()

		sort.Slice(tempMatchIdx, func(i, j int) bool { return tempMatchIdx[i] > tempMatchIdx[j] })
		majorityIdx := len(tempMatchIdx) / 2
		nextCommit := tempMatchIdx[majorityIdx]

		if nextCommit > rf.commitIndex && nextCommit > rf.lastIncludedIndex && nextCommit <= rf.getLastLogIndex() {
			entry := rf.getAbsoluteLogEntry(nextCommit)
			if entry != nil && entry.TermReceived == rf.currentTerm {
				rf.commitIndex = nextCommit
			}
		}
		rf.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.lastHeartbeat) > rf.electionTimeoutInterval {
			rf.lastHeartbeat = time.Now()
			go rf.startElection()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350 milliseconds.
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	// To begin ellection, incremenet current term, transition to candidate state, and vote for self
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.lastHeartbeat = time.Now()
	rf.electionTimeoutInterval = time.Duration(400+rand.Intn(200)) * time.Millisecond
	rf.persist()

	// Prepare RequestVote RPC arguments
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	currentTerm := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()

	// Send RequestVote RPCs to peers
	for i := range rf.peers {
		if i == me {
			continue
		}

		go func(server int) {
			requestVoteArgs := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, requestVoteArgs, reply)

			// process RequestVote reply from peer
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// If reply term is greater than current term, become follower
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				rf.lastHeartbeat = time.Now()
				return
			}
			// If vote is granted, increment vote count
			if rf.state == Candidate && reply.Term == rf.currentTerm && reply.VoteGranted {
				rf.voteCount++

				// If vote count is greater than half of peers, become leader
				if rf.voteCount > len(rf.peers)/2 && rf.state == Candidate {
					rf.state = Leader
					rf.matchIndex = make([]int, len(rf.peers))
					rf.nextIndex = make([]int, len(rf.peers))

					nextIdx := rf.getLastLogIndex() + 1

					for i := range rf.peers {
						rf.matchIndex[i] = rf.lastIncludedIndex
						rf.nextIndex[i] = nextIdx
					}
					rf.lastHeartbeat = time.Now()
					go rf.updateLeaderCommitIndex()
					go rf.heartbeat()
				}
			}
		}(i)
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		currentTerm := rf.currentTerm
		prevLogIndex := rf.getLastLogIndex()
		prevLogTerm := rf.getLastLogTerm()
		commitIndex := rf.commitIndex

		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(server int) {
				args := &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      nil,
					LeaderCommit: commitIndex,
				}
				reply := &AppendEntriesReply{}

				ok := rf.sendAppendEntries(server, args, reply)

				if !ok {
					return
				}

				rf.mu.Lock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					rf.lastHeartbeat = time.Now()
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()
			}(i)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// Helper functions
func (rf *Raft) getRelativeLogIndex(absoluteIndex int) int {
	return absoluteIndex - rf.lastIncludedIndex - 1
}

func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].TermReceived
}

func (rf *Raft) getLogTerm(absoluteIndex int) int {
	// If index is at snapshot boundary
	if absoluteIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	// If index is older than snapshot
	if absoluteIndex < rf.lastIncludedIndex {
		return -1
	}

	relativeIndex := rf.getRelativeLogIndex(absoluteIndex)
	if relativeIndex < 0 || relativeIndex >= len(rf.log) {
		return -1
	}
	return rf.log[relativeIndex].TermReceived
}

func (rf *Raft) getAbsoluteLogEntry(absoluteIndex int) *LogEntry {
	relativeIndex := rf.getRelativeLogIndex(absoluteIndex)
	if relativeIndex < 0 || relativeIndex >= len(rf.log) {
		return nil
	}
	return &rf.log[relativeIndex]
}

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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.state = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteCount = 0
	rf.lastHeartbeat = time.Now()
	rf.electionTimeoutInterval = time.Duration(400+rand.Intn(200)) * time.Millisecond
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.applyCommittedCommands()

	if rf.state == Leader {
		go rf.heartbeat()
		go rf.updateLeaderCommitIndex()
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
