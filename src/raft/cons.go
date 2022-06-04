package raft

import "time"

const (
	electionTimeout               = 100 * time.Millisecond
	retryInstallInterval          = 5 //second
	electionRandomGap             = 100 * time.Millisecond
	rpcTimeout                    = 50 * time.Millisecond
	appendRpcTimeoutRetryInterval = 50 * time.Millisecond
	reVoteRandomTime              = 20 * time.Millisecond
	checkMajorVoteInterVal        = 30 * time.Millisecond
	leaderInterval                = 600 * time.Millisecond
	fuckApplyInterval             = 50 * time.Millisecond
	roleFollower                  = 0
	roleCandidate                 = 1
	roleLeader                    = 2
	NoReply                       = 1
	SeeLeader                     = 2
	RpcSuccess                    = 0
	MajorVote                     = 6
	NoMajorVote                   = 7
)
