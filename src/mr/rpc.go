package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type GetTaskArgs struct {
}
type CompleteTaskArgs struct {
	Type      string
	Index     int
	FileNames []string
}

type CompleteTaskReply struct {
}

const (
	mapType    = 1
	reduceType = 2
	NoTask     = 3
	PleaseExit = 4
)

//1=map   2=reduce   3=no task free
type GetTaskReply struct {
	Type     int
	FileName string
	Content  string
	Index    int
	Nreduce  int64
	MapNum   int64
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
