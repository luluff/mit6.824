package mr

import (
	"log"
	"strings"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var (
	mapLock           []int64
	reduceLock        []int64
	totalReduceNum    int64
	totalMapNum       int64
	mapCompleteNum    int64 = 0
	reduceCompleteNum int64 = 0
	allfiles          []string
	mapResultTmpl     = "mr-%d-%d"
	resultTmpl        = "mr-out-%d"
)

const (
	free = iota
	running
	finish
)

type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if getComplete(&mapCompleteNum) != totalMapNum {
		for i := 0; i < len(mapLock); i++ {
			if tryRunFromFree("map", i) {
				sendMapTask(reply, i)
				go checkTimeout("map", i)
				return nil
			}
		}
		//全都在执行
		reply.Type = NoTask
		return nil
	}
	if getComplete(&reduceCompleteNum) != totalReduceNum {
		for i := 0; i < len(reduceLock); i++ {
			if tryRunFromFree("reduce", i) {
				sendReduceTask(reply, i)
				go checkTimeout("reduce", i)
				return nil
			}
		}
		//全都在执行
		reply.Type = NoTask
		return nil
	}
	// 所有任务都已经完成
	reply.Type = PleaseExit
	return nil
}
func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	var se, tmpLock *int64
	switch args.Type {
	case "map":
		tmpLock = &mapLock[args.Index]
		se = &mapCompleteNum
	case "reduce":
		tmpLock = &reduceLock[args.Index]
		se = &reduceCompleteNum
	default:
		log.Fatal("CompleteTask")
	}
	if atomic.CompareAndSwapInt64(tmpLock, running, finish) {
		commitResult(args.FileNames)
		addComplete(se)
	}
	return nil
}
func commitResult(files []string) {
	for _, f := range files {
		if err := commitFile(f); err != nil {
			log.Fatal(err)
		}
	}
}

//把它从tmp目录移动到当前目录
func commitFile(file string) error {
	name := strings.TrimPrefix(file, "/tmp/")
	index := strings.Index(name, "-suffix")
	return os.Rename(file, name[0:index])
}
func checkTimeout(s string, index int) {
	time.Sleep(10 * time.Second)
	var tmpLock *int64
	switch s {
	case "map":
		tmpLock = &mapLock[index]
	case "reduce":
		tmpLock = &reduceLock[index]
	default:
		log.Fatal("checkTimeout")
	}
	atomic.CompareAndSwapInt64(tmpLock, running, free)
}

func sendReduceTask(reply *GetTaskReply, reduceIndex int) {
	reply.Type = reduceType
	reply.Index = reduceIndex
	reply.MapNum = totalMapNum
	return
}

func sendMapTask(reply *GetTaskReply, index int) {
	fileName := allfiles[index]
	reply.Type = mapType
	reply.Index = index
	reply.FileName = fileName
	reply.Nreduce = totalReduceNum
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return getComplete(&reduceCompleteNum) == totalReduceNum

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	//TODO check files len
	mapLock = make([]int64, len(files))
	reduceLock = make([]int64, nReduce)
	totalMapNum = int64(len(files))
	totalReduceNum = int64(nReduce)
	// Your code here.
	allfiles = files
	c.server()
	return &c
}

func tryRunFromFree(tp string, index int) bool {
	var tmpLock *int64
	switch tp {
	case "map":
		tmpLock = &mapLock[index]
	case "reduce":
		tmpLock = &reduceLock[index]
	default:
		log.Fatal("try lock")
	}
	return atomic.CompareAndSwapInt64(tmpLock, free, running)
}

func addComplete(se *int64) {
	atomic.AddInt64(se, 1)
}
func getComplete(se *int64) int64 {
	return atomic.LoadInt64(se)
}
