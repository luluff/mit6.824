package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

var (
	workerMapTmpl    = "mr-%d-%d-suffix*"
	workerReduceTmpl = "mr-out-%d-suffix*"
	mapFunc          func(string, string) []KeyValue
	reduceFunc       func(string, []string) string
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	mapFunc = mapf
	reduceFunc = reducef
	for {
		reply := getTaskFromMaster()
		switch reply.Type {
		case PleaseExit:
			continue
		case NoTask:
			continue
		case mapType:
			doMap(reply.FileName, reply.Index, reply.Nreduce)
		case reduceType:
			doReduce(reply.Index, reply.MapNum)
		default:
			log.Fatal(reply.Type)
		}
		time.Sleep(time.Second)
	}

}

func doReduce(index int, mapNum int64) {
	var (
		i   int64
		kva []KeyValue
	)
	//把reduce编号的所有map结果整合成kva
	for i = 0; i < mapNum; i++ {
		fileName := fmt.Sprintf(mapResultTmpl, i, index)
		ks := getFileKV(fileName)
		kva = append(kva, ks...)
	}
	sort.Sort(ByKey(kva))
	file, err := ioutil.TempFile("/tmp/", getReduceFileName(int64(index)))
	if err != nil {
		log.Fatal(err)
	}
	i = 0
	length := int64(len(kva))
	for i < length {
		j := i + 1
		for j < length && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reduceFunc(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		i = j
	}
	file.Close()
	completeTask(index, "reduce", []string{file.Name()})
}

func doMap(fileName string, mapIndex int, nReduce int64) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}
	file.Close()
	//一个map任务的所有输出的文件名字
	var result []string
	fileMap := map[string]*os.File{}
	kva := mapFunc(fileName, string(content))
	sort.Sort(ByKey(kva))
	for _, kv := range kva {
		reduceNo := int64(ihash(kv.Key)) % nReduce
		writeToFile(fileMap, mapIndex, reduceNo, kv)
	}
	for _, file := range fileMap {
		result = append(result, file.Name())
		file.Close()
	}
	completeTask(mapIndex, "map", result)

}

func writeToFile(fileMap map[string]*os.File, mapIndex int, reduceNo int64, kv KeyValue) {
	var (
		file  *os.File
		exist bool
		err   error
	)
	fileName := getMapFileName(mapIndex, reduceNo)
	if file, exist = fileMap[fileName]; !exist {
		file, err = ioutil.TempFile("/tmp/", fileName)
		if err != nil {
			log.Fatal(err)
		}
		fileMap[fileName] = file
	}
	enc := json.NewEncoder(file)
	err = enc.Encode(kv)
	if err != nil {
		log.Fatal(err)
	}

}

func getMapFileName(mapIndex int, reduceNo int64) string {
	return fmt.Sprintf(workerMapTmpl, mapIndex, reduceNo)
}
func getReduceFileName(reduceNo int64) string {
	return fmt.Sprintf(workerReduceTmpl, reduceNo)
}
func getFileKV(fileName string) []KeyValue {
	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	kva := []KeyValue{}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	file.Close()
	return kva
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func getTaskFromMaster() *GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return &reply
}
func completeTask(taskIndex int, taskType string, fileNames []string) {
	args := CompleteTaskArgs{}
	args.FileNames = fileNames
	args.Index = taskIndex
	args.Type = taskType
	reply := CompleteTaskReply{}
	call("Coordinator.CompleteTask", &args, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatalf("call get err:%s", err.Error())
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Fatalf("call get err:%s", err.Error())
	}

}
