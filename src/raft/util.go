package raft

import (
	"encoding/json"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//func PrintJson(a []*Raft) {
//	for _, r := range a {
//		_, leader := r.GetState()
//		fmt.Printf("peer [%d] ,[%v] \n", r.me, leader)
//		fmt.Printf("logs : %s\n", ToJson(r.logs))
//		fmt.Printf("lastApplied [%d]\n", r.lastApplied)
//		fmt.Printf("commitIndex [%d]\n", r.commitIndex)
//		fmt.Printf("endLogIndex [%d]\n", r.endLogIndex)
//		if leader {
//			fmt.Printf("allNextIndex [%s]\n", ToJson(r.nextIndex))
//			fmt.Printf("allMatchIndex [%s]\n", ToJson(r.matchIndex))
//		}
//	}
//}
func ToJson(v interface{}) string {
	bs, _ := json.Marshal(v)
	return string(bs)
}
