package kvsrv

import (
	"fmt"
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	maps map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, ok := kv.maps[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// perform operations
	prev, ok := kv.maps[args.Key]
	kv.maps[args.Key] = args.Value

	if ok {
		reply.Value = prev
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	prev, ok := kv.maps[args.Key]
	if ok {
		kv.maps[args.Key] = fmt.Sprintf("%s%s", prev, args.Value)
	} else {
		kv.maps[args.Key] = args.Value
	}
	reply.Value = prev
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.maps = make(map[string]string)

	return kv
}
