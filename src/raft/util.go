package raft

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = true

const (
	LEVEL_DEBUG   = 0
	LEVEL_INFO    = 1
	LEVEL_WARN    = 2
	LEVEL_ERROR   = 3
	LEVEL_FATAL   = 4
	LEVEL_DISABLE = 5
)

const LOG_LEVEL = LEVEL_DISABLE

func GetCaller() string {
	pc, _, _, _ := runtime.Caller(3)
	details := runtime.FuncForPC(pc)
	items := strings.Split(details.Name(), ".")
	methodName := items[len(items)-1]

	return methodName
}

func Log(level int, role string, id int, term int64, format string, a ...interface{}) {
	if level < LOG_LEVEL {
		return
	}

	levelStr := ""
	switch level {
	case LEVEL_DEBUG:
		levelStr = "DEBUG"
	case LEVEL_INFO:
		levelStr = "INFO"
	case LEVEL_WARN:
		levelStr = "WARN"
	case LEVEL_ERROR:
		levelStr = "ERROR"
	case LEVEL_FATAL:
		levelStr = "FATAL"
	}

	log.Printf(fmt.Sprintf("[%s] [%s] id=%v term=%d method=%s message=%s", levelStr, role, id, term, GetCaller(), format), a...)
}

func randRange(min, max int64) int64 {
	return rand.Int63n(max-min) + min
}

func RunInTimeLimit[T any](timeMs int64, onRun func() T) (bool, T) {
	flag := make(chan bool)
	race := int32(0)

	var payload T

	go func() {
		ret := onRun()
		if atomic.LoadInt32(&race) == 0 {
			payload = ret
			flag <- true
		}
	}()

	go func() {
		time.Sleep(time.Duration(timeMs) * time.Millisecond)
		if atomic.LoadInt32(&race) == 0 {
			flag <- false
		}
	}()

	res := <-flag
	atomic.StoreInt32(&race, 1)

	return res, payload
}

type GenericRPCResponse struct {
	Ok     bool
	Result interface{}
}

func SendRPCToAllPeersConcurrently[T any](
	rf *Raft,
	rpcName string,
	onRequest func(peerIndex int) *T,
) []GenericRPCResponse {
	rf.info("sending %v to all servers rpc=%v peersCount=%v", rpcName, len(rf.peers))

	response := make([]GenericRPCResponse, len(rf.peers))

	var mu sync.Mutex
	var wg sync.WaitGroup

	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			rf.debug("sending request rpc=%v peer=%v", rpcName, index)

			ok, payload := RunInTimeLimit(RPC_TIMEOUT_MS, func() *T {
				return onRequest(index)
			})

			if !ok {
				rf.error("%v call failed or timeout fromPeer=%v toPeer=%v", rpcName, rf.me, index)
			}

			mu.Lock()
			response[index].Ok = ok
			response[index].Result = payload
			mu.Unlock()
		}()
	}

	wg.Wait()

	return response
}

func ForEachPeers(rf *Raft, cb func(index int)) {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		cb(index)
	}
}
