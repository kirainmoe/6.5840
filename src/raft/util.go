package raft

import (
	"fmt"
	"log"
	"math"
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
	LEVEL_SUCCESS = 1
	LEVEL_INFO    = 2
	LEVEL_WARN    = 3
	LEVEL_ERROR   = 4
	LEVEL_FATAL   = 5
	LEVEL_DISABLE = 6
)

const LOG_LEVEL = LEVEL_DISABLE
const DISABLE_COLORFUL_OUTPUT = false

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
	colorStr := ""
	switch level {
	case LEVEL_DEBUG:
		levelStr = "DEBUG"
		colorStr = "90"
	case LEVEL_SUCCESS:
		levelStr = "SUCC"
		colorStr = "32"
	case LEVEL_INFO:
		levelStr = "INFO"
		colorStr = "96"
	case LEVEL_WARN:
		levelStr = "WARN"
		colorStr = "93"
	case LEVEL_ERROR:
		levelStr = "ERROR"
		colorStr = "91"
	case LEVEL_FATAL:
		levelStr = "FATAL"
		colorStr = "31"
	}

	formatStr := fmt.Sprintf("[%s] [id=%v term=%d role=%s] method=%s message=%s",
		levelStr, id, term, role, GetCaller(), format)

	if !DISABLE_COLORFUL_OUTPUT {
		formatStr = fmt.Sprintf("\033[%sm%v\033[0m", colorStr, formatStr)
	}

	log.Printf(formatStr, a...)
}

func randRange(min, max int64) int64 {
	return rand.Int63n(max-min) + min
}

func RunInTimeLimit[T any](timeMs int64, onRun func() T) (bool, T) {
	flag := make(chan bool)
	race := int32(0)

	var payload T
	var mu sync.Mutex

	go func() {
		ret := onRun()
		if atomic.LoadInt32(&race) == 0 {
			mu.Lock()
			payload = ret
			mu.Unlock()
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

	mu.Lock()
	defer mu.Unlock()
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
	rf.info("sending %v to all servers rpc=%v peersCount=%v", rpcName, rpcName, len(rf.peers))

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

func GetMajority(value int) int64 {
	return int64(math.Ceil(float64(value) / 2))
}

func LockAndRun[T any](rf *Raft, cb func() T) T {
	rf.mu.Lock()
	result := cb()
	rf.mu.Unlock()
	return result
}
