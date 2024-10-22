package raft

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"6.5840/labgob"
)

// #region Log

const (
	LEVEL_DEBUG   = 0
	LEVEL_SUCCESS = 1
	LEVEL_INFO    = 2
	LEVEL_WARN    = 3
	LEVEL_ERROR   = 4
	LEVEL_FATAL   = 5
	LEVEL_DISABLE = 6
)

type DevLog map[string]interface{}

func GetCaller(skip int) string {
	pc, _, _, _ := runtime.Caller(skip)
	details := runtime.FuncForPC(pc)
	items := strings.Split(details.Name(), ".")
	methodName := items[len(items)-1]

	return methodName
}

func Log(level int, payload DevLog) {
	LOG_LEVEL := os.Getenv("LOG_LEVEL")
	DISABLE_COLORFUL_OUTPUT := os.Getenv("COLOR") == "false"

	levelMap := map[string]int{
		"debug":   LEVEL_DEBUG,
		"success": LEVEL_SUCCESS,
		"info":    LEVEL_INFO,
		"warn":    LEVEL_WARN,
		"error":   LEVEL_ERROR,
		"fatal":   LEVEL_FATAL,
		"disable": LEVEL_DISABLE,
		"":        LEVEL_FATAL,
	}

	currentLevel := levelMap[strings.ToLower(LOG_LEVEL)]

	if level < currentLevel {
		return
	}

	var levelStr, colorStr string
	switch level {
	case LEVEL_DEBUG:
		levelStr, colorStr = "DEBUG", "90"
	case LEVEL_SUCCESS:
		levelStr, colorStr = "SUCC", "32"
	case LEVEL_INFO:
		levelStr, colorStr = "INFO", "96"
	case LEVEL_WARN:
		levelStr, colorStr = "WARN", "93"
	case LEVEL_ERROR:
		levelStr, colorStr = "ERROR", "91"
	case LEVEL_FATAL:
		levelStr, colorStr = "FATAL", "31"
	}

	logStr := ""

	if DISABLE_COLORFUL_OUTPUT {
		logStr = fmt.Sprintf("[%s] ", levelStr)
	} else {
		logStr = fmt.Sprintf("\033[%sm[%v]\033[0m ", colorStr, levelStr)
	}

	printKV := func(key string, value interface{}) string {
		if DISABLE_COLORFUL_OUTPUT {
			return fmt.Sprintf("%s=%+v ", key, value)
		}
		return fmt.Sprintf("\033[%sm%s=%+v\033[0m ", colorStr, key, value)
	}

	orders := []string{"_id", "_role", "_term", "_logID", "_method", "_cost", "message", "incoming", "current"}

	for _, pKey := range orders {
		if _, ok := payload[pKey]; ok {
			logStr += printKV(pKey, payload[pKey])
			delete(payload, pKey)
		}
	}

	for key, value := range payload {
		logStr += printKV(key, value)
	}

	log.Println(logStr)
}

// #endregion

// #region Utils
func RunInTimeLimit[T any](timeMs int64, onRun func() T) (bool, T) {
	payload := make(chan T)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(timeMs))
	defer cancel()

	var empty T

	go func() {
		payload <- onRun()
	}()

	select {
	case v := <-payload:
		return true, v

	case <-ctx.Done():
		return false, empty
	}
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
	rf.info(context.Background(), DevLog{
		"message": fmt.Sprintf("sending %v to all servers", rpcName),
		"rpcName": rpcName,
		"peers":   len(rf.peers),
	})

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

			rf.debug(context.Background(), DevLog{
				"message": fmt.Sprintf("sending request rpc=%v", rpcName),
				"peer":    index,
			})

			done, payload := RunInTimeLimit(RPC_TIMEOUT_MS, func() *T {
				return onRequest(index)
			})

			if !done {
				rf.error(context.Background(), DevLog{
					"message":  fmt.Sprintf("%v RPC call timeout", rpcName),
					"fromPeer": rf.me,
					"toPeer":   index,
				})
			}

			mu.Lock()
			response[index].Ok = done
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

func TryDecode[T any](ctx context.Context, rf *Raft, decoder *labgob.LabDecoder, name string, target *T) {
	if err := decoder.Decode(target); err != nil {
		rf.error(ctx, DevLog{
			"message": "decode field from persist error",
			"field":   name,
			"error":   err,
		})
	}
}

type ContextKey string

var (
	ContextKeyLogID  = ContextKey("LogID")
	ContextKeySelfID = ContextKey("SelfID")
)

// #endregion
