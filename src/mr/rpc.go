package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// constants
const ENABLE_DEBUG_LOG = false

// task types
const (
	TASK_TYPE_MAP    = 1
	TASK_TYPE_REDUCE = 2
)

const (
	// Get a new task
	GET_TASK_SUCCESS = 1
	// Currently there are no available task to be assigned, should wait
	GET_TASK_PENDING = 2
	// All tasks are done
	GET_TASK_DONE = 3
)

// example to show how to declare the arguments
// and reply for an RPC.

// RegisterWorker() RPC
type RegisterWorkerRequest struct{}
type RegisterWorkerResponse struct {
	WorkerId int
	NReduce  int
}

// GetTask() RPC requests a task from master (coordinator)
type GetTaskRequest struct {
	WorkerId int
}
type GetTaskResponse struct {
	// GET_TASK_*
	Result int
	// TASK_TYPE_*
	TaskType int
	// Task id or index
	TaskIndex int
	// input files
	Inputs []string
}

// ReportTaskResult() RPC reports the result of a task (either map or reduce) to master
type ReportTaskResultRequest struct {
	WorkerId  int
	TaskType  int
	TaskIndex int
	Outputs   []string
}
type ReportTaskResultResponse struct{}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func Log(role string, format string, args ...any) {
	if !ENABLE_DEBUG_LOG {
		return
	}

	pc, _, _, _ := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	items := strings.Split(details.Name(), ".")
	methodName := items[len(items)-1]

	log.Printf(fmt.Sprintf("[%s] method=%s message=%s", role, methodName, format), args...)
}
