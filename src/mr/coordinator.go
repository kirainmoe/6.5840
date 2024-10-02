package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// State of a map/reduce task
const (
	TASK_PENDING     = 1
	TASK_IN_PROGRESS = 2
	TASK_DONE        = 3
)

const NO_WORKER = -1

const MAX_TIMEOUT_MS = 6_000 // 6s

type TaskMeta struct {
	Type           int
	State          int
	Index          int
	Inputs         []string
	Outputs        []string
	AssignedWorker int
	AssignedTime   int64
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	nReduce  int
	nWorkers int

	// task state
	mapTasks    []TaskMeta
	reduceTasks []TaskMeta

	nMapTaskDone    int
	nReduceTaskDone int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(req *RegisterWorkerRequest, res *RegisterWorkerResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nWorkers++
	res.WorkerId = c.nWorkers
	res.NReduce = c.nReduce

	c.log("new worker registered workerId=%v", c.nWorkers)

	return nil
}

func (c *Coordinator) GetTask(req *GetTaskRequest, res *GetTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.log("receive task assign request from workerId=%v", req.WorkerId)

	// if all tasks are done, return state GET_TASK_DONE
	if c.isAllTasksDone() {
		c.log("all tasks are finished")
		res.Result = GET_TASK_DONE
		return nil
	}

	// if in map pharse, assign a map pharse
	mapDone := c.nMapTaskDone == len(c.mapTasks)
	if !mapDone {
		task, _ := c.pickAnUnassignedTask(&c.mapTasks)

		// there are no pending map tasks
		if task == nil {
			c.log("in map pharse but no pending map task available, nMapDone=%v", c.nMapTaskDone)

			res.Result = GET_TASK_PENDING
			return nil
		}

		// update response structure
		res.TaskType = TASK_TYPE_MAP
		res.TaskIndex = task.Index
		res.Result = GET_TASK_SUCCESS
		res.Inputs = task.Inputs

		// update task state
		c.updateTaskState(task, req.WorkerId, TASK_IN_PROGRESS)

		c.log("assigned map task workerId=%v taskId=%v input=%v", req.WorkerId, res.TaskIndex, res.Inputs)

		return nil
	}

	// in reduce pharse
	task, _ := c.pickAnUnassignedTask(&c.reduceTasks)
	if task == nil {
		c.log("in reduce pharse but no pending reduce task available")
		res.Result = GET_TASK_PENDING
		return nil
	}

	res.TaskType = TASK_TYPE_REDUCE
	res.TaskIndex = task.Index
	res.Result = GET_TASK_SUCCESS
	res.Inputs = task.Inputs

	c.updateTaskState(task, req.WorkerId, TASK_IN_PROGRESS)

	c.log("assigned reduce task workerId=%v taskId=%v input=%v", req.WorkerId, res.TaskIndex, res.Inputs)

	return nil
}

func (c *Coordinator) ReportTaskResult(req *ReportTaskResultRequest, res *ReportTaskResultResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.log("get task result from workerId=%v taskType=%v taskId=%v", req.WorkerId, req.TaskType, req.TaskIndex)

	var task *TaskMeta
	isMapTask := req.TaskType == TASK_TYPE_MAP
	if isMapTask {
		task = &c.mapTasks[req.TaskIndex]
	} else {
		task = &c.reduceTasks[req.TaskIndex]
	}

	// may recover from network failure
	if task.State != TASK_IN_PROGRESS || task.AssignedWorker != req.WorkerId {
		c.log("found mismatched task state or assigned worker state=%v assigned=%v current=%v",
			task.State, task.AssignedWorker, req.WorkerId)
	}

	c.updateTaskState(task, req.WorkerId, TASK_DONE)
	task.Outputs = req.Outputs

	if isMapTask {
		c.nMapTaskDone++
		c.log("complete map tasks num=%v", c.nMapTaskDone)
		if c.nMapTaskDone == len(c.mapTasks) {
			c.log("all map tasks are completed, creating reduce tasks")
			c.createReduceTasks()
		}
	} else {
		c.nReduceTaskDone++
		c.log("complete map tasks num=%v", c.nReduceTaskDone)
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.isAllTasksDone()

	return ret
}

// #region internal

func (c *Coordinator) log(format string, args ...any) {
	if !ENABLE_DEBUG_LOG {
		return
	}

	pc, _, _, _ := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	items := strings.Split(details.Name(), ".")
	methodName := items[len(items)-1]

	log.Printf(fmt.Sprintf("[master] method=%s message=%s", methodName, format), args...)
}

func (c *Coordinator) isAllTasksDone() bool {
	return c.nMapTaskDone == len(c.mapTasks) && c.nReduceTaskDone == len(c.reduceTasks)
}

func (c *Coordinator) createMapTasks(files []string) {
	c.mapTasks = make([]TaskMeta, len(files))

	for index, file := range files {
		c.mapTasks[index] = TaskMeta{
			Type:           TASK_TYPE_MAP,
			State:          TASK_PENDING,
			AssignedWorker: NO_WORKER,
			AssignedTime:   0,
			Index:          index,
			Inputs:         []string{file},
		}
	}

	c.nMapTaskDone = 0
}

func (c *Coordinator) createReduceTasks() {
	c.reduceTasks = make([]TaskMeta, c.nReduce)

	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i] = TaskMeta{
			Type:           TASK_TYPE_REDUCE,
			State:          TASK_PENDING,
			AssignedWorker: NO_WORKER,
			AssignedTime:   0,
			Index:          i,
			Inputs:         make([]string, 0),
		}

		for _, mapTask := range c.mapTasks {
			c.reduceTasks[i].Inputs = append(c.reduceTasks[i].Inputs, mapTask.Outputs[i])
		}
	}

	c.nReduceTaskDone = 0
}

// update the assigned info and recorded state of task depends on next state
func (c *Coordinator) updateTaskState(task *TaskMeta, workerIndex int, nextState int) {
	c.log("update task state id=%v type=%v nextState=%v worker=%d pointer=%p", task.Index, task.Type, nextState, workerIndex, &task)
	task.State = nextState

	if nextState == TASK_PENDING {
		task.AssignedWorker = NO_WORKER
		task.AssignedTime = 0
	}

	if nextState == TASK_IN_PROGRESS {
		task.AssignedWorker = workerIndex
		task.AssignedTime = time.Now().UnixMilli()
	}
}

func (c *Coordinator) pickAnUnassignedTask(tasks *[]TaskMeta) (task *TaskMeta, index int) {
	for id := range *tasks {
		item := &((*tasks)[id])
		if item.State == TASK_PENDING {
			task = item
			index = id
			c.log("assigned task type=%v id=%v pointer=%p", item.Type, item.Index, &item)
			return
		}
	}

	// all tasks are finished or assigned
	c.log("all tasks are finished or assigned")
	task = nil
	index = -1
	return
}

// check is task timeout and reset it to pending state
func (c *Coordinator) daemon() {
	c.mu.Lock()

	if c.isAllTasksDone() {
		c.mu.Unlock()
		return
	}

	var target *[]TaskMeta

	if c.nMapTaskDone < len(c.mapTasks) {
		target = &c.mapTasks
	} else if c.nReduceTaskDone < len(c.reduceTasks) {
		target = &c.reduceTasks
	}

	for i := range *target {
		task := &(*target)[i]
		if task.State != TASK_IN_PROGRESS {
			continue
		}

		if isTimeout(task) {
			c.log("task is timeout type=%v id=%v worker=%v assignedIn=%v",
				task.Type, i, task.AssignedWorker, task.AssignedTime)

			c.updateTaskState(task, NO_WORKER, TASK_PENDING)
		}
	}

	c.mu.Unlock()
	time.Sleep(time.Second * 1)

	go c.daemon()
}

func isTimeout(task *TaskMeta) bool {
	current := time.Now().UnixMilli()
	return current-task.AssignedTime > MAX_TIMEOUT_MS
}

// #endregion internal

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.createMapTasks(files)

	go c.daemon()

	c.server()
	return &c
}
