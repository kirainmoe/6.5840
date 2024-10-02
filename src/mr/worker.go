package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func loadfile(filepaths []string) string {
	content := ""
	for _, filepath := range filepaths {
		file, err := os.ReadFile(filepath)
		if err != nil {
			Log("system", "loadfile failed path=%v err=%v", filepath, err)
			continue
		}
		content += string(file)
	}
	return content
}

// key -> []value
func loadIntermediateFiles(filepaths []string) map[string][]string {
	key2Values := make(map[string][]string, 0)

	for _, filepath := range filepaths {
		text, err := os.ReadFile(filepath)
		if err != nil {
			Log("system", "loadIntermediateFiles failed path=%v err=%v", filepath, err)
			continue
		}

		var kvs []KeyValue
		err = json.Unmarshal(text, &kvs)
		if err != nil {
			Log("system", "parse intermediate JSON kv failed")
			continue
		}

		for _, kv := range kvs {
			_, ok := key2Values[kv.Key]
			if !ok {
				key2Values[kv.Key] = []string{kv.Value}
				continue
			}
			key2Values[kv.Key] = append(key2Values[kv.Key], kv.Value)
		}
	}

	return key2Values
}

func writeIntermediateFiles(workerId int, taskId int, brackets int, content [][]KeyValue) []string {
	outputs := make([]string, brackets)
	for i := 0; i < brackets; i++ {
		outputs[i] = fmt.Sprintf("mr-%d-%d-%d", workerId, taskId, i)
		text, err := json.Marshal(content[i])
		if err != nil {
			Log("system", "JSON encode failure")
		}

		f, _ := os.Create(outputs[i])
		f.Write(text)
	}
	return outputs
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	registerReq := RegisterWorkerRequest{}
	registerRes := RegisterWorkerResponse{}
	call("Coordinator.RegisterWorker", &registerReq, &registerRes)

	workerId := registerRes.WorkerId
	nReduce := registerRes.NReduce
	role := fmt.Sprintf("client:%d", registerRes.WorkerId)
	Log(role, "worker registered id=%v nReduce=%v", registerRes.WorkerId, registerRes.NReduce)

	for {
		getTaskReq := GetTaskRequest{
			WorkerId: workerId,
		}
		getTaskRes := GetTaskResponse{}
		ok := call("Coordinator.GetTask", &getTaskReq, &getTaskRes)
		if !ok {
			continue
		}

		if getTaskRes.Result == GET_TASK_DONE {
			Log(role, "all tasks done, client exit")
			break
		}

		if getTaskRes.Result == GET_TASK_PENDING {
			Log(role, "no available task, sleep for 500ms")
			time.Sleep(time.Millisecond * 200)
			continue
		}

		// map worker
		if getTaskRes.TaskType == TASK_TYPE_MAP {
			Log(role, "get map task index=%v input=%v", getTaskRes.TaskIndex, getTaskRes.Inputs)

			// run map() to separate it into (key, values)
			input := loadfile(getTaskRes.Inputs)
			// Log(role, "map input: %v", input)
			keyValues := mapf(getTaskRes.Inputs[0], input)
			// Log(role, "map result: %v", keyValues)

			// use hash function to break result into nReduce brackets
			brackets := make([][]KeyValue, nReduce)
			for i := 0; i < nReduce; i++ {
				brackets[i] = make([]KeyValue, 0)
			}
			for _, kv := range keyValues {
				index := ihash(kv.Key) % nReduce
				brackets[index] = append(brackets[index], kv)
			}
			outputs := writeIntermediateFiles(workerId, getTaskRes.TaskIndex, nReduce, brackets)

			// report task result
			reportReq := ReportTaskResultRequest{
				WorkerId:  workerId,
				TaskType:  getTaskRes.TaskType,
				TaskIndex: getTaskRes.TaskIndex,
				Outputs:   outputs,
			}
			reportRes := ReportTaskResultResponse{}

			Log(role, "map task done index=%v input=%v outputs=%v", getTaskRes.TaskIndex, getTaskRes.Inputs, outputs)

			call("Coordinator.ReportTaskResult", &reportReq, &reportRes)
		}

		// reduce worker
		if getTaskRes.TaskType == TASK_TYPE_REDUCE {
			Log(role, "get reduce task index=%v input=%v", getTaskRes.TaskIndex, getTaskRes.Inputs)

			key2Values := loadIntermediateFiles(getTaskRes.Inputs)
			output := ""
			for key, values := range key2Values {
				output += fmt.Sprintf("%v %v\n", key, reducef(key, values))
			}

			// write output
			filepath := fmt.Sprintf("mr-out-%d", getTaskRes.TaskIndex)
			f, _ := os.Create(filepath)
			f.WriteString(output)

			// report task result
			reportReq := ReportTaskResultRequest{
				WorkerId:  workerId,
				TaskType:  getTaskRes.TaskType,
				TaskIndex: getTaskRes.TaskIndex,
				Outputs:   []string{filepath},
			}
			reportRes := ReportTaskResultResponse{}

			Log(role, "reduce task done index=%v input=%v outputs=%v", getTaskRes.TaskIndex, getTaskRes.Inputs, filepath)

			call("Coordinator.ReportTaskResult", &reportReq, &reportRes)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
