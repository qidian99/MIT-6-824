package mr

import (
	"strings"
	"bufio"
	"time"
	"sort"
	"io/ioutil"
	"os"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


const (
	CallRegister = "Master.RegisterWorker"
	CallPingPong = "Master.PingPong"
	CallGetTask  = "Master.GetTaskWorker"
	CallReport   = "Master.ReportResult"
)

type WorkerObj struct {
	workerId uint64
	nMap uint64
	nReduce int
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
} 

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	worker := WorkerObj{}
	worker.workerId, worker.nReduce = RegisterWorker()
	worker.mapf, worker.reducef = mapf, reducef
	// fmt.Printf("%v %v %v", MapTask, ReduceTask, ExitTask)

	for {
		task := worker.getTask()

		res, err := worker.execTask(task)

		fmt.Println("Worker: task.Type:", task.Type)
		fmt.Printf("Worker.ExecTask(): Task ID: %d\n", res.TaskID)

		if err == nil {
			worker.reportTask(res)
		}

		time.Sleep(time.Second)
	}
}

func (w *WorkerObj) execTask(task *Task) (ReportTaskArgs, error) {
	res := ReportTaskArgs{}
	// get file from task
	switch (task.Type) {
		case MapTask:
			filename := task.Conf.File[0]
			mapId := task.Conf.TaskId
			res = w.execMap(filename, mapId)
			res.WorkerID = w.workerId
			res.TaskType = MapTask
			res.TaskID = mapId
			break;

		case ReduceTask:
			res = w.execReduce(task)
			res.WorkerID = w.workerId
			res.TaskType = ReduceTask
			res.TaskID = task.Conf.TaskId
			break;
		case EmptyTask:
			return res, fmt.Errorf("Empty task")
		case ExitTask:
			os.Exit(0)
		default:
			break;
	}

	return res, nil
}

func (w *WorkerObj) execMap(filename string, mapId uint64) (ReportTaskArgs) {
	file, err := os.Open(filename);
	defer file.Close()

	res := ReportTaskArgs{}
	res.TmpFiles = make([]string, 0)
	res.TargetFiles = make([]string, 0)

	if err != nil {
		return ReportTaskArgs{Status: Error}
	}

	lines, err := ioutil.ReadAll(file)

	if err != nil {
		return ReportTaskArgs{Status: Error}
	}

	mapPartition := make(map[int][]KeyValue, w.nReduce)

	// generate map partitions
	fmtStr := "mr-worker-%v-%v.out" // mapId-reduceId
	for i := 0; i < w.nReduce; i++ {
		mapPartition[i] = []KeyValue{}
	}

	kvs := w.mapf(filename, string(lines))
	fmt.Printf("Retrieved %d KV records\n", len(kvs))

	for _, kv := range(kvs) {
		bucket := ihash(kv.Key) % w.nReduce
		mapPartition[bucket] = append(mapPartition[bucket], kv) 
	}

	for i := 0; i < w.nReduce; i++ {
		// sort
		sort.Sort(ByKey(mapPartition[i]))
		outFilename := fmt.Sprintf(fmtStr, mapId, i)
		tmpFilename := outFilename + ".tmp"

		// write to file
		tmpFile, err := os.Create(tmpFilename)
		defer tmpFile.Close()
		if err != nil {
			return ReportTaskArgs{Status: Error}
		}
		for _, kv := range(mapPartition[i]) {
			fmt.Fprintf(tmpFile, "%s %s\n", kv.Key, kv.Value)
		}

		res.TmpFiles = append(res.TmpFiles, tmpFilename)
		res.TargetFiles = append(res.TargetFiles, outFilename)
		fmt.Printf("Writing %d records to file: %s\n", len(mapPartition[i]), outFilename)

	}

	return res


	// for _, line := range lines {
	// 	words := strings.Split(string(line), " ")
	// }

}

func (w *WorkerObj) execReduce(task *Task) ReportTaskArgs {
	res := ReportTaskArgs{}
	tmpFmt := "mr-out-%d.out.tmp"
	targetFmt := "mr-out-%d.out"
	tmpFilename := fmt.Sprintf(tmpFmt, task.Conf.TaskId)
	targetFilename := fmt.Sprintf(targetFmt, task.Conf.TaskId)
	tmpFile, _ := os.Create(tmpFilename)
	defer tmpFile.Close()

	keyMap := make(map[string][]string, 0)
	reduceRes := make(map[string]string, 0)

	// read files
	for _, filename := range(task.Conf.File) {
	
		file, _ := os.OpenFile(filename, 0, 0)
		defer file.Close()
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			// fmt.Printf("Scanning: %s\n", filename)
			line := scanner.Text()
			key, val := strings.Split(line, " ")[0], strings.Split(line, " ")[1]
			
			if (keyMap[key] == nil) {
				keyMap[key] = []string{}
			}

			keyMap[key] = append(keyMap[key], val)

		}

		for key, vals := range(keyMap) {
	 		reduceRes[key] = w.reducef(key, vals)
		}
	}

	for k, v := range(reduceRes) {
		fmt.Fprintf(tmpFile, "%s %s\n", k, v)
	}

	res.TargetFiles = append(res.TargetFiles, targetFilename)
	res.TmpFiles = append(res.TmpFiles, tmpFilename)
	// reduce
	return res
}

func (w *WorkerObj) getTask() *Task {

	args, res := GetWorkerTaskArgs{}, GetWorkerTaskReply{}
	args.WorkerID = w.workerId
	call(CallGetTask, &args, &res)

	return res.Task;

}

func RegisterWorker() (uint64, int) {

	args, res := RegisterWorkerArgs{}, RegisterWorkerReply{}

	call(CallRegister, &args, &res)

	fmt.Println("res.nReduce", res.RC)
	return res.WorkerID, res.RC

}

func (w *WorkerObj) reportTask(args ReportTaskArgs) {
	res := ReportTaskReply{}
	call(CallReport, &args, &res)

	return
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
