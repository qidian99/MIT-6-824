package mr

import (
	"sync"
	"fmt"
	"sync/atomic"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"time"
)

func inInt64Slice(slice *[]uint64, val uint64) bool {
	for _, num := range *slice {
		if num == val {
			return true
		}
	}
	return false
}

var dispatcher *Dispatcher
type Master struct {
	// Your definitions here.
	
	taskPhase    TaskPhase
	nextWorkerId uint64
	TaskPool     *TaskPool
	mapTasks     []Task
	reduceTasks  []Task
	nReduce      int
	nMap         int
	status       []bool
	mutex        sync.Mutex
}


type TaskPool struct {
	Pool chan * Task 
}

// 任务
type Task struct {
	Status          TaskStatus 
	Type            TaskType
	Conf            *TaskConf
	FailedWorkers   []uint64
}

type TaskConf struct {
	File []string
	TaskId uint64
}


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.d 
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	for {

		nextWorkerId := m.nextWorkerId;
	
		if atomic.CompareAndSwapUint64(&m.nextWorkerId, nextWorkerId, nextWorkerId + 1) {
			reply.WorkerID = nextWorkerId;
			reply.RC = m.nReduce
			return nil;
		}

		time.Sleep(10 * time.Millisecond)
	}


}

func (m *Master) PingPong(args *ExampleArgs, reply *ExampleReply) error {
	return nil;
}
func (m *Master) GetTaskWorker(args *GetWorkerTaskArgs, reply *GetWorkerTaskReply) error {

	timeout := time.After(1 * time.Second)
	
	select {
		case task, ok := <- m.TaskPool.Pool:
			if !ok {
				task := Task{Type: EmptyTask}
				reply.Task = &task
				return nil
			}
			task.Status = InProgress
			reply.Task = task
			go m.exceedTimeLimt(task, args.WorkerID)
			return nil
		case <-timeout:
			task := Task{}
			if (m.taskPhase == ExitPhase) {
				task.Type = ExitTask
			} else {
				task.Type = EmptyTask
			}
			reply.Task = &task
			return nil

	}
}
func (m *Master) ReportResult(args *ReportTaskArgs, reply *ReportTaskReply) error {
	taskId := args.TaskID
	switch(args.TaskType) {
		case MapTask:
			fmt.Printf("Master.ReportResult: %d, task id: %d\n", m.mapTasks[taskId].Status, taskId)
			if m.mapTasks[taskId].Status == InProgress && !inInt64Slice(&m.mapTasks[taskId].FailedWorkers, args.WorkerID) {
				fmt.Printf("Tmp files: %v\nTarget files: %v\n", args.TmpFiles, args.TargetFiles)
				for i, name := range(args.TmpFiles) {
					os.Rename(name, args.TargetFiles[i])
				}
				m.mapTasks[taskId].Status = Finished
				dispatcher.ReduceChan <- taskId
			}
			break;
		case ReduceTask:
			if m.reduceTasks[taskId].Status == InProgress && !inInt64Slice(&m.reduceTasks[taskId].FailedWorkers, args.WorkerID) {
				fmt.Printf("Tmp files: %v\nTarget files: %v\n", args.TmpFiles, args.TargetFiles)
				for i, name := range(args.TmpFiles) {
					os.Rename(name, args.TargetFiles[i])
				}
				m.reduceTasks[taskId].Status = Finished
				// dispatcher.ReduceChan <- taskId
			}
			break;
	}
	return nil;
}


func (m *Master) exceedTimeLimt(task *Task, workerID uint64) {
	fmt.Println("Master.exceedTimeLimt: Task status:", task.Status)

	time.Sleep(10 * time.Second)

	if task.Status != Finished {
		// re-add task to the pool
		task.Status = NotStarted
		task.FailedWorkers = append(task.FailedWorkers, workerID)
		m.TaskPool.Pool <- task
	}
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}


func (m *Master) mapDone() bool {
	for _, t := range m.mapTasks {
		if t.Status != Finished {
			fmt.Printf("TaskID: %d not finished yet.", t.Conf.TaskId)
			return false
		}
	}

	return true
}

func (m *Master) reduceDone() bool {
	for _, t := range m.reduceTasks {
		if t.Status != Finished {
			return false
		}
	}

	return true
}


//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false


	if (m.taskPhase == ExitPhase) {
		ret = true
	}
	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	sources := make([][]string, len(files)+1) // 多出一行保存完成状态
	for i := 0; i < len(sources); i++ {
		sources[i] = make([]string, nReduce)
	}
	m.taskPhase = MapPhase
	m.nextWorkerId = uint64(0)
	m.nReduce = nReduce
	m.nMap = len(files)
	m.status = make([]bool, nReduce)
	m.TaskPool = &TaskPool{Pool: make(chan *Task, len(files))}
	
	m.mapTasks = make([]Task, len(files))
	m.reduceTasks = make([]Task, nReduce)

	dispatcher = &Dispatcher{
		TimeOut:          10 * time.Second,
		M:                &m,
		CleanWorkerChan:  make(chan uint64, len(files)),
		ReduceChan: 		  make(chan uint64, nReduce),
	}
	dispatcher.run()
	// 初始化map任务
	
	for num, file := range files {
		m.mapTasks[num] = Task{
			Status: NotStarted,
			Type:   MapTask, // 0 map 任务 1 reduce 任务 2 shut down 3 retry
			Conf:   &TaskConf{File: []string{file}, TaskId: uint64(num)},
		}
		m.TaskPool.Pool <- &m.mapTasks[num]
	}

	m.server()
	return &m
}


type Dispatcher struct {
	TimeOut          time.Duration      //默认10秒
	M                *Master            //主节点全局结构
	CleanWorkerChan  chan uint64        // 清理失效的worker
	ReduceChan       chan uint64
}


func (d *Dispatcher) cleanSession() {
	// for workerID := range d.CleanWorkerChan {
	// 	if w, ok := d.M.W.Load(workerID); ok {
	// 		worker := w.(*WorkerSession)
	// 		worker.Mux.Lock()
	// 		task := worker.T
	// 		worker.T = nil
	// 		worker.Mux.Unlock()
	// 		if task != nil {
	// 			task.Status = 0
	// 			//fmt.Println("cleanSession.task",workerID,task.Status,task.Conf.Source)
	// 			d.M.TP.Pool <- task
	// 		}
	// 		d.M.W.Delete(worker)
	// 		//fmt.Println("cleanSession.worker",workerID)
	// 	}
	// }
}

func (d *Dispatcher) updateJobState() {
	d.M.mutex.Lock();
	defer d.M.mutex.Unlock();
	for rs := range d.ReduceChan {
		fmt.Println(rs)
		if (d.M.mapDone()) {
			for j := 0; j < d.M.nReduce; j++ {
				files := make([]string, 0)
				for i := 0; i < d.M.nMap; i++ {
					files = append(files, fmt.Sprintf("mr-worker-%d-%d.out", i, j))
				}
				d.M.reduceTasks[j] = Task{
					Type: ReduceTask,
					Status: NotStarted,
					Conf: &TaskConf{
						File: files,
						TaskId: uint64(j),
					},
				}
				d.M.TaskPool.Pool <- &d.M.reduceTasks[j]
				fmt.Printf("Reduce task assigned: %d\n", j)
				d.M.taskPhase = ReducePhase
			}
		}

		if (d.M.reduceDone()) {
			d.M.taskPhase = ExitPhase
		}
	}

}

func (d *Dispatcher) run() {
	go d.cleanSession()
	go d.updateJobState()
}
