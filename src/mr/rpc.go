package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//


type TaskType uint8


type TaskPhase uint8
const (
	MapPhase    TaskPhase = iota
	ReducePhase TaskPhase = iota
	ExitPhase   TaskPhase = iota
)

const (
	MapTask TaskType = iota
	ReduceTask TaskType = iota
	EmptyTask TaskType = iota
	ExitTask TaskType = iota
)

type TaskStatus uint8

const (
	NotStarted TaskStatus = iota
	InProgress TaskStatus = iota
	Error      TaskStatus = iota
	Finished   TaskStatus = iota
)


type RegisterWorkerArgs struct {
	
}

type RegisterWorkerReply struct {
	WorkerID uint64
	RC int
}


type GetWorkerTaskArgs struct {
	WorkerID uint64
}

type GetWorkerTaskReply struct {
	Task *Task 
}


type ReportTaskArgs struct {
	WorkerID uint64
	TaskID uint64
	TaskType TaskType
	Status   TaskStatus
	Msg      string
	M        []string
	TmpFiles []string
	TargetFiles []string
}

type ReportTaskReply struct {
	Task *Task 
	TaskType TaskType
	Msg  string
}



type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
