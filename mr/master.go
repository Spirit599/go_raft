package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	TaskKind string
	TaskID   int
	TaskName string
}

type Master struct {
	// Your definitions here.
	nReduce int

	numOfMapTask        int
	finishAllMapTask    bool
	numOfReduceTask     int
	finishAllReduceTask bool

	prepareExit bool

	toBeDealMapTask  map[int]Task
	toBeReplyMapTask map[int]Task
	finishedMapTask  map[int]bool

	toBeDealReduceTask  map[int]Task
	toBeReplyReduceTask map[int]Task
	finishedReduceTask  map[int]bool

	MapTaskBatchSize    int
	ReduceTaskBatchSize int

	mu sync.Mutex
}

func (m *Master) addCheckMapTaskTimer(nSeconds int, taskID int) {

	time.Sleep(time.Duration(nSeconds) * time.Second)
	
	m.mu.Lock()
	if m.finishedMapTask[taskID] {
		fmt.Println("check Map ", taskID, " success")
	} else {
		m.toBeDealMapTask[taskID] = m.toBeReplyMapTask[taskID]
		delete(m.toBeReplyMapTask, taskID)
	}
	m.mu.Unlock()
}
func (m *Master) addCheckReduceTaskTimer(nSeconds int, taskID int) {

	time.Sleep(time.Duration(nSeconds) * time.Second)
	m.mu.Lock()
	if m.finishedReduceTask[taskID] {
		fmt.Println("check Reduce ", taskID, " success")
	} else {
		m.toBeDealReduceTask[taskID] = m.toBeReplyReduceTask[taskID]
		delete(m.toBeReplyReduceTask, taskID)
	}
	m.mu.Unlock()
}

func (m *Master) afterNsecondsExit(nSeconds int) {

	time.Sleep(time.Duration(nSeconds) * time.Second)
	m.prepareExit = true

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(args *Request, reply *Reply) error {
	if args.Req == "GetTask" {
		reply.NReduce = m.nReduce

		m.mu.Lock()
		tasks := make([]Task, 0)
		var taskKind string
		fmt.Println("m.finishAllMapTask = ", m.finishAllMapTask)
		if !m.finishAllMapTask {

			if len(m.toBeDealMapTask) == 0 {
				taskKind = "NoneTask"
			} else {
				taskKind = "MapTask"
				cnt := 0
				for k, v := range m.toBeDealMapTask {
					task := v
					tasks = append(tasks, task)
					m.toBeReplyMapTask[k] = m.toBeDealMapTask[k]
					delete(m.toBeDealMapTask, k)
					go m.addCheckMapTaskTimer(10, k)

					cnt++
					if cnt == m.MapTaskBatchSize {
						break
					}
				}

			}
		} else if !m.finishAllReduceTask {
			if len(m.toBeDealReduceTask) == 0 {
				taskKind = "NoneTask"
			} else {
				taskKind = "ReduceTask"
				cnt := 0
				for k, v := range m.toBeDealReduceTask {
					task := v
					tasks = append(tasks, task)
					m.toBeReplyReduceTask[k] = m.toBeDealReduceTask[k]
					delete(m.toBeDealReduceTask, k)
					go m.addCheckReduceTaskTimer(10, k)

					cnt++
					if cnt == m.ReduceTaskBatchSize {
						break
					}
				}
			}
		} else {
			taskKind = "FinishAllTask"
		}

		reply.TaskKind = taskKind
		reply.Tasks = tasks
		m.mu.Unlock()

	}
	return nil
}

func (m *Master) ReplyResult(args *Request, reply *Reply) error {

	if args.Req == "ReplyResult" {
		m.mu.Lock()
		if args.Task.TaskKind == "MapTask" {
			m.finishedMapTask[args.Task.TaskID] = true
			fmt.Println(args.Task.TaskKind, args.Task.TaskID, " success")
			if len(m.finishedMapTask) == m.numOfMapTask {
				m.finishAllMapTask = true
			}
		}
		if args.Task.TaskKind == "ReduceTask" {
			m.finishedReduceTask[args.Task.TaskID] = true
			if len(m.finishedReduceTask) == m.numOfReduceTask {
				m.finishAllReduceTask = true
				go m.afterNsecondsExit(5)
			}
		}
		m.mu.Unlock()
	}
	return nil
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.prepareExit

	// Your code here.

	return ret
}

func MinInt(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (m *Master) initMaster(files []string, nReduce int) {

	m.nReduce = nReduce
	m.finishAllMapTask = false
	m.numOfMapTask = len(files)
	m.finishAllReduceTask = false
	m.prepareExit = false
	m.numOfReduceTask = nReduce
	m.toBeDealMapTask = make(map[int]Task)
	m.toBeReplyMapTask = make(map[int]Task)
	m.finishedMapTask = make(map[int]bool)
	m.toBeDealReduceTask = make(map[int]Task)
	m.toBeReplyReduceTask = make(map[int]Task)
	m.finishedReduceTask = make(map[int]bool)
	m.MapTaskBatchSize = 1
	m.ReduceTaskBatchSize = 1

	for i, file := range files {
		task := Task{}
		task.TaskID = i
		task.TaskKind = "MapTask"
		task.TaskName = file
		m.toBeDealMapTask[i] = task
	}

	for i := 0; i < m.numOfReduceTask; i++ {
		task := Task{}
		task.TaskID = i
		task.TaskKind = "ReduceTask"
		m.toBeDealReduceTask[i] = task
	}

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.initMaster(files, nReduce)
	// Your code here.
	fmt.Println(m.nReduce)

	m.server()
	return &m
}
