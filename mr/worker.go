package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Workerr struct {
	tasks    []Task
	taskKind string
	nReduce  int
	wg       sync.WaitGroup
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

func (worker *Workerr) workerMap(task Task, X int, mapf func(string, string) []KeyValue) {
	filename := task.TaskName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	os.Mkdir("./tmp", os.ModePerm)

	oofile := make([]*os.File, worker.nReduce)

	for i := 0; i < worker.nReduce; i++ {
		outputfile := "./tmp/mr-" + strconv.Itoa(X) + "-" + strconv.Itoa(i)
		oofile[i], err = os.Create(outputfile)
		if err != nil {
			fmt.Println(err)
		}
	}

	i := 0
	for i < len(kva) {
		reduceIndex := ihash(kva[i].Key) % worker.nReduce
		fmt.Fprintf(oofile[reduceIndex], "%v %v\n", kva[i].Key, kva[i].Value)
		fmt.Printf("%v %v\n", kva[i].Key, kva[i].Value)
		i++
	}

	fmt.Println(task.TaskKind, X, " finish")
	CallReplyResult(task)

	defer worker.wg.Done()

}

func (worker *Workerr) workerReduce(task Task, reduceIndex int, reducef func(string, []string) string) {
	var fileList []string
	rd, err := ioutil.ReadDir("./tmp")
	if err != nil {
		fmt.Println(err)
	}

	for _, fi := range rd {
		if !fi.IsDir() {
			tmp := strings.Split(fi.Name(), "-")
			Y := tmp[len(tmp)-1]
			intY, _ := strconv.Atoi(Y)
			if intY == reduceIndex {
				fileList = append(fileList, "./tmp/"+fi.Name())
			}
		}
	}

	intermediate := []KeyValue{}

	for _, fileName := range fileList {
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Println(err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			fmt.Println(err)
		}
		file.Close()
		strContext := string(content)

		ff := func(r rune) bool { return r == ' ' || r == '\n' }
		words := strings.FieldsFunc(strContext, ff)

		if len(words)%2 != 0 {
			fmt.Println(words)
		}

		for i := 0; i < len(words); i += 2 {
			intermediate = append(intermediate, KeyValue{words[i], words[i+1]})
		}
	}

	sort.Sort(ByKey(intermediate))

	outPutName := "mr-out-" + strconv.Itoa(reduceIndex)
	ofile, _ := os.Create(outPutName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	fmt.Println(task.TaskKind, reduceIndex, " finish")
	CallReplyResult(task)

	defer worker.wg.Done()

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {

		worker := CallGetTask()

		taskKind := worker.taskKind
		taskList := worker.tasks

		fmt.Println(taskKind)

		if taskKind == "MapTask" {
			for _, task := range taskList {

				worker.wg.Add(1)
				go worker.workerMap(task, task.TaskID, mapf)
			}
			worker.wg.Wait()
		} else if taskKind == "ReduceTask" {
			for _, task := range taskList {

				worker.wg.Add(1)
				go worker.workerReduce(task, task.TaskID, reducef)
			}
			worker.wg.Wait()

		} else if taskKind == "NoneTask" {
			fmt.Println("NoneTask")
		} else { //taskKind == "FinishAllTask"
			fmt.Println(taskKind)
			break
		}

		// for i := 0; i < worker.reduceGoroutine.nReduce; i++ {
		// 	worker.reduceGoroutine.WaitGroup.Add(1)
		// 	go worker.workerReduce(i, reducef)
		// }

		// worker.reduceGoroutine.WaitGroup.Wait()

		time.Sleep(time.Second)
	}

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

	// reply.Y should be 100.fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallGetTask() *Workerr {

	args := Request{}
	args.Req = "GetTask"
	reply := Reply{}

	call("Master.GetTask", &args, &reply)

	ret := Workerr{}

	ret.nReduce = reply.NReduce
	ret.taskKind = reply.TaskKind
	ret.tasks = reply.Tasks

	return &ret
}

func CallReplyResult(task Task) {

	args := Request{}
	args.Req = "ReplyResult"
	args.Task = task
	reply := Reply{}

	call("Master.ReplyResult", &args, &reply)
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
