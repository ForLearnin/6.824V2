package mr

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"sync/atomic"
)
import "net"
import "net/rpc"
import "net/http"

//可以先完成一个worker版，后面增加worker任务的管理。
//MAP 阶段结束后，才可以进行reduce阶段。
//worker可以重复利用
type Coordinator struct {
	count int32
	//可以正常链接的worker
	normal []WorkerName
	//失去链接的worker
	disconnect []WorkerName
	taskSet    TaskSet
}
type TaskSet struct {
	mapStateSet    map[MapTaskStruct]TaskState
	reduceStateSet map[ReduceTaskStruct]TaskState
}
type WorkerName string

type Task struct {
	MapTask     MapTaskStruct
	ReduceTask  ReduceTaskStruct
	IsMapReduce bool
	x           int
}
type ReduceTaskStruct struct {
	TaskIndex int
}

type MapTaskStruct struct {
	FileName string
}
type TaskState struct {
	//未被分配
	Idle bool
	//处理中
	InProgress bool
	//处理完成
	Finished bool
}

// Your code here -- RPC handlers for the WorkerName to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) ApplyTask(args *ExampleArgs, reply *Task) error {
	set := c.taskSet
	reply.x = 12
	for task, taskState := range set.mapStateSet {
		if taskState.Idle {
			reply.IsMapReduce = true
			reply.MapTask = task
			reply.IsMapReduce = true
			taskState.Idle = false
			taskState.InProgress = true
			break
		}
	}
	return nil
}

/**
心跳还是需要在考虑一下
*/
func (c *Coordinator) ping(args *Ping, reply *Ping) {
	if args.workerName == "" {
		count := c.count
		index := strconv.Itoa(int(count))
		c.count = atomic.AddInt32(&c.count, 1)
		workerName := "WorkerName-" + index
		reply.workerName = workerName
	}
	exist := false
	for _, w := range c.normal {
		if reply.workerName == string(w) {
			exist = true
			break
		}
	}
	if exist {
		c.normal = append(c.normal, WorkerName(reply.workerName))
	}
}

//
// start a thread that listens for RPCs from WorkerName.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":12356")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has Finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		count:      0,
		normal:     []WorkerName{},
		disconnect: []WorkerName{},
		taskSet:    TaskSet{mapStateSet: map[MapTaskStruct]TaskState{}, reduceStateSet: map[ReduceTaskStruct]TaskState{}},
	}

	for _, file := range files {
		task := MapTaskStruct{FileName: file}
		state := TaskState{Idle: true}
		c.taskSet.mapStateSet[task] = state
	}
	for i := 0; i < nReduce; i++ {
		task := ReduceTaskStruct{i}
		state := TaskState{Idle: true}
		c.taskSet.reduceStateSet[task] = state
	}
	allData := readAllData(files)

	fmt.Print(len(allData))
	//for _, filename := range os.Args[2:] {
	//	file, err := os.Open(filename)
	//	if err != nil {
	//		log.Fatalf("cannot open %v", filename)
	//	}
	//	content, err := ioutil.ReadAll(file)
	//	if err != nil {
	//		log.Fatalf("cannot read %v", filename)
	//	}
	//	file.Close()
	//	kva := mapf(filename, string(content))
	//	intermediate = append(intermediate, kva...)
	//执行对数据的分割，并将其划分给不同的mapTask

	// Your code here.

	c.server()
	return &c
}

//获得文件中的所有数据
func readAllData(files []string) []string {
	var res []string
	var byteBuffer bytes.Buffer
	byteBuffer.Grow(10000)
	for _, file := range files {
		content, err := ioutil.ReadFile(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		byteBuffer.WriteString(string(content))
	}
	allData := byteBuffer.String()
	res = append(res, allData)
	return res
}
