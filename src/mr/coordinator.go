package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskStatus int

const (
	idle taskStatus = iota
	running
	finished
	failed
)

type MapTaskInfo struct {
	TaskID	 int
	Status taskStatus
	StartTime int64
}

type ReduceTaskInfo struct {
	Status taskStatus
	StartTime int64
}


type Coordinator struct {
	// Your definitions here.
	NReduce int
	MapTasks map[string]*MapTaskInfo
	ReduceTasks []*ReduceTaskInfo
	Lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AllocTask(args *MessageSend, reply *MessageReply) error {
	// Your code here.
	if args.Type != AskTask { 
		return errors.New("Invalid message type")
	}
	c.Lock.Lock()
	defer c.Lock.Unlock()

	finish_count := 0
	for filename, taskinfo := range c.MapTasks {
		alloc_flag := false
		status := taskinfo.Status
		switch status {
		case idle, failed:
			alloc_flag = true
		case running:
			if time.Now().Unix() - taskinfo.StartTime > 10 {
				alloc_flag = true
			}
		case finished:
			finish_count ++
		}
		if alloc_flag {
			taskinfo.Status = running
			taskinfo.StartTime = time.Now().Unix()
			reply.Type = AllocMap
			reply.NReduce = c.NReduce
			reply.TaskID = taskinfo.TaskID
			reply.TaskName = filename
			return nil
		}
	}

	if finish_count != len(c.MapTasks) {
		reply.Type = Wait
		return nil
	}

	finish_count = 0

	for idx, taskinfo := range c.ReduceTasks { 
		alloc_flag := false
		status := taskinfo.Status
		switch status {
		case idle, failed:
			alloc_flag = true
		case running:
			if time.Now().Unix() - taskinfo.StartTime > 10 {
				alloc_flag = true
			}
		case finished:
			finish_count ++
		}
		if alloc_flag {
			taskinfo.Status = running
			taskinfo.StartTime = time.Now().Unix()
			reply.Type = AllocReduce
			reply.NReduce = c.NReduce
			reply.TaskID = idx
			return nil
		}
	}

	if finish_count != len(c.ReduceTasks) {
		reply.Type = Wait
		return nil
	}

	reply.Type = Shutdown
	return nil
}

func (c *Coordinator) ReportTask(args *MessageSend, reply *MessageReply) error {
	// Your code here.
	c.Lock.Lock()
	defer c.Lock.Unlock()

	taskID := args.TaskID
	switch args.Type {
	case SuccessMap:
		for _, taskinfo := range c.MapTasks {
			if taskinfo.TaskID == taskID {
				taskinfo.Status = finished
				break
			}
		}
	case FailMap:
		for _, taskinfo := range c.MapTasks {
			if taskinfo.TaskID == taskID {
				taskinfo.Status = failed
				break
			}
		}
	case SuccessReduce:
		c.ReduceTasks[taskID].Status = finished
	case FailReduce:
		c.ReduceTasks[taskID].Status = failed
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	for _, taskinfo := range c.MapTasks {
		if taskinfo.Status != finished {
			return false
		}
	}

	for _, taskinfo := range c.ReduceTasks {
		if taskinfo.Status != finished {
			return false
		}
	}

	return true;
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce: nReduce,
		MapTasks: make(map[string]*MapTaskInfo),
		ReduceTasks: make([]*ReduceTaskInfo, nReduce),
	}
	c.InitTask(files)
	c.server()
	return &c
}

func (c *Coordinator) InitTask(files []string) {
	for idx, filename := range files {
		c.MapTasks[filename] = &MapTaskInfo{
			TaskID: idx,
			Status: idle,
		}
	}
	for idx := range c.ReduceTasks {
		c.ReduceTasks[idx] = &ReduceTaskInfo{
			Status: idle,
		}
	}
}