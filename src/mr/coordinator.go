package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here..
	mu                sync.Mutex
	NumMap            int
	NumMApFinished    int
	NumReduce         int
	NumReduceFinished int

	MapTasks    []MapReduceTask
	ReduceTasks []MapReduceTask

	MapFinihed   bool
	ReduceFinish bool
}

// func (c *Coordinator) checkTimeout(args *MapReduceArgs) {
// 	time.Sleep(time.Second * time.Duration(10))
// 	c.mu.Lock()
// 	defer c.mu.Unlock()
// 	if args.MessageType != "finish" && args.Task.TaskType == "Map" {
// 		if c.MapTasks[args.Task.TaskNum].TaskStatus == "Assigned" {
// 			c.MapTasks[args.Task.TaskNum].TaskStatus = "Unassigned"
// 			//fmt.Printf("%v %v has been repeat\n", c.MapTasks[num].TaskType, c.MapTasks[num].TaskNum)
// 		}
// 		c.NumMApFinished = c.NumMApFinished - 1
// 	} else if args.MessageType != "finish" && args.Task.TaskType == "Reduce" {
// 		if c.ReduceTasks[args.Task.TaskNum].TaskStatus == "Assigned" {
// 			c.ReduceTasks[args.Task.TaskNum].TaskStatus = "Unassiged"
// 		}
// 		c.NumReduceFinished = c.NumReduceFinished - 1
// 	}
// }
func (c *Coordinator) checkTimeout(taskType string, num int, timeout int) {
	time.Sleep(time.Second * time.Duration(timeout))
	c.mu.Lock()
	defer c.mu.Unlock()
	if taskType == "Map" {
		if c.MapTasks[num].TaskStatus == "Assigned" {
			c.MapTasks[num].TaskStatus = "Unassigned"
			c.NumMApFinished = c.NumMApFinished - 1
		}
	} else {
		if c.ReduceTasks[num].TaskStatus == "Assigned" {
			c.ReduceTasks[num].TaskStatus = "Unassigned"
			c.NumReduceFinished = c.NumReduceFinished - 1
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (m *Coordinator) MapReduceHandeler(args *MapReduceArgs, reply *MapReduceReply) error {
	// c.mu.Lock()
	// defer c.mu.Unlock()
	// if args.MessageType == "request" {
	// 	if !c.MapFinihed {
	// 		for index, task := range c.MapTasks {
	// 			if task.TaskStatus == "Unassigned" {
	// 				c.MapTasks[index].TaskStatus = "Assigned"
	// 				reply.Task = c.MapTasks[index]
	// 				go c.checkTimeout("Map", index, 10)
	// 				return nil
	// 			}
	// 		}
	// 		reply.Task.TaskType = "Wait"
	// 		return nil
	// 	} else if !c.ReduceFinish {
	// 		for index, task := range c.ReduceTasks {
	// 			if task.TaskStatus == "Unassigned" {
	// 				c.ReduceTasks[index].TaskStatus = "Assigned"
	// 				reply.Task = c.ReduceTasks[index]
	// 				go c.checkTimeout("Reduce", index, 10)
	// 				return nil
	// 			}
	// 		}
	// 		reply.Task.TaskType = "Wait"
	// 		return nil
	// 	} else {
	// 		return nil
	// 	}
	// } else if args.MessageType == "finish" {
	// 	if args.Task.TaskType == "Map" {
	// 		c.NumMApFinished = c.NumMApFinished + 1
	// 		c.MapTasks[args.Task.TaskNum].TaskStatus = "Finished"
	// 		if c.NumMApFinished == c.NumMap {
	// 			c.MapFinihed = true
	// 		}
	// 	} else {
	// 		c.NumReduceFinished = c.NumReduceFinished + 1
	// 		c.ReduceTasks[args.Task.TaskNum].TaskStatus = "Finished"
	// 		if c.NumReduceFinished == c.NumReduce {
	// 			c.ReduceFinish = true
	// 		}
	// 	}
	// 	return nil
	// }
	// //fmt.Println("map/reduce has been done")
	// return nil
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.MessageType == "request" {
		if !m.MapFinihed {
			for index, task := range m.MapTasks {
				if task.TaskStatus == "Unassigned" {
					m.MapTasks[index].TaskStatus = "Assigned"
					reply.Task = m.MapTasks[index]
					go m.checkTimeout("Map", index, 10)
					return nil
				}
			}
			reply.Task.TaskType = "Wait"
			return nil
		} else if !m.ReduceFinish {
			for index, task := range m.ReduceTasks {
				if task.TaskStatus == "Unassigned" {
					m.ReduceTasks[index].TaskStatus = "Assigned"
					reply.Task = m.ReduceTasks[index]
					go m.checkTimeout("Reduce", index, 10)
					return nil
				}
			}
			reply.Task.TaskType = "Wait"
			return nil
		} else {
			return nil
		}
	} else if args.MessageType == "finish" {
		if args.Task.TaskType == "Map" {
			m.MapTasks[args.Task.TaskNum].TaskStatus = "Finished"
			m.NumMApFinished = m.NumMApFinished + 1
			if m.NumMApFinished == m.NumMap {
				m.MapFinihed = true
			}
		} else {
			m.ReduceTasks[args.Task.TaskNum].TaskStatus = "Finished"
			m.NumReduceFinished = m.NumReduceFinished + 1
			if m.NumReduceFinished == m.NumReduce {
				m.ReduceFinish = true
			}
		}
		return nil
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	return c.ReduceFinish && c.MapFinihed
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// c := Coordinator{}
	// // Your code here.
	// c.NumMap = len(files)
	// c.NumReduce = nReduce
	// c.MapFinihed = false
	// c.ReduceFinish = false
	// for index, file := range files {
	// 	var tempTask MapReduceTask
	// 	tempTask.NumMap = c.NumMap
	// 	tempTask.NumReduce = c.NumReduce
	// 	tempTask.TaskType = "Map"
	// 	tempTask.TaskStatus = "Unassigned"
	// 	tempTask.TaskNum = index
	// 	tempTask.MapFile = file
	// 	c.MapTasks = append(c.MapTasks, tempTask)
	// }
	// for i := 0; i < c.NumReduce; i++ {
	// 	var tempTask MapReduceTask
	// 	tempTask.NumMap = c.NumMap
	// 	tempTask.NumReduce = c.NumReduce
	// 	tempTask.TaskType = "Reduce"
	// 	tempTask.TaskStatus = "Unassigned"
	// 	tempTask.TaskNum = i
	// 	for j := 0; j < c.NumMap; j++ {
	// 		tempTask.ReduceFiles = append(tempTask.ReduceFiles, intermediateFilename(j, i))
	// 	}
	// 	c.ReduceTasks = append(c.ReduceTasks, tempTask)
	// }

	// c.server()
	// return &c
	m := Coordinator{}

	// Your code here.
	m.NumMap = len(files)
	m.NumReduce = nReduce
	m.MapFinihed = false
	m.ReduceFinish = false
	for index, file := range files {
		var tempTask MapReduceTask
		tempTask.NumMap = m.NumMap
		tempTask.NumReduce = m.NumReduce
		tempTask.TaskType = "Map"
		tempTask.TaskStatus = "Unassigned"
		tempTask.TaskNum = index
		tempTask.MapFile = file
		m.MapTasks = append(m.MapTasks, tempTask)
	}
	for i := 0; i < m.NumReduce; i++ {
		var tempTask MapReduceTask
		tempTask.NumMap = m.NumMap
		tempTask.NumReduce = m.NumReduce
		tempTask.TaskType = "Reduce"
		tempTask.TaskStatus = "Unassigned"
		tempTask.TaskNum = i
		for j := 0; j < m.NumMap; j++ {
			tempTask.ReduceFiles = append(tempTask.ReduceFiles, intermediateFilename(j, i))
		}
		m.ReduceTasks = append(m.ReduceTasks, tempTask)
	}

	m.server()
	return &m
}
