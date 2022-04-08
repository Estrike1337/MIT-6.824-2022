package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MapPhase    = "MapPhase"
	ReducePhase = "ReducePhase"
)

type Coordinator struct {
	// Your definitions here.
	Files   []string
	NReduce int
	NMap    int
	Phase   string

	wg               sync.WaitGroup
	TaskDoneChan     []chan Task
	TaskReadyChan    chan Task
	TotalJobDoneChan chan struct{}
}

type Task struct {
	FileName string
	Id       int
	NReduce  int
	NMap     int
	Phase    string
}

func (c *Coordinator) initMapTask() {
	for idx, filename := range c.Files {
		task := Task{
			FileName: filename,
			Id:       idx,
			NReduce:  c.NReduce,
			Phase:    c.Phase,
			NMap:     c.NMap,
		}
		c.wg.Add(1)
		c.TaskDoneChan[task.Id] = make(chan Task, 1)
		c.TaskReadyChan <- task
	}
}

func (c *Coordinator) initReduceTask() {

	for idx := 0; idx < c.NReduce; idx++ {
		task := Task{
			Id:      idx,
			NReduce: c.NReduce,
			Phase:   c.Phase,
			NMap:    c.NMap,
		}
		c.wg.Add(1)
		c.TaskDoneChan[task.Id] = make(chan Task, 1)
		c.TaskReadyChan <- task
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(req *GetTaskReq, reply *GetTaskResp) error {
	task := <-c.TaskReadyChan

	go func() {
		select {
		case task = <-c.TaskDoneChan[task.Id]:
			log.Printf("coor:%d %s task  success", task.Id, task.Phase)
			c.wg.Done()
			return
		case <-time.After(10 * time.Second):
			log.Printf("coor:%d %s task  timeout", task.Id, task.Phase)
			c.TaskReadyChan <- task
			return
		}
	}()

	reply.Task = task
	return nil
}

func (c *Coordinator) ReportTask(req *ReportTaskReq, reply *ReportTaskResp) error {
	c.TaskDoneChan[req.Task.Id] <- req.Task
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
	select {
	case <-c.TotalJobDoneChan:
		return true
	default:
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	c := Coordinator{
		NReduce:          nReduce,
		Files:            files,
		NMap:             nMap,
		TotalJobDoneChan: make(chan struct{}, 1),
	}

	if nReduce > nMap {
		c.TaskReadyChan = make(chan Task, nReduce)
		c.TaskDoneChan = make([]chan Task, nReduce)
	} else {
		c.TaskReadyChan = make(chan Task, nMap)
		c.TaskDoneChan = make([]chan Task, nMap)
	}

	c.server()
	go c.schedule()
	return &c
}

func (c *Coordinator) schedule() {
	//map
	c.Phase = MapPhase
	c.initMapTask()

	c.wg.Wait()
	log.Printf("coor:all map tasks success done")

	c.Phase = ReducePhase
	c.initReduceTask()

	c.wg.Wait()
	log.Printf("total job done success")

	c.TotalJobDoneChan <- struct{}{}
}
