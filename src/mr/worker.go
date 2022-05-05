package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

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

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}

//
// main/mrworker.go calls this function.
//

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.run()
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func (w *worker) run() {
	for {
		resp := &GetTaskResp{}
		req := &GetTaskReq{}

		if ok := call("Coordinator.GetTask", req, resp); !ok {
			os.Exit(1)
		}
		task := resp.Task
		switch task.Phase {
		case MapPhase:
			w.doMapTask(task)
		case ReducePhase:
			w.doReduceTask(task)
		}
	}
}

func (w *worker) doMapTask(t Task) {
	req := &ReportTaskReq{Task: t}
	resp := &ReportTaskResp{}

	file, err := os.Open(t.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", t.FileName)
		return
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", t.FileName)
		return
	}

	file.Close()

	kvs := w.mapf(t.FileName, string(content))

	reduces := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		fileName := reduceName(t.Id, idx)
		f, err := os.Create(fileName)
		if err != nil {
			log.Printf("%d map task fail,err:%v", req.Task.Id, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				log.Printf("%d map task fail,err:%v", req.Task.Id, err)
				return
			}

		}
		if err := f.Close(); err != nil {
			log.Printf("%d map task fail,err:%v", req.Task.Id, err)
			return
		}
	}
	log.Printf("worker: %d map task success done", req.Task.Id)
	call("Coordinator.ReportTask", req, resp)
}

func (w *worker) doReduceTask(t Task) {
	maps := make(map[string][]string)
	req := &ReportTaskReq{Task: t}
	resp := &ReportTaskResp{}
	for idx := 0; idx < t.NMap; idx++ {
		fileName := reduceName(idx, t.Id)

		file, err := os.Open(fileName)
		if err != nil {
			log.Printf("%d reduce task fail,err:%v", req.Task.Id, err)
			return
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := ioutil.WriteFile(mergeName(t.Id), []byte(strings.Join(res, "")), 0600); err != nil {
		log.Printf("%d reduce task fail,err:%v", req.Task.Id, err)
		return
	}

	call("Coordinator.ReportTask", req, resp)
	log.Printf("worker:%d reduce task success done", req.Task.Id)

}

//
// example function to show how to make an RPC call to the coordinator.
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
