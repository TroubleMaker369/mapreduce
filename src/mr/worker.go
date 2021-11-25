package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := MapReduceArgs{MessageType: "request"}
		reply := MapReduceReply{}

		resp := call("Coordinator.MapReduceHandeler", &args, &reply)
		//resp  ---- bool
		if !resp {
			break
		}

		switch reply.Task.TaskType {
		case "Map":
			mapTask(mapf, reply.Task)
		case "Reduce":
			reduceTask(reducef, reply.Task)
		case "Wait":
			waitTask()
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}
func mapTask(mapf func(string, string) []KeyValue, task MapReduceTask) {
	// filename := task.MapFile

	// file, err := os.Open(filename)
	// if err != nil {
	// 	log.Fatalf("mapTask cannot open %v", filename)
	// }
	// content, err := ioutil.ReadAll(file) //var content []byte
	// if err != nil {
	// 	log.Fatalf("connot read %v", filename)
	// }

	// file.Close()
	// kva := mapf(filename, string(content)) //var kva []KeyValue

	// kvaa := make([][]KeyValue, task.NumReduce)
	// for _, kv := range kva {
	// 	idx := ihash(kv.Key) % task.NumReduce
	// 	kvaa[idx] = append(kvaa[idx], kv)
	// }
	// for i := 0; i < task.NumReduce; i++ {
	// 	// if len(kvaa[i]) == 0 {
	// 	// 	continue
	// 	// }
	// 	storeIntermediateFile(kvaa[i], intermediateFilename(task.TaskNum, i))
	// }
	// defer finishTask(task)
	filename := task.MapFile

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

	kvaa := make([][]KeyValue, task.NumReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NumReduce
		kvaa[idx] = append(kvaa[idx], kv)
	}

	for i := 0; i < task.NumReduce; i++ {
		storeIntermediateFile(kvaa[i], intermediateFilename(task.TaskNum, i))
	}

	defer finishTask(task)
}

/*
The worker’s map task code will need a way to store intermediate
key/value pairs in files in a way that can be correctly read back
during reduce tasks. One possibility is to use Go’s encoding/json package.
To write key/value pairs to a JSON file:
*/
func storeIntermediateFile(kva []KeyValue, filename string) {
	// file, err := os.Create(filename)
	// if err != nil {
	// 	log.Fatalf("cannot create %v\n", filename)
	// }
	// defer file.Close()

	// enc := json.NewEncoder(file) //var enc *json.Encoder
	// if err != nil {
	// 	log.Fatal("cannot create encoder")
	// }
	// for _, kv := range kva {
	// 	err := enc.Encode(&kv)
	// 	if err != nil {
	// 		log.Fatalf("cannot encode")
	// 	}
	// }
	file, err := os.Create(filename)
	defer file.Close()

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	enc := json.NewEncoder(file)
	if err != nil {
		log.Fatal("cannot create encoder")
	}
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("cannot encode")
		}
	}
	// //这里为了防止 worker 中途挂掉，先将结果写到零时文件中，最后通过 Rename 函数重新命名
	// ofile, _ := ioutil.TempFile("./", "tmp_")
	// enc := json.NewEncoder(ofile)
	// for _, kv := range kva {
	// 	err := enc.Encode(&kv)
	// 	if err != nil {
	// 		log.Fatalf("Json encode error: Key-%s, Value-%s", kv.Key, kv.Value)
	// 	}
	// }
	// defer ofile.Close()
	// os.Rename(ofile.Name(), filename)
}
func loadIntermediateFile(filename string) []KeyValue {
	// var kva []KeyValue
	// file, err := os.Open(filename)
	// if err != nil {
	// 	log.Fatalf("reduce cannot open %v", filename)
	// }
	// fmt.Printf("open %v success", file)
	// defer file.Close()
	// dec := json.NewDecoder(file)
	// for {
	// 	kv := KeyValue{}
	// 	if err := dec.Decode(&kv); err != nil {
	// 		break
	// 	}
	// 	kva = append(kva, kv)
	// }
	// return kva
	var kva []KeyValue
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	dec := json.NewDecoder(file)
	for {
		kv := KeyValue{}
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	return kva
}

//A reasonable naming convention for intermediate files is mr-X-Y,
//where X is the Map task number, and Y is the reduce task number.
func intermediateFilename(numMapTask int, numReduceTask int) string {
	return fmt.Sprintf("mr-%v-%v", numMapTask, numReduceTask)
}

func reduceTask(reducef func(string, []string) string, task MapReduceTask) {
	// //fmt.Printf("%v %v has been begin/n", task.TaskType, task.TaskNum)
	// var intermediate []KeyValue
	// for _, filename := range task.ReduceFiles {
	// 	intermediate = append(intermediate, loadIntermediateFile(filename)...)
	// }
	// sort.Sort(ByKey(intermediate))
	// outname := fmt.Sprintf("mr-out-%v", task.TaskNum)
	// outfile, err := os.Create(outname)

	// if err != nil {
	// 	log.Fatalf("cannot create %v", outname)
	// }
	// //fmt.Printf("%v has been success", outfile)
	// // ofile, _ := ioutil.TempFile("./", "tmp_")
	// // i := 0
	// // for i < len(intermediate) {
	// // 	j := i + 1
	// // 	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
	// // 		j++
	// // 	}
	// // 	values := []string{}
	// // 	for k := i; k < j; k++ {
	// // 		values = append(values, intermediate[k].Value)
	// // 	}
	// // 	output := reducef(intermediate[i].Key, values) //string []string  (k  list<v>)
	// // 	fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
	// // 	i = j
	// // }
	// // ofile.Close()
	// // os.Rename(ofile.Name(), outname)
	// i := 0
	// for i < len(intermediate) {
	// 	j := i + 1
	// 	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
	// 		j++
	// 	}
	// 	values := []string{}
	// 	for k := i; k < j; k++ {
	// 		values = append(values, intermediate[k].Value)
	// 	}
	// 	output := reducef(intermediate[i].Key, values)

	// 	// this is the correct format for each line of Reduce output.
	// 	fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)

	// 	i = j
	// }

	// outfile.Close()
	// defer finishTask(task)
	var intermediate []KeyValue
	for _, filename := range task.ReduceFiles {
		intermediate = append(intermediate, loadIntermediateFile(filename)...)
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", task.TaskNum)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

	defer finishTask(task)

}
func waitTask() {
	time.Sleep(time.Second)
}
func finishTask(task MapReduceTask) {
	args := MapReduceArgs{MessageType: "finish", Task: task}
	reply := MapReduceReply{}
	//fmt.Printf("%v %v has been finish\n", task.TaskType, task.TaskNum)
	call("Coordinator.MapReduceHandeler", &args, &reply)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

	//fmt.Println(err)
	return false
}
