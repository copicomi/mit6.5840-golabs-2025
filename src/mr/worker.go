package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := GetTask()
		switch reply.Type {
		case AllocMap:
			ok := ExecuteMap(reply, mapf);
			if ok {
				ReportTask(reply.TaskID, SuccessMap)
			} else {
				ReportTask(reply.TaskID, FailMap)
			}
		case AllocReduce:
			ok := ExecuteReduce(reply, reducef);
			if ok {
				ReportTask(reply.TaskID, SuccessReduce)
			} else {
				ReportTask(reply.TaskID, FailReduce)
			}
		case Wait:
			time.Sleep(100 * time.Millisecond)
		case Shutdown:
			os.Exit(0)
		}
		time.Sleep(100 * time.Millisecond) 
	}

}

func ExecuteReduce(reply *MessageReply, reducef func(string, []string) string) bool { 
	key_id := reply.TaskID
	kvs := map[string][]string{}

	files, err := GetFiles(fmt.Sprintf("^mr-\\d+-%d$", key_id), "./");
	if err != nil {
		log.Fatalf("cannot get files")
		return false;
	}

	for _, file := range files {
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
		file.Close();
	}

	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	tempfile, err := os.CreateTemp("", fmt.Sprintf("mr-tmp-%d", key_id))
	if err != nil {
		log.Fatalf("cannot create temp file")
		return false
	}

	for _, k := range keys {
		values := kvs[k]
		output := reducef(k, values)
		_, err := fmt.Fprintf(tempfile, "%v %v\n", k, output)
		if err != nil {
			log.Fatalf("cannot write to temp file")
			return false
		}
	}

	tempfile.Close()
	err = os.Rename(tempfile.Name(), fmt.Sprintf("mr-out-%d", key_id));
	if err != nil {
		log.Fatalf("cannot rename temp file")
		return false
	}

	RemoveFiles(fmt.Sprintf("^mr-\\d+-%d$", key_id), "./");

	return true;
}

func ExecuteMap(reply *MessageReply, mapf func(string, string) []KeyValue) bool {

	filename := reply.TaskName
	file, err := os.Open(filename)
	if err != nil {
		return false
	}
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return false
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	tempFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for _, kv := range kva {
		reduceID := ihash(kv.Key) % reply.NReduce
		if tempFiles[reduceID] == nil {
			tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-tmp-%d", reduceID))
			if err != nil {
				log.Fatalf("cannot create temp file")
				return false
			}
			defer tempFile.Close()
			tempFiles[reduceID] = tempFile
			encoders[reduceID] = json.NewEncoder(tempFile)
		}
		err := encoders[reduceID].Encode(kv)
		if err != nil {
			log.Fatalf("cannot encode")
			return false
		}
	}

	for i, file := range tempFiles { 
		if file != nil {
			file.Close()
			err := os.Rename(file.Name(), fmt.Sprintf("mr-%d-%d", reply.TaskID, i))
			if err != nil {
				log.Fatalf("cannot rename")
				return false
			}
		}
	}
	return true
}

func GetTask() *MessageReply { 
	args := MessageSend{ Type: AskTask }
	reply := MessageReply{}
	ok := call("Coordinator.AllocTask", &args, &reply)
	if ok {
		return &reply
	} else {
		return nil
	}
}

func ReportTask(taskID int, msgType MsgType) bool {
	args := MessageSend{ Type: msgType, TaskID: taskID }
	reply := MessageReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	return ok
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
