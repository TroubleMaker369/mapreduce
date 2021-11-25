package kvraft

import (
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

// const (
// 	OP_TYPE_PUT    = "Put"
// 	OP_TYPE_APPEND = "Append"
// 	OP_TYPE_GET    = "Get"
// )

// const Debug = false

// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug {
// 		log.Printf(format, a...)
// 	}
// 	return
// }

// type Op struct {
// 	// Your definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// 	Index    int    //写入raft log时的index
// 	Term     int    //写入raft log时的term
// 	Type     string //PutAppend,Get
// 	Key      string
// 	Value    string
// 	SeqId    int64
// 	ClientId int64
// }
// type OpContext struct {
// 	op          *Op
// 	committed   chan byte
// 	wrongLeader bool // 因为index位置log的term不一致, 说明leader换过了
// 	ignore      bool //因为req id过期, 导致该日志被跳过
// 	// Get操作的结果
// 	keyExist bool
// 	value    string
// }

// type KVServer struct {
// 	mu      sync.Mutex
// 	me      int
// 	rf      *raft.Raft
// 	applyCh chan raft.ApplyMsg
// 	dead    int32 // set by Kill()

// 	maxraftstate int // snapshot if log grows this big

// 	kvStore map[string]string  //// kv存储
// 	reqMap  map[int]*OpContext // log index -> 请求上下文
// 	seqMap  map[int64]int64    // 客户端id -> 客户端seq

// 	// Your definitions here.
// }

// func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
// 	// Your code here.
// 	reply.Err = OK
// 	op := &Op{
// 		Type:     OP_TYPE_GET,
// 		Key:      args.Key,
// 		ClientId: args.ClientId,
// 		SeqId:    args.SeqId,
// 	}
// 	var isLeader bool

// 	op.Index, op.Term, isLeader = kv.rf.Start(op)
// 	if !isLeader {
// 		reply.Err = ErrWrongLeader
// 		return
// 	}

// 	opCtx := newOpContext(op)

// 	func() {
// 		kv.mu.Lock()
// 		defer kv.mu.Unlock()

// 		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
// 		kv.reqMap[op.Index] = opCtx
// 	}()

// 	func() {
// 		kv.mu.Lock()
// 		defer kv.mu.Unlock()

// 		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
// 		kv.reqMap[op.Index] = opCtx
// 	}()

// 	timer := time.NewTimer(2000 * time.Millisecond)
// 	defer timer.Stop()
// 	select {
// 	case <-opCtx.committed: // 如果提交了
// 		if opCtx.wrongLeader { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
// 			reply.Err = ErrWrongLeader
// 		} else if !opCtx.keyExist { // key不存在
// 			reply.Err = ErrNoKey
// 		} else {
// 			reply.Value = opCtx.value // 返回值
// 		}
// 	case <-timer.C: // 如果2秒都没提交成功，让client重试
// 		reply.Err = ErrWrongLeader
// 	}
// }
// func newOpContext(op *Op) (opCtx *OpContext) {
// 	opCtx = &OpContext{
// 		op:        op,
// 		committed: make(chan byte),
// 	}
// 	return
// }
// func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
// 	// Your code here.
// 	reply.Err = OK

// 	op := &Op{
// 		Type:     args.Op,
// 		Key:      args.Key,
// 		Value:    args.Value,
// 		ClientId: args.ClientId,
// 		SeqId:    args.SeqId,
// 	}
// 	// 写入raft层
// 	var isLeader bool
// 	op.Index, op.Term, isLeader = kv.rf.Start(op)
// 	if !isLeader {
// 		reply.Err = ErrWrongLeader
// 		return
// 	}

// 	opCtx := newOpContext(op)

// 	func() {
// 		//// 保存RPC上下文，等待提交回调，
// 		//可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
// 		kv.mu.Lock()
// 		defer kv.mu.Unlock()

// 		kv.reqMap[op.Index] = opCtx
// 	}()
// 	// RPC结束前清理上下文

// 	defer func() {
// 		kv.mu.Lock()
// 		defer kv.mu.Unlock()

// 		if one, ok := kv.reqMap[op.Index]; ok {
// 			if one == opCtx {
// 				delete(kv.reqMap, op.Index)
// 			}
// 		}
// 	}()
// 	timer := time.NewTimer(2000 * time.Millisecond)
// 	defer timer.Stop()
// 	select {
// 	case <-opCtx.committed: // 如果提交了
// 		if opCtx.wrongLeader { //// 同样index位置的term不一样了,
// 			//说明leader变了，需要client向新leader重新写入
// 			reply.Err = ErrWrongLeader
// 		} else if opCtx.ignore {
// 			reply.Err = OK
// 		}
// 	case <-timer.C:
// 		{
// 			reply.Err = ErrWrongLeader
// 		}
// 	}
// }

// //
// // the tester calls Kill() when a KVServer instance won't
// // be needed again. for your convenience, we supply
// // code to set rf.dead (without needing a lock),
// // and a killed() method to test rf.dead in
// // long-running loops. you can also add your own
// // code to Kill(). you're not required to do anything
// // about this, but it may be convenient (for example)
// // to suppress debug output from a Kill()ed instance.
// //
// func (kv *KVServer) Kill() {
// 	atomic.StoreInt32(&kv.dead, 1)
// 	kv.rf.Kill()
// 	// Your code here, if desired.
// }

// func (kv *KVServer) killed() bool {
// 	z := atomic.LoadInt32(&kv.dead)
// 	return z == 1
// }

// //
// // servers[] contains the ports of the set of
// // servers that will cooperate via Raft to
// // form the fault-tolerant key/value service.
// // me is the index of the current server in servers[].
// // the k/v server should store snapshots through the underlying Raft
// // implementation, which should call persister.SaveStateAndSnapshot() to
// // atomically save the Raft state along with the snapshot.
// // the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// // in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// // you don't need to snapshot.
// // StartKVServer() must return quickly, so it should start goroutines
// // for any long-running work.
// //
// func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
// 	// call labgob.Register on structures you want
// 	// Go's RPC library to marshall/unmarshall.
// 	labgob.Register(&Op{})

// 	kv := new(KVServer)
// 	kv.me = me
// 	kv.maxraftstate = maxraftstate

// 	// You may need initialization code here.

// 	kv.applyCh = make(chan raft.ApplyMsg)
// 	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

// 	// You may need initialization code here.
// 	kv.kvStore = make(map[string]string)
// 	kv.reqMap = make(map[int]*OpContext)
// 	kv.seqMap = make(map[int64]int64)

// 	go kv.applyLoop()
// 	return kv
// }

// func (kv *KVServer) applyLoop() {
// 	for !kv.killed() {
// 		msg := <-kv.applyCh
// 		cmd := msg.Command
// 		index := msg.CommandIndex
// 		func() {
// 			kv.mu.Lock()
// 			defer kv.mu.Unlock()
// 			op := cmd.(*Op)

// 			opCtx, existOp := kv.reqMap[index]
// 			prevSeq, existSeq := kv.seqMap[op.ClientId]
// 			kv.seqMap[op.ClientId] = op.SeqId

// 			if existOp { // 存在等待结果的RPC, 那么判断状态是否与写入时一致
// 				if opCtx.op.Term != op.Term {
// 					opCtx.wrongLeader = true
// 				}
// 			}
// 			if op.Type == OP_TYPE_PUT || op.Type == OP_TYPE_APPEND {
// 				if !existSeq || op.SeqId > prevSeq { //// 如果是递增的请求ID，那么接受它的变更
// 					if op.Type == OP_TYPE_PUT { // put操作
// 						kv.kvStore[op.Key] = op.Value
// 					} else if op.Type == OP_TYPE_APPEND {
// 						if val, exist := kv.kvStore[op.Key]; exist {
// 							kv.kvStore[op.Key] = val + op.Value
// 						} else {
// 							kv.kvStore[op.Key] = op.Value
// 						}
// 					}
// 				} else if existOp {
// 					opCtx.ignore = true
// 				}
// 			} else { // OP_TYPE_GET
// 				if existOp {
// 					opCtx.value, opCtx.keyExist = kv.kvStore[op.Key]
// 				}
// 			}
// 			DPrintf("RaftNode[%d] applyLoop, kvStore[%v]", kv.me, kv.kvStore)
// 			// 唤醒挂起的RPC
// 			if existOp {
// 				close(opCtx.committed)
// 			}
// 		}()
// 	}
// }
type Op struct {
	Ch  chan (interface{})
	Req interface{}
}

var kvOnce sync.Once

type KVServer struct {
	mu             sync.Mutex
	me             int
	rf             *raft.Raft
	applyCh        chan raft.ApplyMsg
	maxraftstate   int // snapshot if log grows this big
	kvs            map[string]string
	msgIDs         map[int64]int64
	killChan       chan (bool)
	persister      *raft.Persister
	logApplyIndex  int
	EnableDebugLog bool
}

func (kv *KVServer) println(args ...interface{}) {
	if kv.EnableDebugLog {
		log.Println(args...)
	}
}

//raft操作
func (kv *KVServer) opt(client int64, msgId int64, req interface{}) (bool, interface{}) {
	if msgId > 0 && kv.isRepeated(client, msgId, false) { //去重
		return true, nil
	}
	op := Op{
		Req: req,                      //请求数据
		Ch:  make(chan (interface{})), //日志提交chan
	}
	_, _, isLeader := kv.rf.Start(op) //写入Raft
	if !isLeader {                    //判定是否是leader
		return false, nil
	}
	select {
	case resp := <-op.Ch:
		return true, resp
	case <-time.After(time.Millisecond * 1000): //超时
	}
	return false, nil
}

//读请求
func (kv *KVServer) Get(req *GetArgs, reply *GetReply) {
	ok, value := kv.opt(-1, -1, *req)
	reply.WrongLeader = !ok
	if ok {
		reply.Value = value.(string)
	}
}

//写请求
func (kv *KVServer) PutAppend(req *PutAppendArgs, reply *PutAppendReply) {
	ok, _ := kv.opt(req.Me, req.MsgId, *req)
	reply.WrongLeader = !ok
}

//写操作
func (kv *KVServer) putAppend(req *PutAppendArgs) {
	kv.println(kv.me, "on", req.Op, req.Key, ":", req.Value)
	if req.Op == "Put" {
		kv.kvs[req.Key] = req.Value
	} else if req.Op == "Append" {
		value, ok := kv.kvs[req.Key]
		if !ok {
			value = ""
		}
		value += req.Value
		kv.kvs[req.Key] = value
	}
}

//读操作
func (kv *KVServer) get(args *GetArgs) (value string) {
	value, ok := kv.kvs[args.Key]
	if !ok {
		value = ""
	}
	kv.println(kv.me, "on get", args.Key, ":", value)
	return
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.killChan <- true
}

//判定重复请求
func (kv *KVServer) isRepeated(client int64, msgId int64, update bool) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rst := false
	index, ok := kv.msgIDs[client]
	if ok {
		rst = index >= msgId
	}
	if update && !rst {
		kv.msgIDs[client] = msgId
	}
	return rst
}

//判定是否写入快照
// func (kv *KVServer) ifSaveSnapshot() {
// 	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
// 		writer := new(bytes.Buffer)
// 		encoder := labgob.NewEncoder(writer)
// 		encoder.Encode(kv.msgIDs)
// 		encoder.Encode(kv.kvs)
// 		data := writer.Bytes()
// 		kv.rf.SaveSnapshot(kv.logApplyIndex, data)
// 		return
// 	}
// }

//更新快照
// func (kv *KVServer) updateSnapshot(index int, data []byte) {
// 	if data == nil || len(data) < 1 {
// 		return
// 	}
// 	kv.logApplyIndex = index
// 	reader := bytes.NewBuffer(data)
// 	decoder := labgob.NewDecoder(reader)

// 	if decoder.Decode(&kv.msgIDs) != nil ||
// 		decoder.Decode(&kv.kvs) != nil {
// 		kv.println("Error in unmarshal raft state")
// 	}
// }

//apply 状态机
func (kv *KVServer) onApply(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid { //非状态机apply消息
		// if command, ok := applyMsg.Command.(raft.LogSnapshot); ok { //更新快照消息
		// 	kv.updateSnapshot(command.Index, command.Datas)
		// }
		// kv.ifSaveSnapshot()
		return
	}
	//更新日志索引，用于创建最新快照
	kv.logApplyIndex = applyMsg.CommandIndex
	opt := applyMsg.Command.(Op)
	var resp interface{}
	if command, ok := opt.Req.(PutAppendArgs); ok { //Put && append操作
		if !kv.isRepeated(command.Me, command.MsgId, true) { //去重复
			kv.putAppend(&command)
		}
		resp = true
	} else { //Get操作
		command := opt.Req.(GetArgs)
		resp = kv.get(&command)
	}
	select {
	case opt.Ch <- resp:
	default:
	}
	//kv.ifSaveSnapshot()
}

//轮询
func (kv *KVServer) mainLoop() {
	for {
		select {
		case <-kv.killChan:
			return
		case msg := <-kv.applyCh:
			if cap(kv.applyCh)-len(kv.applyCh) < 5 {
				log.Println("warn : maybe dead lock...")
			}
			kv.onApply(msg)
		}
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg, 10000)
	kv.kvs = make(map[string]string)
	kv.msgIDs = make(map[int64]int64)
	kv.killChan = make(chan (bool))
	kv.persister = persister
	kv.EnableDebugLog = false
	kv.logApplyIndex = 0
	kvOnce.Do(func() {
		labgob.Register(Op{})
		labgob.Register(PutAppendArgs{})
		labgob.Register(GetArgs{})
	})
	go kv.mainLoop()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//kv.rf.EnableDebugLog = false
	return kv
}
