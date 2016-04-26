package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}
type Status struct {
    jobid int
    ok bool
} 


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
    statuschan := make(chan Status)
    var callworker = func(addr string, method JobType, jobid int, num int) bool {
        var args DoJobArgs
        var reply DoJobReply
        args.File = mr.file
        args.JobNumber = jobid
        args.Operation = method
        args.NumOtherPhase = num
        return call(addr, "Worker.DoJob", args, &reply)
    }

    domap := func(jobid int) {
        addr := <- mr.registerChannel
        ok := callworker(addr, Map, jobid, mr.nReduce)
        statuschan <- Status{jobid, ok}
        mr.registerChannel <- addr
    }
    for i := 0; i < mr.nMap; i++ {
        go domap(i)
    }

    var success = 0
    for success < mr.nMap {
        status := <- statuschan
        if status.ok {
            success++
        }else {
           // go domap(status.jobid)
           fmt.Println("fuck")
        }
    }
    fmt.Println("Map done")

    doreduce := func(jobid int) {
        addr := <- mr.registerChannel
        ok := callworker(addr, Reduce, jobid, mr.nMap)
        statuschan <- Status{jobid, ok}
        mr.registerChannel <- addr
    }
    for i := 0; i < mr.nReduce; i++ {
        go doreduce(i)
    }
    success = 0
    for success < mr.nReduce {
        status := <- statuschan
        if status.ok {
            success++
        }else {
            //go doreduce(status.jobid)
            fmt.Println("fuck")
        }
    }


    /*
    var jobnumber int


    for address := range mr.registerChannel {                  //success one
        go func(name string, job int) {
            mr.Workers[name] = &WorkerInfo{}
            //fmt.Println(mr.Workers)
            mr.Workers[name].address = name
            args := DoJobArgs{}
            args.File = mr.file
            args.JobNumber = job
            args.Operation = Map
            var reply DoJobReply
            ok := call(name, "Worker.DoJob", args, &reply)
            if ok == true {
                  fmt.Println("fuck")
                  args.Operation = Reduce
                  ok = call(name, "Worker.DoJob", args, &reply)

            } 
        }(address,jobnumber) 
        jobnumber += 1
    }
    */
	return mr.KillWorkers()
}
