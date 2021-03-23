package pbservice

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
	"viewservice"
)

//
// additions to PBServer state.
//
type PBServerImpl struct {
	curViewNum uint
	curPrimary string
	curBackup  string
	kv         map[string]string
	pIds       map[int64]bool
	gIds       map[int64]bool
	pmu        sync.Mutex
}

//
// your pb.impl.* initializations here.
//
func (pb *PBServer) initImpl() {
	pb.impl.curViewNum = 0
	pb.impl.curPrimary = ""
	pb.impl.curBackup = ""
	pb.impl.kv = make(map[string]string)
	pb.impl.pIds = make(map[int64]bool)
	pb.impl.gIds = make(map[int64]bool)
}

//
// server Get() RPC handler.
//
func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	//	fmt.Println("Got a Get!")
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.isdead() {
		errString := fmt.Sprintf("Get: server %s dead", pb.me)
		fmt.Println(errString)
		*reply = GetReply{Err(errString), ""}
		return errors.New(errString)
	}

	if pb.impl.kv == nil {
		errString := fmt.Sprintf("null kv store in the server %s during Get operation", pb.me)
		fmt.Println(errString)
		*reply = GetReply{Err(errString), ""}
		return errors.New(errString)
	}
	if args.Key == "" {
		errString := fmt.Sprintf("Empty Key arg during Get operation for %s", pb.me)
		fmt.Println(errString)
		*reply = GetReply{Err(errString), ""}
		return errors.New(errString)
	}
	if pb.me != pb.impl.curBackup && pb.me != pb.impl.curPrimary {
		*reply = GetReply{Err(ErrWrongServer), ""}
		fmt.Printf("Trying to Get value for %s from non-primary server %s\n", args.Key, pb.me)
		return errors.New(ErrWrongServer)
	}
	if val, exists := pb.impl.kv[args.Key]; !exists {
		//	fmt.Printf("Key %s was not found on the server %s  !\n", args.Key, pb.me)
		*reply = GetReply{Err(ErrNoKey), ""}
	} else {
		// fmt.Printf("Key %s was found on the server %s  : %s!\n", args.Key, pb.me, val.Val)
		switch pb.me {
		case pb.impl.curBackup:
			if args.Impl.ViewNum < pb.impl.curViewNum {
				fmt.Printf("Received a Get from out of synch primary %d\n", args.Impl.ViewNum)
				*reply = GetReply{Err(ErrWrongServer), ""}
				return errors.New(ErrWrongServer)
			}
			delete(pb.impl.gIds, args.Impl.LastGetID)
			pb.impl.gIds[args.Impl.GetID] = true
			*reply = GetReply{"", val}
			break

		case pb.impl.curPrimary:
			// If duplicate request answer without consulting the backup
			*reply = GetReply{"", val}
			if _, exists := pb.impl.gIds[args.Impl.GetID]; !exists {
				// Consult with the backup and if values in synch all is well
				if pb.impl.curBackup != "" {
					bkArgs := &GetArgs{}
					bkArgs.Key = args.Key
					bkArgs.Impl.ViewNum = pb.impl.curViewNum
					bkArgs.Impl.GetID = args.Impl.GetID
					bkArgs.Impl.LastGetID = args.Impl.LastGetID
					var bkReply GetReply
					// populate args.Impl
					if !call(pb.impl.curBackup, "PBServer.Get", bkArgs, &bkReply) {
						// ASK: Keep retrying until rpc succeeds and server has an answer
						//time.Sleep(viewservice.PingInterval)
						errStr := fmt.Sprintf("There was an error getting key %s from backup %s", bkArgs.Key, pb.impl.curBackup)
						fmt.Println(errStr)

						if bkReply.Err == ErrWrongServer {
							fmt.Printf("ALERT: partitioned primary! : %s, and viewnum: %d\n", pb.me, pb.impl.curViewNum)
							*reply = GetReply{bkReply.Err, ""}
							return errors.New(string(bkReply.Err))
						}
					}
					if bkReply.Err != "" {
						fmt.Printf("Unfound error. Will likely return stale value%s\n", bkReply.Err)
						//*reply = GetReply{bkReply.Err, ""}
						break
					}
					if bkReply.Value != reply.Value {
						//fmt.Printf("Backup and primary values don't match %v \n\n%v\n", bkReply.Value, reply.Value)
						errStr := fmt.Sprintf("Backup and primary values don't match")
						*reply = GetReply{Err(errStr), ""}
						return errors.New(errStr)
					}
				}

			}
			// update GID
			pb.impl.gIds[args.Impl.GetID] = true
			delete(pb.impl.gIds, args.Impl.LastGetID)
		}

	}
	//	fmt.Printf("The reply to this Get call was %#v\n", reply)
	return nil
}

//
// server PutAppend() RPC handler.
//
func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	//	fmt.Printf("Got a putAppend! Key:%s Val: %s, server: %s\n", args.Key, args.Value, pb.me)
	pb.impl.pmu.Lock()
	defer pb.impl.pmu.Unlock()
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.isdead() {
		errString := fmt.Sprintf("PutAppend: server %s dead", pb.me)
		fmt.Println(errString)
		*reply = PutAppendReply{Err(errString)}
		return errors.New(errString)
	}

	if pb.impl.kv == nil {
		errString := fmt.Sprintf("null kv store in the server %s during PutAppend operation", pb.me)
		fmt.Println(errString)
		*reply = PutAppendReply{Err(errString)}
		return errors.New(errString)
	}
	// Wrong server or no longer the primary
	if pb.me != pb.impl.curBackup && pb.me != pb.impl.curPrimary {
		*reply = PutAppendReply{Err(ErrWrongServer)}
		fmt.Printf("Trying to %d value for %s from non-primary server %s\n", args.Impl.OpID, args.Key, pb.me)
		return errors.New(ErrWrongServer)
	}

	if pb.me == pb.impl.curBackup {
		if args.Impl.ViewNum != pb.impl.curViewNum {
			fmt.Printf("Received a PutAppend from out of synch primary %d\n", args.Impl.ViewNum)
			*reply = PutAppendReply{Err(ErrWrongServer)}
			return errors.New(ErrWrongServer)
		}
	}

	var err Err = Err("")

	if _, ok := pb.impl.pIds[args.Impl.OpID]; ok {
		// fmt.Printf("This %s operation on key %s and value %s to server %s is a dup. Hence rejecting", args.Impl.Operation, args.Key, args.Value, pb.me)
		*reply = PutAppendReply{err}
		return nil
	}
	// Save previous state to enable rolling back should backup sync fail
	prevKV := pb.impl.kv
	prePids := pb.impl.pIds

	keyVal, _ := pb.impl.kv[args.Key]
	switch args.Impl.Operation {
	case "Put":
		pb.impl.kv[args.Key] = args.Value
		pb.impl.pIds[args.Impl.OpID] = true
		delete(pb.impl.pIds, args.Impl.LastOpID)
		break

	case "Append":
		keyVal = keyVal + args.Value
		pb.impl.kv[args.Key] = keyVal
		pb.impl.pIds[args.Impl.OpID] = true
		delete(pb.impl.pIds, args.Impl.LastOpID)
		break
	}
	// sync to backup if any
	if pb.impl.curPrimary == pb.me && pb.impl.curBackup != "" {
		bkArgs := &PutAppendArgs{}
		bkArgs.Value = args.Value
		bkArgs.Key = args.Key
		bkArgs.Impl = PutAppendArgsImpl{args.Impl.Operation, args.Impl.OpID, args.Impl.LastOpID, pb.impl.curViewNum}

		var bkReply PutAppendReply
		for !call(pb.impl.curBackup, "PBServer.PutAppend", bkArgs, &bkReply) {
			if pb.isdead() {
				errString := fmt.Sprintf("PutAppend: server %s DEAD", pb.me)
				fmt.Println(errString)
				bkReply.Err = Err(errString)
				break
			}
			errstr := fmt.Sprintf("Failed syncing PutAppend to the backup server %s from primary %s\n", pb.impl.curBackup, pb.me)
			fmt.Println(errstr)
			if bkReply.Err == "" {
				fmt.Println("Unlocking  to sleep")
				pb.mu.Unlock()
				time.Sleep(viewservice.PingInterval)

				pb.mu.Lock()
				if pb.isdead() {
					errString := fmt.Sprintf("PutAppend: server %s DEAD", pb.me)
					fmt.Println(errString)
					bkReply.Err = Err(errString)
					break
				}
				fmt.Println("locking after sleep")
				if pb.impl.curPrimary != pb.me || pb.impl.curBackup == "" {
					if pb.impl.curPrimary != pb.me {
						fmt.Println("oops primary has since changed!")
						bkReply.Err = Err(ErrWrongServer)
					}
					break
				}
				bkArgs.Impl.ViewNum = pb.impl.curViewNum
			} else {
				break
			}
		}
		if bkReply.Err != "" {
			// rollback changes made to primary and add the error in the response

			fmt.Printf("Before: %#v\n", pb.impl.kv)
			fmt.Printf("ROLLING back:\n")
			pb.impl.kv = prevKV
			pb.impl.pIds = prePids
			fmt.Printf("After: %#v\n", pb.impl.kv)
			err = bkReply.Err
		}
	}
	*reply = PutAppendReply{err}
	if err != "" {
		return errors.New(string(err))
	}
	//fmt.Printf("Put or appended %s for key %s in server %s\n", args.Value, args.Key, pb.me)
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	if pb.isdead() {
		errString := fmt.Sprintf("tick: Server %s dead", pb.me)
		fmt.Println(errString)
		return
	}
	//	fmt.Printf("Locking on %s for tick\n", pb.me)
	pb.mu.Lock()
	defer pb.mu.Unlock()
	curView, err := pb.vs.Ping(pb.impl.curViewNum)
	if err != nil {
		fmt.Printf("Error in pb server Ping.  %s\n", err)
		return
	}
	if curView.Viewnum != pb.impl.curViewNum {
		pb.impl.curViewNum = curView.Viewnum
		if pb.impl.curPrimary != curView.Primary {
			// Do something special if I was the primary?
			// Do something special if I am the new primary?
			pb.impl.curPrimary = curView.Primary
			fmt.Printf("Changed current primary on %s to %s\n", pb.me, pb.impl.curPrimary)
		}
		if pb.impl.curBackup != curView.Backup {
			pb.impl.curBackup = curView.Backup
			// if I am the primary then bootstrap the new backup
			if pb.me == pb.impl.curPrimary && pb.impl.curBackup != "" {
				bootstrpArgs := &BootstrapBackupArgs{}
				bootstrpArgs.KVMap = pb.impl.kv
				bootstrpArgs.PIds = pb.impl.pIds
				bootstrpArgs.GIds = pb.impl.gIds
				var bootstrpReply BootstrapBackupReply
				if !call(pb.impl.curBackup, "PBServer.BootstrapBackup", bootstrpArgs, &bootstrpReply) {
					log.Printf("Bootstrapping  backup server %s failed\n", pb.impl.curBackup)
				}
				if bootstrpReply.Error != "" {
					log.Printf("There was an error when initializing the backup %s from the primary: %s\nError: %s\n", pb.impl.curBackup, pb.impl.curPrimary, bootstrpReply.Error)
				}
			}
			// if I am the Backup do something special as part of tick()?
			if pb.me == pb.impl.curBackup {
				//do something?
			}
		}
	}
}

//
// add RPC handlers for any new RPCs that you include in your design.
//

// rpc to handle bootstrap of new backup
func (pb *PBServer) BootstrapBackup(args *BootstrapBackupArgs, reply *BootstrapBackupReply) error {

	//  fmt.Printf("%s Recieved a bootstrap call with args %#v\n", pb.me, args)
	fmt.Printf("%s Recieved a bootstrap call \n", pb.me)
	if pb.isdead() {
		errString := fmt.Sprintf("New Backup server %s dead", pb.me)
		fmt.Println(errString)
		*reply = BootstrapBackupReply{Err(errString)}
		return nil
	}
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// if pb.me == pb.impl.curBackup {
	if pb.impl.kv == nil {
		pb.impl.kv = make(map[string]string)
	}
	if pb.impl.gIds == nil {
		pb.impl.gIds = make(map[int64]bool)
	}
	if pb.impl.pIds == nil {
		pb.impl.pIds = make(map[int64]bool)
	}
	pb.impl.kv = args.KVMap
	pb.impl.pIds = args.PIds
	pb.impl.gIds = args.GIds
	*reply = BootstrapBackupReply{Err("")}

	// } else {
	// 	*reply = BootstrapBackupReply{Err(ErrWrongServer + " " + BootstrapFailed)}
	// }
	return nil
}
