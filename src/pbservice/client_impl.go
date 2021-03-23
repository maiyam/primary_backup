package pbservice

import (
	"fmt"
	"log"
	"time"
	"viewservice"
)

//
// additions to Clerk state.
//
type ClerkImpl struct {
	CurPrimary string
	LastPutID  int64
	LastGetID  int64
}

//
// your ck.impl.* initializations here.
//
func (ck *Clerk) initImpl() {
	ck.impl.CurPrimary = ck.vs.Primary()
	ck.impl.LastPutID = 0
	ck.impl.LastGetID = 0
}

//
// fetch a key's value from the current primary;
// if the key has never been set, return "".
// Get() must keep trying until either the
// primary replies with the value or the primary
// says the key doesn't exist, i.e., has never been Put().
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{}
	args.Key = key
	args.Impl.GetID = nrand()
	args.Impl.LastGetID = ck.impl.LastGetID
	var reply GetReply

	for !call(ck.impl.CurPrimary, "PBServer.Get", args, &reply) {
		// Keep retrying until rpc succeeds and server has an answer
		time.Sleep(viewservice.PingInterval)

		ck.impl.CurPrimary = ck.vs.Primary()

	}
	ck.impl.LastGetID = args.Impl.GetID
	if reply.Err != "" {
		fmt.Printf("%s\n", reply.Err)
	}
	return reply.Value
	// return ""
}

//
// send a Put() or Append() RPC
// must keep trying until it succeeds.
//
func (ck *Clerk)  PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{}
	args.Value = value
	args.Key = key
	args.Impl.Operation = Operation(op)
	args.Impl.OpID = nrand()
	args.Impl.LastOpID = ck.impl.LastPutID
	// fmt.Printf("Shipping these args %#v in call to server %s PutAppend\n", args, ck.impl.CurPrimary)
	var reply PutAppendReply
	for !call(ck.impl.CurPrimary, "PBServer.PutAppend", args, &reply) {
		//for !call(ck.vs.Primary(), "PutAppend", args, &reply) {
		// keep retrying until the rpc succeeds
		time.Sleep(viewservice.PingInterval)
		ck.impl.CurPrimary = ck.vs.Primary()
	}
	ck.impl.LastPutID = args.Impl.OpID
	if reply.Err != "" {
		log.Printf("Error while trying operation %s targeted at primary %s", op, ck.vs.Primary())
	}
}
