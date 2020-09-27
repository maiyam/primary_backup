package pbservice

//
// additions to PBServer state.
//
type PBServerImpl struct {
}

//
// your pb.impl.* initializations here.
//
func (pb *PBServer) initImpl() {
}

//
// server Get() RPC handler.
//
func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	return nil
}

//
// server PutAppend() RPC handler.
//
func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
}

//
// add RPC handlers for any new RPCs that you include in your design.
//
