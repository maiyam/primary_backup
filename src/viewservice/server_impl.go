package viewservice

//
// additions to ViewServer state.
//
type ViewServerImpl struct {
}

//
// your vs.impl.* initializations here.
//
func (vs *ViewServer) initImpl() {
}

//
// server Ping() RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
}
