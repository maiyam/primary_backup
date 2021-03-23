package pbservice

// In all data types that represent arguments to RPCs, field names
// must start with capital letters, otherwise RPC will break.

//
// additional state to include in arguments to PutAppend RPC.
//
const (
	Put             = "Put"
	Append          = "Append"
	BootstrapFailed = "Bootstrap Failed"
)

type Operation string

type PutAppendArgsImpl struct {
	Operation Operation
	OpID      int64
	LastOpID  int64
	ViewNum   uint
}

//
// additional state to include in arguments to Get RPC.
//
type GetArgsImpl struct {
	GetID     int64
	LastGetID int64
	ViewNum   uint
}

//
// for new RPCs that you add, declare types for arguments and reply.
//
type BootstrapBackupArgs struct {
	KVMap map[string]string
	PIds  map[int64]bool
	GIds  map[int64]bool
}

type BootstrapBackupReply struct {
	Error Err
}
