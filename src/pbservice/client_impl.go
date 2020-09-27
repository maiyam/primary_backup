package pbservice

//
// additions to Clerk state.
//
type ClerkImpl struct {
}

//
// your ck.impl.* initializations here.
//
func (ck *Clerk) initImpl() {
}

//
// fetch a key's value from the current primary;
// if the key has never been set, return "".
// Get() must keep trying until either the
// primary replies with the value or the primary
// says the key doesn't exist, i.e., has never been Put().
//
func (ck *Clerk) Get(key string) string {
	return ""
}

//
// send a Put() or Append() RPC
// must keep trying until it succeeds.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
}
