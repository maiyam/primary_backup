package viewservice

import (
	"errors"
)

//
// additions to ViewServer state.
//
type ViewServerImpl struct {
	CurrentView            *View
	PrimaryMissedPingCount int
	BackupMissedPingCount  int
	Candidates             map[string]int
	AckedView              uint
}

//
// your vs.impl.* initializations here.
//
func (vs *ViewServer) initImpl() {
	vs.impl.CurrentView = &View{0, "", ""}
	vs.impl.PrimaryMissedPingCount = 0
	vs.impl.BackupMissedPingCount = 0
	vs.impl.Candidates = make(map[string]int)
	vs.impl.AckedView = 0
}

//
// server Ping() RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	if vs.isdead() {
		return errors.New("Caught Ping when server is dead")
	}
	// fmt.Printf("Receiving Ping from %s : Viewnum %d\n", args.Me, args.Viewnum)
	vs.mu.Lock()
	// The very first ping when vs.viewnum is at 0
	if vs.impl.CurrentView.Primary == "" && args.Viewnum == vs.impl.CurrentView.Viewnum {

		vs.impl.CurrentView = &View{vs.impl.CurrentView.Viewnum + 1, args.Me, vs.impl.CurrentView.Backup}
		delete(vs.impl.Candidates, args.Me)
		reply.View = View{vs.impl.CurrentView.Viewnum, vs.impl.CurrentView.Primary, vs.impl.CurrentView.Backup}
		// fmt.Printf("Responding with this reply when its the very first call %v\n", reply.View)
		// fmt.Printf("And this is the  current view %v\n", vs.impl.CurrentView)
		vs.mu.Unlock()
		return nil
	}
	// fmt.Printf("Starting with current view %v\n", vs.impl.CurrentView)

	switch args.Me {

	// args.Me == Primary
	case vs.impl.CurrentView.Primary:
		vs.impl.PrimaryMissedPingCount = 0 // reset missed pings counter

		if args.Viewnum == vs.impl.CurrentView.Viewnum-1 {
			reply.View = View{vs.impl.CurrentView.Viewnum, vs.impl.CurrentView.Primary, vs.impl.CurrentView.Backup}
			break
		}
		if args.Viewnum != vs.impl.CurrentView.Viewnum {
			// Primary crashed! trigger view change
			// fmt.Printf("Primary out of synch likely crashed. Claims view num : %d\n", args.Viewnum)
			if vs.impl.CurrentView.Backup != "" && vs.impl.AckedView == vs.impl.CurrentView.Viewnum {
				// fmt.Printf("Primary crashed. Promoting Backup %s as the new Primary\n", vs.impl.CurrentView.Backup)
				vs.impl.CurrentView.Viewnum = vs.impl.CurrentView.Viewnum + 1
				vs.impl.CurrentView.Primary = vs.impl.CurrentView.Backup
				vs.impl.PrimaryMissedPingCount = vs.impl.BackupMissedPingCount
				vs.impl.CurrentView.Backup = getCandidateIfAny(vs.impl.Candidates)
				if vs.impl.CurrentView.Backup != "" {
					vs.impl.BackupMissedPingCount = vs.impl.Candidates[vs.impl.CurrentView.Backup]
					delete(vs.impl.Candidates, vs.impl.CurrentView.Backup)
					//Put former Primary in the list of future Candidates
					vs.impl.Candidates[args.Me] = 0
				} else {
					vs.impl.CurrentView.Backup = args.Me
					vs.impl.BackupMissedPingCount = 0
				}
			}

		} else { //  args.Viewnum == CurrentView.Viewnum
			vs.impl.AckedView = vs.impl.CurrentView.Viewnum
			// Check if there is room to promote candidate to being backup and hence trigger view change.
			if vs.impl.CurrentView.Backup == "" {
				vs.impl.CurrentView.Backup = getCandidateIfAny(vs.impl.Candidates)
				if vs.impl.CurrentView.Backup != "" {
					vs.impl.CurrentView.Viewnum = vs.impl.CurrentView.Viewnum + 1
					vs.impl.BackupMissedPingCount = vs.impl.Candidates[vs.impl.CurrentView.Backup]
					delete(vs.impl.Candidates, vs.impl.CurrentView.Backup)
				}
				// Likewise check if there is pending action necessary to trigger view change to demote a non-responsive backup.
			} else if vs.impl.BackupMissedPingCount >= 5 {
				// Invalidate the current backup.
				vs.impl.CurrentView.Viewnum = vs.impl.CurrentView.Viewnum + 1
				vs.impl.CurrentView.Backup = getCandidateIfAny(vs.impl.Candidates)
				if vs.impl.CurrentView.Backup != "" {
					vs.impl.BackupMissedPingCount = vs.impl.Candidates[vs.impl.CurrentView.Backup]
					delete(vs.impl.Candidates, vs.impl.CurrentView.Backup)
				} else {
					vs.impl.BackupMissedPingCount = 0
				}
			}
		}
		reply.View = View{vs.impl.CurrentView.Viewnum, vs.impl.CurrentView.Primary, vs.impl.CurrentView.Backup}
		break

	// args.Me == Backup
	case vs.impl.CurrentView.Backup:
		vs.impl.BackupMissedPingCount = 0 //reset missed pings counter for Backup

		if args.Viewnum == vs.impl.CurrentView.Viewnum-1 {
			reply.View = View{vs.impl.CurrentView.Viewnum, vs.impl.CurrentView.Primary, vs.impl.CurrentView.Backup}
			break
		}
		if args.Viewnum != vs.impl.CurrentView.Viewnum {
			//Backup crashed! Promote a candidate to become new backup
			// fmt.Printf("Backup out of synch. Likely crashed, says view is : %d\n", args.Viewnum)
			if vs.impl.AckedView == vs.impl.CurrentView.Viewnum {
				vs.impl.CurrentView.Viewnum = vs.impl.CurrentView.Viewnum + 1
				vs.impl.CurrentView.Backup = getCandidateIfAny(vs.impl.Candidates)
				if vs.impl.CurrentView.Backup != "" {
					vs.impl.BackupMissedPingCount = vs.impl.Candidates[vs.impl.CurrentView.Backup]
					delete(vs.impl.Candidates, vs.impl.CurrentView.Backup)
				}
				//Put former Backup in the list of future Candidates
				vs.impl.Candidates[args.Me] = 0
			}
		}
		reply.View = View{vs.impl.CurrentView.Viewnum, vs.impl.CurrentView.Primary, vs.impl.CurrentView.Backup}
		break

	default:
		if vs.impl.CurrentView.Backup == "" && vs.impl.AckedView == vs.impl.CurrentView.Viewnum {
			// + No current backup
			// Make this node a backup
			vs.impl.CurrentView = &View{vs.impl.CurrentView.Viewnum + 1, vs.impl.CurrentView.Primary, args.Me}
			vs.impl.BackupMissedPingCount = 0
			// move it out for candidates if it exists
			delete(vs.impl.Candidates, args.Me)

		} else {
			// Just add it to set of Candidates if it does not exist and reset missedping count to 0 if we
			// have already seen this node before
			vs.impl.Candidates[args.Me] = 0
		}
		reply.View = View{vs.impl.CurrentView.Viewnum, vs.impl.CurrentView.Primary, vs.impl.CurrentView.Backup}

	}

	vs.mu.Unlock()
	// fmt.Printf("Responding with this reply when ping is from primary / secondary %v\n", reply.View)
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	if vs.isdead() {
		return errors.New("Caught Get when server is dead")
	}
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = View{vs.impl.CurrentView.Viewnum, vs.impl.CurrentView.Primary, vs.impl.CurrentView.Backup}

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	if vs.isdead() {
		return
	}
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.impl.PrimaryMissedPingCount++
	vs.impl.BackupMissedPingCount++

	if vs.impl.PrimaryMissedPingCount >= 5 {
		if vs.impl.AckedView != vs.impl.CurrentView.Viewnum {
			// fmt.Printf("Current view unacked by Pimary. No view changes can be triggered. Lasted Acked View : %d \n", vs.impl.AckedView)
			return
		}
		// Promote a Backup if exists and is alive
		// Backup is dead as well. Go back to square one?
		if vs.impl.BackupMissedPingCount >= 5 {
			//Reset both primary and backup
			vs.impl.CurrentView = &View{vs.impl.CurrentView.Viewnum + 1, "", ""}
		} else {
			vs.impl.CurrentView.Viewnum = vs.impl.CurrentView.Viewnum + 1
			vs.impl.CurrentView.Primary = vs.impl.CurrentView.Backup
			vs.impl.PrimaryMissedPingCount = vs.impl.BackupMissedPingCount
			vs.impl.CurrentView.Backup = getCandidateIfAny(vs.impl.Candidates)
			if vs.impl.CurrentView.Backup != "" {
				vs.impl.BackupMissedPingCount = vs.impl.Candidates[vs.impl.CurrentView.Backup]
				delete(vs.impl.Candidates, vs.impl.CurrentView.Backup)
			} else {
				vs.impl.BackupMissedPingCount = 0
			}
		}

	} else if vs.impl.BackupMissedPingCount >= 5 {
		if vs.impl.AckedView != vs.impl.CurrentView.Viewnum {
			// fmt.Printf("Current view unacked by Pimary. No view changes can be triggered. Lasted Acked View : %d \n", vs.impl.AckedView)
			return
		}
		// Invalidate the current backup.
		vs.impl.CurrentView.Viewnum = vs.impl.CurrentView.Viewnum + 1
		vs.impl.CurrentView.Backup = getCandidateIfAny(vs.impl.Candidates)
		if vs.impl.CurrentView.Backup != "" {
			vs.impl.BackupMissedPingCount = vs.impl.Candidates[vs.impl.CurrentView.Backup]
			delete(vs.impl.Candidates, vs.impl.CurrentView.Backup)
		} else {
			vs.impl.BackupMissedPingCount = 0
		}
	}
}

func getCandidateIfAny(m map[string]int) string {
	for k := range m {
		return k
	}
	return ""
}
