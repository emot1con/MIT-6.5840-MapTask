package lock

import (
	"math/rand/v2"
	"strconv"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	LockName string
	ClientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	lk.LockName = lockname
	lk.ClientID = strconv.FormatInt(rand.Int64(), 10)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, rep := lk.ck.Get(lk.LockName)
		if rep == rpc.OK && val == "" {
			if rep := lk.ck.Put(lk.LockName, lk.ClientID, ver); rep == rpc.OK {
				return
			}
		}

		if rep == rpc.ErrNoKey {
			if rep := lk.ck.Put(lk.LockName, lk.ClientID, 0); rep == rpc.OK {
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	if val, ver, _ := lk.ck.Get(lk.LockName); val == lk.ClientID {
		if rep := lk.ck.Put(lk.LockName, "", ver); rep == rpc.OK {
			return
		}
	}
}
