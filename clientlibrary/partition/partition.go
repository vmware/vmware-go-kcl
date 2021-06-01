/*
 * Copyright (c) 2018 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package worker

import (
	"sync"
	"time"

	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
)

type ShardStatus struct {
	ID            string
	ParentShardId string
	Checkpoint    string
	AssignedTo    string
	Mux           *sync.RWMutex
	LeaseTimeout  time.Time
	// Shard Range
	StartingSequenceNumber string
	// child shard doesn't have end sequence number
	EndingSequenceNumber string
	ClaimRequest         string
}

func (ss *ShardStatus) GetLeaseOwner() string {
	ss.Mux.RLock()
	defer ss.Mux.RUnlock()
	return ss.AssignedTo
}

func (ss *ShardStatus) SetLeaseOwner(owner string) {
	ss.Mux.Lock()
	defer ss.Mux.Unlock()
	ss.AssignedTo = owner
}

func (ss *ShardStatus) GetCheckpoint() string {
	ss.Mux.RLock()
	defer ss.Mux.RUnlock()
	return ss.Checkpoint
}

func (ss *ShardStatus) SetCheckpoint(c string) {
	ss.Mux.Lock()
	defer ss.Mux.Unlock()
	ss.Checkpoint = c
}

func (ss *ShardStatus) GetLeaseTimeout() time.Time {
	ss.Mux.Lock()
	defer ss.Mux.Unlock()
	return ss.LeaseTimeout
}

func (ss *ShardStatus) SetLeaseTimeout(timeout time.Time) {
	ss.Mux.Lock()
	defer ss.Mux.Unlock()
	ss.LeaseTimeout = timeout
}

func (ss *ShardStatus) IsClaimRequestExpired(kclConfig *config.KinesisClientLibConfiguration) bool {
	if leaseTimeout := ss.GetLeaseTimeout(); leaseTimeout.IsZero() {
		return false
	} else {
		return leaseTimeout.
			Before(time.Now().UTC().Add(time.Duration(-kclConfig.LeaseStealingClaimTimeoutMillis) * time.Millisecond))
	}
}
