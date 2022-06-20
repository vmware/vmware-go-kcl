package models

import "time"

type Checkpoint struct {
	ShardID        string
	StreamName     string
	SequenceNumber string
	LeaseOwner     string
	ParentID       string
	LeaseTimeout   *time.Time
}
