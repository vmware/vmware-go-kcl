package config

import (
	"time"
)

func newInitialPositionAtTimestamp(timestamp *time.Time) *InitialPositionInStreamExtended {
	return &InitialPositionInStreamExtended{Position: AT_TIMESTAMP, Timestamp: timestamp}
}

func newInitialPosition(position InitialPositionInStream) *InitialPositionInStreamExtended {
	return &InitialPositionInStreamExtended{Position: position, Timestamp: nil}
}
