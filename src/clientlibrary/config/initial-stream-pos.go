package config

import (
	"time"
)

func newInitialPositionAtTimestamp(timestamp *time.Time) *InitialPositionInStreamExtended {
	return &InitialPositionInStreamExtended{position: AT_TIMESTAMP, timestamp: timestamp}
}

func newInitialPosition(position InitialPositionInStream) *InitialPositionInStreamExtended {
	return &InitialPositionInStreamExtended{position: position, timestamp: nil}
}
