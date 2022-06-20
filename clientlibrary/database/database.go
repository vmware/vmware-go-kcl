package database

import (
	"context"
	"database/sql"
	"time"

	"github.com/vmware/vmware-go-kcl/clientlibrary/database/models"
)

type Datastore interface {
	ServiceName() string
	GetDBStats() sql.DBStats
	PingContext(context.Context) error
	Close() error
}

type ConsumerDatastore interface {
	Datastore
	GetCheckpoint(shardID, streamName string) (*models.Checkpoint, error)
	SaveCheckpoint(*models.Checkpoint, bool) error
	RemoveCheckpoint(shardID, streamName string) error
	UnassignCheckpointLease(shardID, streamName string) error
	GetCheckpoints() ([]*models.Checkpoint, error)
	UpdateCheckpoint(cp *models.Checkpoint, whereClause *string) error
}
type Config struct {
	DSN            string
	ServiceName    string
	ServiceVersion string
	MaxIdleConns   int
	MaxOpenConns   int
	QueryTimeout   time.Duration
}
