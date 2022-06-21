package database

import (
	"context"
	"database/sql"

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
	UpdateCheckpoint(cp *models.Checkpoint, whereClause *string, whereClauseParams []string) error
}
