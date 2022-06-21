package checkpoint

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	"github.com/vmware/vmware-go-kcl/clientlibrary/database"
	"github.com/vmware/vmware-go-kcl/clientlibrary/database/models"
	par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
)

const (
	NumMaxRetriesPostgress = 5
)

type PostgresCheckpoint struct {
	log                     zerolog.Logger
	TableName               string
	streamName              string
	leaseTableReadCapacity  int64
	leaseTableWriteCapacity int64
	LeaseDuration           int
	kclConfig               *config.KinesisClientLibConfiguration
	Retries                 int
	Datastore               database.ConsumerDatastore
	lastLeaseSync           time.Time
}

func (c *PostgresCheckpoint) GetLease(shard *par.ShardStatus, assignTo string) error {
	ck, _ := c.Datastore.GetCheckpoint(shard.ID, c.streamName)
	if ck == nil {
		return nil
	}

	leaseTimeout := time.Now().Add(time.Duration(c.LeaseDuration) * time.Millisecond).UTC()

	if ck.LeaseTimeout != nil && ck.LeaseOwner != "" {
		if time.Now().UTC().Before(*ck.LeaseTimeout) && ck.LeaseOwner != assignTo {
			return errors.New("unable to acquire lease: lease is already held by another node")
		}
	}

	ret := &models.Checkpoint{
		ShardID:      shard.ID,
		StreamName:   c.streamName,
		LeaseOwner:   assignTo,
		LeaseTimeout: &leaseTimeout,
	}

	if len(shard.ParentShardId) > 0 {
		ret.ParentID = shard.ParentShardId
	}

	if shard.Checkpoint != "" {
		ret.SequenceNumber = shard.Checkpoint
	}

	err := c.Datastore.SaveCheckpoint(ret, ck.LeaseTimeout == nil || ck.LeaseOwner == "")
	if err != nil {
		log.Error().Err(err).Msgf("unable to acquire lease: %s", err)
		return err
	}

	shard.Mux.Lock()
	shard.AssignedTo = assignTo
	shard.LeaseTimeout = leaseTimeout
	shard.Mux.Unlock()

	return nil
}

func (c *PostgresCheckpoint) CheckpointSequence(shard *par.ShardStatus) error {
	ret := &models.Checkpoint{
		ShardID:        shard.ID,
		StreamName:     c.streamName,
		SequenceNumber: shard.Checkpoint,
		LeaseOwner:     shard.AssignedTo,
		LeaseTimeout:   &shard.LeaseTimeout,
	}

	if len(shard.ParentShardId) > 0 {
		ret.ParentID = shard.ParentShardId
	}

	return c.Datastore.SaveCheckpoint(ret, false)
}

func (c *PostgresCheckpoint) FetchCheckpoint(shard *par.ShardStatus) error {
	ck, err := c.Datastore.GetCheckpoint(shard.ID, c.streamName)
	if err != nil {
		log.Error().Err(err).Msgf("unable to fetch checkpoint: %s", err)
		return err
	}

	if ck == nil {
		return ErrSequenceIDNotFound
	}

	log.Printf("Retrieved Shard Iterator %s\n", ck.SequenceNumber)

	shard.Mux.Lock()
	defer shard.Mux.Unlock()

	shard.Checkpoint = ck.SequenceNumber

	if ck.LeaseOwner != "" {
		shard.AssignedTo = ck.LeaseOwner
	}

	return nil
}

func (c *PostgresCheckpoint) RemoveLeaseInfo(shardID string) error {
	err := c.Datastore.RemoveCheckpoint(shardID, c.streamName)
	if err != nil {
		log.Error().Err(err).Msgf("unable to remove lease info for shard: %s, stream: %s: %s", shardID, c.streamName, err)
		return err
	}

	log.Printf("Lease info for shard: %s has been removed.\n", shardID)

	return nil
}

func (c *PostgresCheckpoint) RemoveLeaseOwner(shardID string) error {
	err := c.Datastore.UnassignCheckpointLease(shardID, c.streamName)
	if err != nil {
		log.Error().Err(err).Msgf("unable to remove lease owner for shard: %s, stream: %s: %s", shardID, c.streamName, err)
		return err
	}

	return nil
}

// ListActiveWorkers returns a map of workers and their shards
func (checkpointer *PostgresCheckpoint) ListActiveWorkers(shardStatus map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error) {
	err := checkpointer.syncLeases(shardStatus)
	if err != nil {
		return nil, err
	}

	workers := map[string][]*par.ShardStatus{}
	for _, shard := range shardStatus {
		if shard.GetCheckpoint() == ShardEnd {
			continue
		}

		leaseOwner := shard.GetLeaseOwner()
		if leaseOwner == "" {
			log.Debug().Msgf("Shard Not Assigned Error. ShardID: %s, WorkerID: %s", shard.ID, checkpointer.kclConfig.WorkerID)
			return nil, ErrShardNotAssigned
		}
		if w, ok := workers[leaseOwner]; ok {
			workers[leaseOwner] = append(w, shard)
		} else {
			workers[leaseOwner] = []*par.ShardStatus{shard}
		}
	}
	return workers, nil
}

// ClaimShard places a claim request on a shard to signal a steal attempt
func (checkpointer *PostgresCheckpoint) ClaimShard(shard *par.ShardStatus, claimID string) error {
	err := checkpointer.FetchCheckpoint(shard)
	if err != nil && err != ErrSequenceIDNotFound {
		return err
	}
	leaseTimeoutString := shard.GetLeaseTimeout().Format(time.RFC3339)

	i := 5
	// TODO: timestamp condition should check '=' only
	// TODO what to be done with attribute_not_exists(ClaimRequest)
	whereClause := fmt.Sprintf("WHERE shard_id = %s AND lease_timeout >= %s", nextVal(&i), nextVal(&i))
	params := []string{shard.ID, leaseTimeoutString}

	checkPointRow := &models.Checkpoint{
		ShardID:        shard.ID,
		StreamName:     checkpointer.streamName,
		SequenceNumber: shard.Checkpoint,
		LeaseOwner:     shard.AssignedTo,
		LeaseTimeout:   &shard.LeaseTimeout,
	}
	if leaseOwner := shard.GetLeaseOwner(); leaseOwner == "" {
		whereClause += "AND (lease_owner is null or lease_owner = '')"
	} else {
		whereClause += fmt.Sprintf("and lease_owner = %s", nextVal(&i))
		params = append(params, shard.GetLeaseOwner())
	}

	// TODO: Not sure what should be the quivalent to 'Checkpoint' as an attribute as no such columns exists
	// if checkpoint := shard.GetCheckpoint(); checkpoint == "" {
	// 	conditionalExpression += " AND attribute_not_exists(Checkpoint)"
	// } else if checkpoint == ShardEnd {
	// 	conditionalExpression += " AND Checkpoint <> :checkpoint"
	// 	expressionAttributeValues[":checkpoint"] = &dynamodb.AttributeValue{S: aws.String(ShardEnd)}
	// } else {
	// 	conditionalExpression += " AND Checkpoint = :checkpoint"
	// 	expressionAttributeValues[":checkpoint"] = &dynamodb.AttributeValue{S: &checkpoint}
	// }

	if shard.ParentShardId == "" {
		whereClause += fmt.Sprintf("and parent_id != %s", nextVal(&i))
		params = append(params, shard.ParentShardId)
	} else {
		whereClause += "AND (parent_id is null or parent_id = '')"
	}
	return checkpointer.Datastore.UpdateCheckpoint(checkPointRow, &whereClause, params)
}

func (checkpointer *PostgresCheckpoint) syncLeases(shardStatus map[string]*par.ShardStatus) error {
	log := checkpointer.kclConfig.Logger

	if (checkpointer.lastLeaseSync.Add(time.Duration(checkpointer.kclConfig.LeaseSyncingTimeIntervalMillis) * time.Millisecond)).After(time.Now()) {
		return nil
	}

	checkpointer.lastLeaseSync = time.Now()

	results, err := checkpointer.Datastore.GetCheckpoints()
	if err != nil {
		log.Debugf("Error performing SyncLeases. Error: %+v ", err)
		return err
	}
	if results == nil {
		return nil
	}
	for _, result := range results {
		shardId := result.ShardID
		assignedTo := result.LeaseOwner
		checkpoint := result.SequenceNumber

		for _, shard := range shardStatus {
			if ok := (shard.ID == shardId); ok {
				shard.SetLeaseOwner(aws.StringValue(&assignedTo))
				shard.SetCheckpoint(aws.StringValue(&checkpoint))
			}
		}
	}
	log.Debugf("Lease sync completed. Next lease sync will occur in %s", time.Duration(checkpointer.kclConfig.LeaseSyncingTimeIntervalMillis)*time.Millisecond)
	return nil
}

func NewPostgresCheckpoint(kclConfig *config.KinesisClientLibConfiguration, db database.ConsumerDatastore) *PostgresCheckpoint {
	return &PostgresCheckpoint{
		log:                     log.Logger,
		TableName:               kclConfig.TableName,
		streamName:              kclConfig.StreamName,
		leaseTableReadCapacity:  int64(kclConfig.InitialLeaseTableReadCapacity),
		leaseTableWriteCapacity: int64(kclConfig.InitialLeaseTableWriteCapacity),
		LeaseDuration:           kclConfig.FailoverTimeMillis,
		kclConfig:               kclConfig,
		Retries:                 NumMaxRetriesPostgress,
		Datastore:               db,
	}
}

func (c *PostgresCheckpoint) Init() error {
	return nil
}

func nextVal(i *int) string {
	*i += 1
	return fmt.Sprintf("%s%d", "$", *i)
}
