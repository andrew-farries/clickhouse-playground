package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

const (
	startTime          = "2023-03-01 00:00:00"
	endTime            = "2023-04-01 00:00:00"
	interval           = 5 * time.Minute
	initialRecordCount = 120
	branchCount        = 10000
	workspaceCount     = 1000
)

func main() {
	conn, err := connect()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ctx := context.Background()

	if err := dropTable(ctx, conn); err != nil {
		log.Fatalf("Failed to drop table: %s", err)
	}
	if err := createSchema(ctx, conn); err != nil {
		log.Fatalf("Failed to create schema: %s", err)
	}

	workspaceIds := fillSlice(uuid.NewString, workspaceCount)
	branchIDs := fillSlice(uuid.NewString, branchCount)

	usagePerBranch := make(map[string]int)
	for _, id := range branchIDs {
		usagePerBranch[id] = initialRecordCount
	}

	// workspaceId -> branchIDs
	branchesPerWs := make(map[string][]string)
	// Assign branches to workspaces at random
	for _, branchId := range branchIDs {
		workspaceId := workspaceIds[rand.Intn(workspaceCount)]
		branchesPerWs[workspaceId] = append(branchesPerWs[workspaceId], branchId)
	}

	// Generate sample data
	start, _ := time.Parse("2006-01-02 15:04:05", startTime)
	end, _ := time.Parse("2006-01-02 15:04:05", endTime)
	currentTime := start

	for currentTime.Before(end) {
		log.Println(currentTime)

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO metrics")
		if err != nil {
			log.Fatalf("Failed to prepare batch: %s", err)
		}

		for _, workspaceId := range workspaceIds {
			branches := branchesPerWs[workspaceId]
			for _, branchId := range branches {
				// Generate a random value between 0 and 50 to add or subtract from the record count
				change := rand.Intn(50)
				currentUsage := usagePerBranch[branchId]
				if rand.Intn(10) == 0 {
					currentUsage -= change
				} else {
					currentUsage += change
				}
				if currentUsage < 0 {
					currentUsage = 0
				}
				usagePerBranch[branchId] = currentUsage

				if err := batch.Append(
					currentTime,
					workspaceId,
					branchId,
					uint32(currentUsage),
				); err != nil {
					log.Fatalf("Failed to append to batch: %s", err)
				}
			}
		}

		if err := batch.Send(); err != nil {
			log.Fatalf("Failed to send batch: %s", err)
		}

		currentTime = currentTime.Add(interval)
	}

	log.Println("Data generation complete")
}

func dropTable(ctx context.Context, conn driver.Conn) error {
	return conn.Exec(ctx,
		`DROP TABLE IF EXISTS metrics`)
}

func createSchema(ctx context.Context, conn driver.Conn) error {
	return conn.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS metrics
	("time" DateTime, "workspaceId" String, "branchId" String, "value" UInt32)
	ENGINE = MergeTree
	PRIMARY KEY (workspaceId, branchId, toStartOfHour(time), time)
	ORDER BY (workspaceId, branchId, toStartOfHour(time), time)
	TTL time + INTERVAL 1 DAY
	GROUP BY workspaceId, branchId, toStartOfHour(time)
	SET value = avg(value);
	`)
}
