package repository

import (
	"context"
	"fmt"
	"pulseguard/services/processing/internal/models"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgreRepository struct {
	pool *pgxpool.Pool
}

func NewPostgreRepository(pool *pgxpool.Pool) *PostgreRepository {
	return &PostgreRepository{
		pool: pool,
	}
}

func (pr *PostgreRepository) SaveNewIssues(ctx context.Context, issues []models.ErrorIssue) (map[uint64]uuid.UUID, error) {
	if len(issues) == 0 {
		return nil, fmt.Errorf("empty issue slice")
	}

	const query = ""
	res := make(map[uint64]uuid.UUID, len(issues))

	batch := &pgx.Batch{}
	for _, issue := range issues {
		batch.Queue(query, issue.ProjectId, issue.Fingerprint, issue.Title, issue.Status, issue.UpdatedAt)
	}
	br := pr.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(issues); i++ {
		var id uuid.UUID
		var fingerprint uint64
		if err := br.QueryRow().Scan(&id, &fingerprint); err != nil {
			return nil, fmt.Errorf("id and fingerprint data read error: %w", err)
		}
		res[fingerprint] = id
	}

	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("error closing batch results: %w", err)
	}

	return res, nil
}

func (pr *PostgreRepository) SaveErrorEvents(ctx context.Context, events []models.ErrorEvent) error {
	if len(events) == 0 {
		return fmt.Errorf("empty events slice")
	}

	const query = ""
	tableName := pgx.Identifier{"error_event"}
	columns := []string{
		"issues_id",
		"level",
		"payload",
		"timestamp",
		"received_at",
	}

	copySource := pgx.CopyFromSlice(len(events), func(i int) ([]any, error) {
		return []any{
			events[i].IssueId,
			events[i].Level,
			events[i].Payload,
			events[i].TimeStamp,
			events[i].ReceivedAt,
		}, nil
	})

	_, err := pr.pool.CopyFrom(ctx, tableName, columns, copySource)

	if err != nil {
		return fmt.Errorf("failed to bulk insert error_event: %w", err)
	}

	return nil
}
