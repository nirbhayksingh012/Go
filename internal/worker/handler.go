package worker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/snowstream/snowstream/internal/db"
)

// JobHandler is the function signature for executing a specific background job.
type JobHandler func(ctx context.Context, job *db.RefreshJob) error

// Builtin job type constants
const (
	JobTypeMaterializedViewRefresh = "materialized_view_refresh"
	JobTypeRetentionCleanup        = "retention_cleanup"
	JobTypeStreamCompaction        = "stream_compaction"
)

// DefaultMaterializedViewRefreshHandler creates a handler for refreshing materialized views.
func DefaultMaterializedViewRefreshHandler(logger *slog.Logger) JobHandler {
	return func(ctx context.Context, job *db.RefreshJob) error {
		if job.ViewID == nil {
			return fmt.Errorf("view_id is required for %s", JobTypeMaterializedViewRefresh)
		}
		logger.Info("executing materialized view refresh",
			slog.String("job_id", job.ID.String()),
			slog.String("view_id", job.ViewID.String()),
		)
		// In Phase 5 (analytics engine), this executes the view's query definition incrementally
		return nil
	}
}

// DefaultRetentionCleanupHandler creates a handler for cleaning up events past their retention policy.
func DefaultRetentionCleanupHandler(logger *slog.Logger) JobHandler {
	return func(ctx context.Context, job *db.RefreshJob) error {
		logger.Info("executing retention policy cleanup",
			slog.String("job_id", job.ID.String()),
		)
		return nil
	}
}
