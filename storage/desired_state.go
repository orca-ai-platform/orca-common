package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/orca-ai-platform/orca-common/models"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ADDED: Save desired resource state
func (p *PostgresClient) SaveDesiredResource(ctx context.Context, resource *models.DesiredResource) error {

	logger := log.FromContext(ctx)

	specJSON, err := json.Marshal(resource.Spec)
	if err != nil {
		return fmt.Errorf("failed to marshal spec: %w", err)
	}

	metadataJSON, err := json.Marshal(resource.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	var collectionIDParam interface{}
	if resource.CollectionID == "" {
		collectionIDParam = nil
	} else {
		collectionIDParam = resource.CollectionID
	}

	return p.executeInTransaction(ctx, func(tx *sql.Tx) error {
		// CHANGED: Upsert desired resource within transaction
		query := `
			INSERT INTO desired_resources (
				collection_id, kind, api_version, namespace, name, uid,
				spec, metadata, created_by, status
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (namespace, name, kind) DO UPDATE SET
				spec = EXCLUDED.spec,
				metadata = EXCLUDED.metadata,
				updated_at = NOW(),
				status = 'pending'
			RETURNING id
		`

		var resourceID int
		err = tx.QueryRowContext(ctx, query,
			collectionIDParam,
			resource.Kind,
			resource.APIVersion,
			resource.Namespace,
			resource.Name,
			resource.UID,
			specJSON,
			metadataJSON,
			resource.CreatedBy,
			"pending",
		).Scan(&resourceID)

		if err != nil {
			// ADDED: Return error to trigger rollback
			logger.Error(err, "Failed to upsert desired resource",
				"collectionID", resource.CollectionID,
				"namespace", resource.Namespace,
				"kind", resource.Kind,
				"name", resource.Name)
			return fmt.Errorf("failed to upsert desired resource: %w", err)
		}

		// ADDED: Update resource ID for subsequent operations
		resource.ID = resourceID

		// ADDED: Create initial change history entry within same transaction
		changeHistoryQuery := `
			INSERT INTO resource_changes (
				desired_resource_id, change_type, diff, applied_by, status
			)
			VALUES ($1, $2, $3, $4, $5)
		`

		// ADDED: Create simple diff for initial save
		diff := map[string]interface{}{
			"action": "created",
			"resource": map[string]interface{}{
				"kind":      resource.Kind,
				"namespace": resource.Namespace,
				"name":      resource.Name,
			},
		}
		diffJSON, err := json.Marshal(diff)
		if err != nil {
			return fmt.Errorf("failed to marshal change diff: %w", err)
		}

		_, err = tx.ExecContext(ctx, changeHistoryQuery,
			resourceID,
			"create",
			diffJSON,
			resource.CreatedBy,
			"pending",
		)

		if err != nil {
			// ADDED: Return error to trigger rollback
			logger.Error(err, "Failed to create change history, transaction will rollback",
				"resourceID", resourceID)
			return fmt.Errorf("failed to create change history: %w", err)
		}

		// ADDED: Log successful completion before commit
		logger.Info("Saved desired resource with change history",
			"id", resourceID,
			"kind", resource.Kind,
			"namespace", resource.Namespace,
			"name", resource.Name)

		return nil
	})
}

// ADDED: Get desired resource by ID
func (p *PostgresClient) GetDesiredResource(ctx context.Context, id int) (*models.DesiredResource, error) {
	query := `
        SELECT id, collection_id, kind, api_version, namespace, name, uid,
               spec, metadata, created_by, created_at, updated_at, status,
               last_applied_at, apply_error
        FROM desired_resources
        WHERE id = $1
    `

	resource := &models.DesiredResource{}
	var specJSON, metadataJSON []byte
	var lastAppliedAt sql.NullTime
	var applyError sql.NullString

	err := p.db.QueryRowContext(ctx, query, id).Scan(
		&resource.ID,
		&resource.CollectionID,
		&resource.Kind,
		&resource.APIVersion,
		&resource.Namespace,
		&resource.Name,
		&resource.UID,
		&specJSON,
		&metadataJSON,
		&resource.CreatedBy,
		&resource.CreatedAt,
		&resource.UpdatedAt,
		&resource.Status,
		&lastAppliedAt,
		&applyError,
	)

	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(specJSON, &resource.Spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal spec: %w", err)
	}

	if err := json.Unmarshal(metadataJSON, &resource.Metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	if lastAppliedAt.Valid {
		resource.LastAppliedAt = &lastAppliedAt.Time
	}

	if applyError.Valid {
		resource.ApplyError = applyError.String
	}

	return resource, nil
}

// ADDED: List desired resources for a collection
func (p *PostgresClient) ListDesiredResources(ctx context.Context, collectionID string) ([]*models.DesiredResource, error) {
	query := `
        SELECT id, collection_id, kind, api_version, namespace, name, uid,
               spec, metadata, created_by, created_at, updated_at, status,
               last_applied_at, apply_error
        FROM desired_resources
        WHERE collection_id = $1
        ORDER BY created_at DESC
    `

	rows, err := p.db.QueryContext(ctx, query, collectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resources := []*models.DesiredResource{}
	for rows.Next() {
		resource := &models.DesiredResource{}
		var specJSON, metadataJSON []byte
		var lastAppliedAt sql.NullTime
		var applyError sql.NullString

		err := rows.Scan(
			&resource.ID,
			&resource.CollectionID,
			&resource.Kind,
			&resource.APIVersion,
			&resource.Namespace,
			&resource.Name,
			&resource.UID,
			&specJSON,
			&metadataJSON,
			&resource.CreatedBy,
			&resource.CreatedAt,
			&resource.UpdatedAt,
			&resource.Status,
			&lastAppliedAt,
			&applyError,
		)

		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(specJSON, &resource.Spec); err != nil {
			return nil, fmt.Errorf("failed to unmarshal spec: %w", err)
		}

		if err := json.Unmarshal(metadataJSON, &resource.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		if lastAppliedAt.Valid {
			resource.LastAppliedAt = &lastAppliedAt.Time
		}

		if applyError.Valid {
			resource.ApplyError = applyError.String
		}

		resources = append(resources, resource)
	}

	return resources, nil
}

// ADDED: Delete desired resource
func (p *PostgresClient) DeleteDesiredResource(ctx context.Context, id int) error {
	query := `DELETE FROM desired_resources WHERE id = $1`
	_, err := p.db.ExecContext(ctx, query, id)
	return err
}

// ADDED: Save pending change
func (p *PostgresClient) SavePendingChange(ctx context.Context, change *models.PendingChange) error {

	logger := log.FromContext(ctx)

	// ADDED: Marshal JSON outside transaction to fail fast
	diffJSON, err := json.Marshal(change.Diff)
	if err != nil {
		return fmt.Errorf("failed to marshal diff: %w", err)
	}

	// ADDED: Use transaction wrapper for atomic operations
	return p.executeInTransaction(ctx, func(tx *sql.Tx) error {
		// CHANGED: Insert pending change within transaction
		query := `
			INSERT INTO pending_changes (
				desired_resource_id, change_type, diff, requested_by, status
			)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id
		`

		err = tx.QueryRowContext(ctx, query,
			change.DesiredResourceID,
			change.ChangeType,
			diffJSON,
			change.RequestedBy,
			"pending",
		).Scan(&change.ID)

		if err != nil {
			return fmt.Errorf("failed to insert pending change: %w", err)
		}

		// ADDED: Log successful completion before commit
		logger.Info("Saved pending change",
			"changeID", change.ID,
			"resourceID", change.DesiredResourceID,
			"changeType", change.ChangeType)

		return nil
	})
}

// ADDED: List pending changes
func (p *PostgresClient) ListPendingChanges(ctx context.Context, status string) ([]*models.PendingChange, error) {
	query := `
        SELECT id, desired_resource_id, change_type, diff, requested_by, 
               requested_at, status, reviewed_by, reviewed_at, review_comment
        FROM pending_changes
        WHERE status = $1
        ORDER BY requested_at DESC
    `

	rows, err := p.db.QueryContext(ctx, query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	changes := []*models.PendingChange{}
	for rows.Next() {
		change := &models.PendingChange{}
		var diffJSON []byte
		var reviewedBy sql.NullString
		var reviewedAt sql.NullTime
		var reviewComment sql.NullString

		err := rows.Scan(
			&change.ID,
			&change.DesiredResourceID,
			&change.ChangeType,
			&diffJSON,
			&change.RequestedBy,
			&change.RequestedAt,
			&change.Status,
			&reviewedBy,
			&reviewedAt,
			&reviewComment,
		)

		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(diffJSON, &change.Diff); err != nil {
			return nil, fmt.Errorf("failed to unmarshal diff: %w", err)
		}

		if reviewedBy.Valid {
			change.ReviewedBy = reviewedBy.String
		}
		if reviewedAt.Valid {
			change.ReviewedAt = &reviewedAt.Time
		}
		if reviewComment.Valid {
			change.ReviewComment = reviewComment.String
		}

		changes = append(changes, change)
	}

	return changes, nil
}

// ADDED: Update pending change status
func (p *PostgresClient) UpdatePendingChangeStatus(ctx context.Context, id int, status, reviewedBy, comment string) error {
	logger := log.FromContext(ctx)

	// ADDED: Use transaction wrapper for atomic operations
	return p.executeInTransaction(ctx, func(tx *sql.Tx) error {
		// CHANGED: Update pending change status within transaction
		query := `
			UPDATE pending_changes
			SET status = $1, reviewed_by = $2, reviewed_at = NOW(), review_comment = $3, updated_at = NOW()
			WHERE id = $4
		`

		result, err := tx.ExecContext(ctx, query, status, reviewedBy, comment, id)
		if err != nil {
			return fmt.Errorf("failed to update pending change status: %w", err)
		}

		// ADDED: Check if any rows were affected
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}

		if rowsAffected == 0 {
			return fmt.Errorf("pending change not found: %d", id)
		}

		// ADDED: Log successful completion before commit
		logger.Info("Updated pending change status",
			"changeID", id,
			"status", status,
			"reviewedBy", reviewedBy)

		return nil
	})
}

// ADDED: Save resource change history
func (p *PostgresClient) SaveResourceChange(ctx context.Context, change *models.ResourceChange) error {

	logger := log.FromContext(ctx)

	// ADDED: Marshal JSON outside transaction to fail fast
	diffJSON, err := json.Marshal(change.Diff)
	if err != nil {
		return fmt.Errorf("failed to marshal diff: %w", err)
	}

	// ADDED: Use transaction wrapper for atomic operations
	return p.executeInTransaction(ctx, func(tx *sql.Tx) error {
		// CHANGED: Insert resource change within transaction
		changeQuery := `
			INSERT INTO resource_changes (
				desired_resource_id, change_type, diff, applied_by, status, error_message
			)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING id
		`

		var changeID int
		err = tx.QueryRowContext(ctx, changeQuery,
			change.DesiredResourceID,
			change.ChangeType,
			diffJSON,
			change.AppliedBy,
			change.Status,
			change.ErrorMessage,
		).Scan(&changeID)

		if err != nil {
			return fmt.Errorf("failed to insert resource change: %w", err)
		}

		change.ID = changeID

		// ADDED: Update desired resource status based on change status
		updateQuery := `
			UPDATE desired_resources
			SET status = $1, last_applied_at = $2, apply_error = $3, updated_at = NOW()
			WHERE id = $4
		`

		var lastAppliedAt *time.Time
		var applyError *string

		if change.Status == "success" {
			now := time.Now()
			lastAppliedAt = &now
		} else if change.Status == "failed" {
			applyError = &change.ErrorMessage
		}

		_, err = tx.ExecContext(ctx, updateQuery,
			change.Status,
			lastAppliedAt,
			applyError,
			change.DesiredResourceID,
		)

		if err != nil {
			// ADDED: Return error to trigger rollback
			logger.Error(err, "Failed to update desired resource status, transaction will rollback",
				"changeID", changeID,
				"resourceID", change.DesiredResourceID)
			return fmt.Errorf("failed to update desired resource status: %w", err)
		}

		// ADDED: Log successful completion before commit
		logger.Info("Saved resource change and updated status",
			"changeID", changeID,
			"resourceID", change.DesiredResourceID,
			"status", change.Status)

		return nil
	})
}

// ADDED: Get latest change ID for a desired resource
func (p *PostgresClient) GetLatestChangeID(ctx context.Context, desiredResourceID int) (int, error) {
	query := `
        SELECT id
        FROM resource_changes
        WHERE desired_resource_id = $1
        ORDER BY applied_at DESC
        LIMIT 1
    `

	var changeID int
	err := p.db.QueryRowContext(ctx, query, desiredResourceID).Scan(&changeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("no changes found for desired resource %d", desiredResourceID)
		}
		return 0, fmt.Errorf("failed to get latest change ID: %w", err)
	}

	return changeID, nil
}

// ADDED: Get a specific resource change by ID
func (p *PostgresClient) GetResourceChange(ctx context.Context, changeID int) (*models.ResourceChange, error) {
	query := `
        SELECT id, desired_resource_id, change_type, diff, applied_by,
               applied_at, status, error_message
        FROM resource_changes
        WHERE id = $1
    `

	change := &models.ResourceChange{}
	var diffJSON []byte
	var errorMessage sql.NullString

	err := p.db.QueryRowContext(ctx, query, changeID).Scan(
		&change.ID,
		&change.DesiredResourceID,
		&change.ChangeType,
		&diffJSON,
		&change.AppliedBy,
		&change.AppliedAt,
		&change.Status,
		&errorMessage,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("change %d not found", changeID)
		}
		return nil, fmt.Errorf("failed to get resource change: %w", err)
	}

	// ADDED: Unmarshal diff JSON
	if err := json.Unmarshal(diffJSON, &change.Diff); err != nil {
		return nil, fmt.Errorf("failed to unmarshal diff: %w", err)
	}

	if errorMessage.Valid {
		change.ErrorMessage = errorMessage.String
	}

	return change, nil
}

// ADDED: Get change history for a resource
func (p *PostgresClient) GetResourceChangeHistory(ctx context.Context, namespace, name, kind string, limit int) ([]*models.ResourceChange, error) {
	query := `
        SELECT rc.id, rc.desired_resource_id, rc.change_type, rc.diff,
               rc.applied_by, rc.applied_at, rc.status, rc.error_message
        FROM resource_changes rc
        JOIN desired_resources dr ON rc.desired_resource_id = dr.id
        WHERE dr.namespace = $1 AND dr.name = $2 AND dr.kind = $3
        ORDER BY rc.applied_at DESC
        LIMIT $4
    `

	rows, err := p.db.QueryContext(ctx, query, namespace, name, kind, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query change history: %w", err)
	}
	defer rows.Close()

	changes := []*models.ResourceChange{}
	for rows.Next() {
		change := &models.ResourceChange{}
		var diffJSON []byte
		var errorMessage sql.NullString

		err := rows.Scan(
			&change.ID,
			&change.DesiredResourceID,
			&change.ChangeType,
			&diffJSON,
			&change.AppliedBy,
			&change.AppliedAt,
			&change.Status,
			&errorMessage,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan change: %w", err)
		}

		// ADDED: Unmarshal diff JSON
		if err := json.Unmarshal(diffJSON, &change.Diff); err != nil {
			return nil, fmt.Errorf("failed to unmarshal diff: %w", err)
		}

		if errorMessage.Valid {
			change.ErrorMessage = errorMessage.String
		}

		changes = append(changes, change)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating changes: %w", err)
	}

	return changes, nil
}

// ADDED: Update desired resource status after apply
func (p *PostgresClient) UpdateDesiredResourceStatus(ctx context.Context, id int, status string, applyError string) error {
	query := `
        UPDATE desired_resources
        SET status = $1,
            last_applied_at = NOW(),
            apply_error = $2,
            updated_at = NOW()
        WHERE id = $3
    `

	_, err := p.db.ExecContext(ctx, query, status, applyError, id)
	if err != nil {
		return fmt.Errorf("failed to update desired resource status: %w", err)
	}

	return nil
}
