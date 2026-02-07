package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/orca-ai-platform/orca-common/models"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type PostgresClient struct {
	db *sql.DB
}

// CollectionRecord represents a collection record in PostgreSQL
type CollectionRecord struct {
	ID              string
	Name            string
	Namespace       string
	AppName         string
	Environment     string
	Team            string
	Description     string
	Tags            []string
	ConfidenceScore int
	Status          string
	ResourceCount   int
	DiscoveredAt    time.Time
	UpdatedAt       time.Time
}

// CollectionHistoryRecord represents a change history entry for collection resources
type CollectionHistoryRecord struct {
	ID           int       `json:"id"`
	Timestamp    time.Time `json:"timestamp"`
	ChangeType   string    `json:"change_type"`
	ChangedBy    string    `json:"changed_by"`
	ResourceKind string    `json:"resource_kind"`
	ResourceName string    `json:"resource_name"`
	DiffSummary  string    `json:"diff_summary"`
	Status       string    `json:"status"`
}

// OrphanResourceRecord represents an orphaned resource in PostgreSQL
type OrphanResourceRecord struct {
	ID        int
	Kind      string
	Name      string
	Namespace string
	UID       string
	Reason    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type AuditLogRecord struct {
	ID           int                    `json:"id"`
	CollectionID string                 `json:"collection_id"`
	ResourceUID  string                 `json:"resource_uid"`
	Action       string                 `json:"action"`
	Actor        string                 `json:"actor"`
	Details      map[string]interface{} `json:"details"`
	CreatedAt    time.Time              `json:"created_at"`
}

func (p *PostgresClient) executeInTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	logger := log.FromContext(ctx)

	// ADDED: Begin transaction with context
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		logger.Error(err, "Failed to begin transaction")
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// ADDED: Ensure rollback on panic or error
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			logger.Error(fmt.Errorf("panic in transaction: %v", p), "Transaction panicked, rolled back")
			panic(p)
		}
	}()

	// ADDED: Execute the transaction function
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			logger.Error(rbErr, "Failed to rollback transaction", "originalError", err)
		} else {
			logger.Info("Transaction rolled back due to error", "error", err)
		}
		return err
	}

	// ADDED: Commit the transaction
	if err := tx.Commit(); err != nil {
		logger.Error(err, "Failed to commit transaction")
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logger.V(1).Info("Transaction committed successfully")
	return nil
}

// NewPostgresClient creates a new PostgreSQL client
func NewPostgresClient(connString string) (*PostgresClient, error) {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	client := &PostgresClient{db: db}

	// Initialize schema
	if err := client.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return client, nil
}

// initSchema creates the necessary tables if they don't exist
func (p *PostgresClient) initSchema() error {
	schema := `
	-- Collections table
	CREATE TABLE IF NOT EXISTS collections (
		id VARCHAR(255) PRIMARY KEY,
	    name VARCHAR(255) NOT NULL,
	    namespace VARCHAR(255) NOT NULL,
	    app_name VARCHAR(255),
	    environment VARCHAR(255),
	    team VARCHAR(255),
	    description TEXT,
	    tags TEXT[],
	    confidence_score INT NOT NULL,
	    status VARCHAR(50) NOT NULL,
	    resource_count INT DEFAULT 0,
	    discovered_at TIMESTAMP NOT NULL,
	    updated_at TIMESTAMP NOT NULL,
	    UNIQUE(namespace, name)
	);

	-- ADDED: Migration to add new columns to existing databases
	ALTER TABLE collections ADD COLUMN IF NOT EXISTS description TEXT;
	ALTER TABLE collections ADD COLUMN IF NOT EXISTS tags TEXT[];

	-- Collection resources table
	CREATE TABLE IF NOT EXISTS collection_resources (
        id SERIAL PRIMARY KEY,
        collection_id VARCHAR(255) NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
        kind VARCHAR(100) NOT NULL,
        name VARCHAR(255) NOT NULL,
        namespace VARCHAR(255) NOT NULL,
        uid VARCHAR(255) NOT NULL,
        is_primary BOOLEAN NOT NULL DEFAULT FALSE,
        resource_version VARCHAR(50),
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        UNIQUE(collection_id, kind, namespace, name)
    );

	CREATE INDEX IF NOT EXISTS idx_collection_resources_uid ON collection_resources(uid);
    CREATE INDEX IF NOT EXISTS idx_collection_resources_resource_version ON collection_resources(resource_version);

    -- Orphaned resources table
    CREATE TABLE IF NOT EXISTS orphan_resources (
        id SERIAL PRIMARY KEY,
        kind VARCHAR(100) NOT NULL,
        name VARCHAR(255) NOT NULL,
        namespace VARCHAR(255) NOT NULL,
        uid VARCHAR(255) NOT NULL,
        resource_version VARCHAR(50), -- ADDED: Track ResourceVersion for orphans too
        reason TEXT,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        UNIQUE(kind, namespace, name)
    );

	-- ADDED: Desired state storage (user's intended configuration)
    CREATE TABLE IF NOT EXISTS desired_resources (
        id SERIAL PRIMARY KEY,
        collection_id VARCHAR(255) REFERENCES collections(id) ON DELETE SET NULL,
        kind VARCHAR(100) NOT NULL,
        api_version VARCHAR(100) NOT NULL,
        namespace VARCHAR(255) NOT NULL,
        name VARCHAR(255) NOT NULL,
        uid VARCHAR(255), -- NULL for resources not yet created
        spec JSONB NOT NULL, -- Full resource specification
        metadata JSONB NOT NULL, -- Labels, annotations, etc.
        created_by VARCHAR(255) NOT NULL, -- User who created/edited
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, applied, drift, error
        last_applied_at TIMESTAMP,
        apply_error TEXT,
        UNIQUE(namespace, name, kind)
    );

    -- ADDED: Change history/audit log
    CREATE TABLE IF NOT EXISTS resource_changes (
        id SERIAL PRIMARY KEY,
        desired_resource_id INTEGER REFERENCES desired_resources(id) ON DELETE CASCADE,
        change_type VARCHAR(50) NOT NULL, -- create, update, delete
        diff JSONB NOT NULL, -- JSON patch or full diff
        applied_by VARCHAR(255) NOT NULL,
        applied_at TIMESTAMP NOT NULL DEFAULT NOW(),
        status VARCHAR(50) NOT NULL, -- success, failed, pending
        error_message TEXT
    );

    -- ADDED: Pending changes waiting for approval
    CREATE TABLE IF NOT EXISTS pending_changes (
        id SERIAL PRIMARY KEY,
        desired_resource_id INTEGER REFERENCES desired_resources(id) ON DELETE CASCADE,
        change_type VARCHAR(50) NOT NULL, -- create, update, delete
        diff JSONB NOT NULL,
        requested_by VARCHAR(255) NOT NULL,
        requested_at TIMESTAMP NOT NULL DEFAULT NOW(),
        status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, approved, rejected, applied
        reviewed_by VARCHAR(255),
        reviewed_at TIMESTAMP,
        review_comment TEXT
    );

	-- ADDED: Audit log table for tracking all API actions
    CREATE TABLE IF NOT EXISTS audit_log (
        id SERIAL PRIMARY KEY,
        collection_id VARCHAR(255),
        resource_uid VARCHAR(255),
        action VARCHAR(50) NOT NULL,
        actor VARCHAR(255) NOT NULL,
        details JSONB,
        created_at TIMESTAMP DEFAULT NOW()
    );

    -- ADDED: Indexes for audit_log efficient querying
    CREATE INDEX IF NOT EXISTS idx_audit_log_collection_id ON audit_log(collection_id);
    CREATE INDEX IF NOT EXISTS idx_audit_log_resource_uid ON audit_log(resource_uid);
    CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action);
    CREATE INDEX IF NOT EXISTS idx_audit_log_actor ON audit_log(actor);
    CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON audit_log(created_at);

	-- ADDED: Core indexes for foreign key relationships and common queries
    CREATE INDEX IF NOT EXISTS idx_desired_resources_collection ON desired_resources(collection_id);
    CREATE INDEX IF NOT EXISTS idx_desired_resources_status ON desired_resources(status);
    CREATE INDEX IF NOT EXISTS idx_desired_resources_uid ON desired_resources(uid);
    
    -- ADDED: Index for resource_changes.desired_resource_id (PRIMARY REQUIREMENT)
    -- Purpose: Speeds up queries fetching change history for a specific desired resource
    CREATE INDEX IF NOT EXISTS idx_resource_changes_desired_resource_id ON resource_changes(desired_resource_id);
    
    -- ADDED: Index for resource_changes.status
    -- Purpose: Speeds up queries filtering by change status (success, failed, pending)
    CREATE INDEX IF NOT EXISTS idx_resource_changes_status ON resource_changes(status);
    
    -- ADDED: Existing index for resource_changes.applied_at
    CREATE INDEX IF NOT EXISTS idx_resource_changes_applied_at ON resource_changes(applied_at);
    
    -- ADDED: Composite index for resource_changes time-series queries
    -- Purpose: Optimizes queries that filter by resource ID and order by time
    CREATE INDEX IF NOT EXISTS idx_resource_changes_desired_resource_applied_at ON resource_changes(desired_resource_id, applied_at DESC);
    
    -- ADDED: Index for resource_changes.applied_by
    -- Purpose: Speeds up audit queries filtering by user
    CREATE INDEX IF NOT EXISTS idx_resource_changes_applied_by ON resource_changes(applied_by);
    
    -- ADDED: Index for pending_changes.desired_resource_id (PRIMARY REQUIREMENT)
    -- Purpose: Speeds up queries fetching pending changes for a specific desired resource
    CREATE INDEX IF NOT EXISTS idx_pending_changes_desired_resource_id ON pending_changes(desired_resource_id);
    
    -- ADDED: Existing index for pending_changes.status
    CREATE INDEX IF NOT EXISTS idx_pending_changes_status ON pending_changes(status);
    
    -- ADDED: Collections indexes
    CREATE INDEX IF NOT EXISTS idx_collections_namespace ON collections(namespace);
    CREATE INDEX IF NOT EXISTS idx_collections_environment ON collections(environment);
    CREATE INDEX IF NOT EXISTS idx_collections_team ON collections(team);
    CREATE INDEX IF NOT EXISTS idx_collections_app_name ON collections(app_name);
    
    -- ADDED: Composite index on collections(namespace, environment)
    -- Purpose: Speeds up queries filtering by both namespace and environment
    CREATE INDEX IF NOT EXISTS idx_collections_namespace_environment ON collections(namespace, environment);
    
    -- NOTE: UNIQUE(namespace, name) constraint on collections already creates an implicit index
    -- No separate composite index needed for (namespace, name)
    
    -- ADDED: Collection resources indexes
    CREATE INDEX IF NOT EXISTS idx_collection_resources_collection_id ON collection_resources(collection_id);
    CREATE INDEX IF NOT EXISTS idx_collection_resources_uid ON collection_resources(uid);
    CREATE INDEX IF NOT EXISTS idx_collection_resources_resource_version ON collection_resources(resource_version);
    
    -- ADDED: Composite index for collection_resources namespace+kind queries
    -- Purpose: Speeds up queries finding resources of specific kind in a namespace
    CREATE INDEX IF NOT EXISTS idx_collection_resources_namespace_kind ON collection_resources(namespace, kind);
    
    -- ADDED: Orphan resources indexes
    CREATE INDEX IF NOT EXISTS idx_orphan_resources_namespace ON orphan_resources(namespace);
    CREATE INDEX IF NOT EXISTS idx_orphan_resources_uid ON orphan_resources(uid);
    `

	_, err := p.db.Exec(schema)
	return err
}

// SaveCollection saves or updates a collection in PostgreSQL
func (p *PostgresClient) SaveCollection(ctx context.Context, collection *models.Collection) error {
	logger := log.FromContext(ctx)

	return p.executeInTransaction(ctx, func(tx *sql.Tx) error {

		// CHANGED: Use ON CONFLICT on the actual unique constraint (namespace, name)
		// The original code used ON CONFLICT (id) which doesn't handle the
		// UNIQUE(namespace, name) constraint, causing race condition errors
		query := `
			INSERT INTO collections (
				id, name, namespace, app_name, environment, team,
				confidence_score, status, resource_count, discovered_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT (namespace, name) DO UPDATE SET
				app_name = EXCLUDED.app_name,
				environment = CASE 
	        	    WHEN collections.environment = 'unknown' THEN EXCLUDED.environment 
	        	    ELSE collections.environment 
	        	END,
	        	team = CASE 
	        	    WHEN collections.team = 'unknown' THEN EXCLUDED.team 
	        	    ELSE collections.team 
	        	END,
				confidence_score = EXCLUDED.confidence_score,
				status = EXCLUDED.status,
				resource_count = EXCLUDED.resource_count,
				updated_at = EXCLUDED.updated_at
			RETURNING id
		`

		// ADDED: Get the actual ID that was inserted/updated
		var actualID string
		err := tx.QueryRowContext(ctx, query,
			collection.ID,
			collection.Name,
			collection.Namespace,
			collection.AppName,
			collection.Environment,
			collection.Team,
			collection.ConfidenceScore,
			string(collection.Status),
			len(collection.Resources),
			collection.DiscoveredAt,
			time.Now(),
		).Scan(&actualID)

		if err != nil {
			// ADDED: Log detailed error information
			logger.Error(err, "Failed to upsert collection",
				"id", collection.ID,
				"namespace", collection.Namespace,
				"name", collection.Name)
			return fmt.Errorf("failed to upsert collection: %w", err)
		}

		// ADDED: If the returned ID differs, it means we updated an existing collection
		if actualID != collection.ID {
			logger.Info("Collection already existed with different ID, used existing ID",
				"requested_id", collection.ID,
				"actual_id", actualID,
				"namespace", collection.Namespace,
				"name", collection.Name)
			collection.ID = actualID
		}

		logger.V(1).Info("Upserted collection successfully",
			"id", actualID,
			"namespace", collection.Namespace,
			"name", collection.Name)

		// CHANGED: Delete old resources within transaction using the actual collection ID
		deleteQuery := "DELETE FROM collection_resources WHERE collection_id = $1"
		result, err := tx.ExecContext(ctx, deleteQuery, actualID)
		if err != nil {
			return fmt.Errorf("failed to delete old resources: %w", err)
		}

		// ADDED: Log how many resources were deleted
		if deletedRows, err := result.RowsAffected(); err == nil && deletedRows > 0 {
			logger.V(1).Info("Deleted old resources",
				"count", deletedRows,
				"collection_id", actualID)
		}

		// CHANGED: Insert new resources within transaction
		resourceQuery := `
			INSERT INTO collection_resources (
				collection_id, kind, name, namespace, uid, is_primary, 
				resource_version, created_at, updated_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		`

		insertedCount := 0
		for _, resource := range collection.Resources {
			isPrimary := collection.PrimaryResource != nil && resource.Ref.Key() == collection.PrimaryResource.Ref.Key()

			_, err = tx.ExecContext(ctx, resourceQuery,
				actualID, // CHANGED: Use actualID instead of collection.ID
				resource.Ref.Kind,
				resource.Ref.Name,
				resource.Ref.Namespace,
				resource.Ref.UID,
				isPrimary,
				resource.ResourceVersion,
			)
			if err != nil {
				// CHANGED: Log error with full context and return to trigger rollback
				logger.Error(err, "Failed to insert resource, transaction will rollback",
					"collection_id", actualID,
					"collection_namespace", collection.Namespace,
					"collection_name", collection.Name,
					"resource", resource.Ref.Key(),
					"resourceKind", resource.Ref.Kind,
					"resourceUID", resource.Ref.UID)
				return fmt.Errorf("failed to insert resource %s: %w", resource.Ref.Key(), err)
			}
			insertedCount++
		}

		// ADDED: Log successful completion with full details
		logger.Info("Saved collection to PostgreSQL",
			"id", actualID,
			"name", collection.Name,
			"namespace", collection.Namespace,
			"resources_inserted", insertedCount,
			"total_resources", len(collection.Resources))

		return nil
	})
}

// SaveOrphanResources saves orphaned resources to PostgreSQL
func (p *PostgresClient) SaveOrphanResources(ctx context.Context, orphans []*models.ResourceNode) error {
	logger := log.FromContext(ctx)

	if len(orphans) == 0 {
		return nil
	}

	return p.executeInTransaction(ctx, func(tx *sql.Tx) error {
		// CHANGED: Clear old orphans within transaction
		_, err := tx.ExecContext(ctx, "DELETE FROM orphan_resources")
		if err != nil {
			return fmt.Errorf("failed to clear old orphans: %w", err)
		}

		// CHANGED: Insert new orphans within transaction
		query := `
			INSERT INTO orphan_resources (
				kind, name, namespace, uid, resource_version, reason, 
				created_at, updated_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
		`

		for _, orphan := range orphans {
			reason := p.getOrphanReason(orphan)

			_, err = tx.ExecContext(ctx, query,
				orphan.Ref.Kind,
				orphan.Ref.Name,
				orphan.Ref.Namespace,
				orphan.Ref.UID,
				orphan.ResourceVersion,
				reason,
			)
			if err != nil {
				// CHANGED: Log and return error to trigger rollback instead of continue
				logger.Error(err, "Failed to insert orphan resource, transaction will rollback",
					"resource", orphan.Ref.Key())
				return fmt.Errorf("failed to insert orphan resource: %w", err)
			}
		}

		// ADDED: Log successful completion before commit
		logger.Info("Saved orphan resources to PostgreSQL", "count", len(orphans))

		return nil
	})
}

// Get stored ResourceVersion for a resource by UID
func (p *PostgresClient) GetResourceVersion(ctx context.Context, uid string) (string, error) {
	query := `
        SELECT resource_version 
        FROM collection_resources 
        WHERE uid = $1
        UNION
        SELECT resource_version 
        FROM orphan_resources 
        WHERE uid = $1
        LIMIT 1
    `

	var resourceVersion sql.NullString
	err := p.db.QueryRowContext(ctx, query, uid).Scan(&resourceVersion)

	if err == sql.ErrNoRows {
		// ADDED: Resource not found in DB, this is a new resource
		return "", nil
	}

	if err != nil {
		return "", fmt.Errorf("failed to query resource version: %w", err)
	}

	if !resourceVersion.Valid {
		return "", nil
	}

	return resourceVersion.String, nil
}

// getOrphanReason determines why a resource is orphaned
func (p *PostgresClient) getOrphanReason(resource *models.ResourceNode) string {
	reasons := []string{}

	if resource.OwnedBy == nil && len(resource.OwnerReferences) == 0 {
		reasons = append(reasons, "no_owner")
	}

	if len(resource.ReferencedBy) == 0 {
		reasons = append(reasons, "not_referenced")
	}

	if len(resource.Labels) == 0 {
		reasons = append(reasons, "no_labels")
	}

	if len(reasons) == 0 {
		return "unknown"
	}

	return strings.Join(reasons, ", ")
}

// GetCollectionsByEnvironment retrieves collections filtered by environment
func (p *PostgresClient) GetCollectionsByEnvironment(ctx context.Context, environment string) ([]*CollectionRecord, error) {
	query := `
		SELECT id, name, namespace, app_name, environment, team,
		       confidence_score, status, resource_count, discovered_at, updated_at
		FROM collections
		WHERE environment = $1
		ORDER BY updated_at DESC
	`

	rows, err := p.db.QueryContext(ctx, query, environment)
	if err != nil {
		return nil, fmt.Errorf("failed to query collections: %w", err)
	}
	defer rows.Close()

	collections := []*CollectionRecord{}
	for rows.Next() {
		col := &CollectionRecord{}
		err := rows.Scan(
			&col.ID, &col.Name, &col.Namespace, &col.AppName, &col.Environment,
			&col.Team, &col.ConfidenceScore, &col.Status, &col.ResourceCount,
			&col.DiscoveredAt, &col.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan collection: %w", err)
		}
		collections = append(collections, col)
	}

	return collections, nil
}

// GetCollectionsByTeam retrieves collections filtered by team
func (p *PostgresClient) GetCollectionsByTeam(ctx context.Context, team string) ([]*CollectionRecord, error) {
	query := `
		SELECT id, name, namespace, app_name, environment, team,
		       confidence_score, status, resource_count, discovered_at, updated_at
		FROM collections
		WHERE team = $1
		ORDER BY updated_at DESC
	`

	rows, err := p.db.QueryContext(ctx, query, team)
	if err != nil {
		return nil, fmt.Errorf("failed to query collections: %w", err)
	}
	defer rows.Close()

	collections := []*CollectionRecord{}
	for rows.Next() {
		col := &CollectionRecord{}
		err := rows.Scan(
			&col.ID, &col.Name, &col.Namespace, &col.AppName, &col.Environment,
			&col.Team, &col.ConfidenceScore, &col.Status, &col.ResourceCount,
			&col.DiscoveredAt, &col.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan collection: %w", err)
		}
		collections = append(collections, col)
	}

	return collections, nil
}

// GetOrphanResources retrieves all orphaned resources
func (p *PostgresClient) GetOrphanResources(ctx context.Context) ([]*OrphanResourceRecord, error) {
	query := `
		SELECT id, kind, name, namespace, uid, reason, created_at, updated_at
		FROM orphan_resources
		ORDER BY created_at DESC
	`

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query orphan resources: %w", err)
	}
	defer rows.Close()

	orphans := []*OrphanResourceRecord{}
	for rows.Next() {
		orphan := &OrphanResourceRecord{}
		err := rows.Scan(
			&orphan.ID, &orphan.Kind, &orphan.Name, &orphan.Namespace,
			&orphan.UID, &orphan.Reason, &orphan.CreatedAt, &orphan.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan orphan resource: %w", err)
		}
		orphans = append(orphans, orphan)
	}

	return orphans, nil
}

// Close closes the database connection
func (p *PostgresClient) Close() error {
	return p.db.Close()
}

func (p *PostgresClient) ListCollections(ctx context.Context, page, pageSize int, environment, team, namespace string, minConfidence int) ([]*CollectionRecord, int, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 50
	}
	// ADDED: Limit maximum page size to prevent excessive memory usage
	if pageSize > 1000 {
		pageSize = 1000
	}

	offset := (page - 1) * pageSize

	// ADDED: Build WHERE clause with filters - shared between count and list queries
	whereClause := "WHERE 1=1"
	args := []interface{}{}
	argIndex := 1

	if environment != "" {
		whereClause += fmt.Sprintf(" AND environment = $%d", argIndex)
		args = append(args, environment)
		argIndex++
	}

	if team != "" {
		whereClause += fmt.Sprintf(" AND team = $%d", argIndex)
		args = append(args, team)
		argIndex++
	}

	if namespace != "" {
		whereClause += fmt.Sprintf(" AND namespace = $%d", argIndex)
		args = append(args, namespace)
		argIndex++
	}

	if minConfidence > 0 {
		whereClause += fmt.Sprintf(" AND confidence_score >= $%d", argIndex)
		args = append(args, minConfidence)
		argIndex++
	}

	// ADDED: Get total count with same filters - simplified query
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM collections %s", whereClause)
	var total int
	err := p.db.QueryRowContext(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get total count: %w", err)
	}

	// ADDED: Handle edge case - page beyond total results
	if total == 0 {
		// CHANGED: Return empty result set with total count 0
		return []*CollectionRecord{}, 0, nil
	}

	// ADDED: Build list query with same WHERE clause
	listQuery := fmt.Sprintf(`
        SELECT id, name, namespace, app_name, environment, team,
               confidence_score, status, resource_count, discovered_at, updated_at
        FROM collections
        %s
        ORDER BY updated_at DESC
        LIMIT $%d OFFSET $%d
    `, whereClause, argIndex, argIndex+1)

	// ADDED: Append pagination parameters
	listArgs := append(args, pageSize, offset)

	rows, err := p.db.QueryContext(ctx, listQuery, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query collections: %w", err)
	}
	defer rows.Close()

	collections := []*CollectionRecord{}
	for rows.Next() {
		col := &CollectionRecord{}
		err := rows.Scan(
			&col.ID, &col.Name, &col.Namespace, &col.AppName, &col.Environment,
			&col.Team, &col.ConfidenceScore, &col.Status, &col.ResourceCount,
			&col.DiscoveredAt, &col.UpdatedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan collection: %w", err)
		}
		collections = append(collections, col)
	}

	// ADDED: Check for row iteration errors
	if err = rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error iterating collections: %w", err)
	}

	return collections, total, nil
}

func (p *PostgresClient) CountCollections(ctx context.Context, environment, team, namespace string, minConfidence int) (int, error) {
	// ADDED: Build WHERE clause matching ListCollections
	whereClause := "WHERE 1=1"
	args := []interface{}{}
	argIndex := 1

	if environment != "" {
		whereClause += fmt.Sprintf(" AND environment = $%d", argIndex)
		args = append(args, environment)
		argIndex++
	}

	if team != "" {
		whereClause += fmt.Sprintf(" AND team = $%d", argIndex)
		args = append(args, team)
		argIndex++
	}

	if namespace != "" {
		whereClause += fmt.Sprintf(" AND namespace = $%d", argIndex)
		args = append(args, namespace)
		argIndex++
	}

	if minConfidence > 0 {
		whereClause += fmt.Sprintf(" AND confidence_score >= $%d", argIndex)
		args = append(args, minConfidence)
		argIndex++
	}

	// ADDED: Execute count query
	query := fmt.Sprintf("SELECT COUNT(*) FROM collections %s", whereClause)
	var count int
	err := p.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count collections: %w", err)
	}

	return count, nil
}

// ADDED: Method to get collection by ID
func (p *PostgresClient) GetCollectionByID(ctx context.Context, id string) (*CollectionRecord, error) {
	query := `
        SELECT id, name, namespace, app_name, environment, team,
               description, tags,
               confidence_score, status, resource_count, discovered_at, updated_at
        FROM collections
        WHERE id = $1
    `

	col := &CollectionRecord{}
	// CHANGED: Use sql.NullString for nullable columns
	var description sql.NullString
	var tags []string

	err := p.db.QueryRowContext(ctx, query, id).Scan(
		&col.ID, &col.Name, &col.Namespace, &col.AppName, &col.Environment,
		&col.Team, &description, pq.Array(&tags), &col.ConfidenceScore, &col.Status, &col.ResourceCount,
		&col.DiscoveredAt, &col.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	// ADDED: Convert nullable fields
	if description.Valid {
		col.Description = description.String
	}
	if tags != nil {
		col.Tags = tags
	}

	return col, nil
}

// ADDED: Additional storage methods for API endpoints
// CollectionResourceRecord represents a resource within a collection
type CollectionResourceRecord struct {
	ID        int
	Kind      string
	Name      string
	Namespace string
	UID       string
	IsPrimary bool
}

func (p *PostgresClient) GetCollectionResources(ctx context.Context, collectionID string) ([]*CollectionResourceRecord, error) {
	query := `
        SELECT id, kind, name, namespace, uid, is_primary
        FROM collection_resources
        WHERE collection_id = $1
        ORDER BY is_primary DESC, kind, name
    `

	rows, err := p.db.QueryContext(ctx, query, collectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resources := []*CollectionResourceRecord{}
	for rows.Next() {
		res := &CollectionResourceRecord{}
		err := rows.Scan(&res.ID, &res.Kind, &res.Name, &res.Namespace, &res.UID, &res.IsPrimary)
		if err != nil {
			return nil, err
		}
		resources = append(resources, res)
	}

	return resources, nil
}

// ADDED: Method to get filtered orphan resources
func (p *PostgresClient) GetOrphanResourcesFiltered(ctx context.Context, namespace, kind string) ([]*OrphanResourceRecord, error) {
	query := `
        SELECT id, kind, name, namespace, uid, reason, created_at, updated_at
        FROM orphan_resources
        WHERE 1=1
    `
	args := []interface{}{}
	argIndex := 1

	if namespace != "" {
		query += fmt.Sprintf(" AND namespace = $%d", argIndex)
		args = append(args, namespace)
		argIndex++
	}

	if kind != "" {
		query += fmt.Sprintf(" AND kind = $%d", argIndex)
		args = append(args, kind)
		argIndex++
	}

	query += " ORDER BY created_at DESC"

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orphans := []*OrphanResourceRecord{}
	for rows.Next() {
		orphan := &OrphanResourceRecord{}
		err := rows.Scan(
			&orphan.ID, &orphan.Kind, &orphan.Name, &orphan.Namespace,
			&orphan.UID, &orphan.Reason, &orphan.CreatedAt, &orphan.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		orphans = append(orphans, orphan)
	}

	return orphans, nil
}

// ADDED: Statistics-related structs and methods
type Statistics struct {
	TotalCollections       int
	TotalResources         int
	TotalOrphans           int
	HighConfidenceCount    int
	MediumConfidenceCount  int
	LowConfidenceCount     int
	ResourcesByKind        map[string]int
	CollectionsByNamespace map[string]int
}

type EnvironmentStatistics struct {
	Environment     string
	CollectionCount int
	ResourceCount   int
}

type TeamStatistics struct {
	Team            string
	CollectionCount int
	ResourceCount   int
}

func (p *PostgresClient) GetStatistics(ctx context.Context) (*Statistics, error) {
	stats := &Statistics{
		ResourcesByKind:        make(map[string]int),
		CollectionsByNamespace: make(map[string]int),
	}

	// Get total collections
	err := p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collections").Scan(&stats.TotalCollections)
	if err != nil {
		return nil, err
	}

	// Get total resources
	err = p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collection_resources").Scan(&stats.TotalResources)
	if err != nil {
		return nil, err
	}

	// Get total orphans
	err = p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orphan_resources").Scan(&stats.TotalOrphans)
	if err != nil {
		return nil, err
	}

	// Get confidence distribution
	err = p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collections WHERE confidence_score >= 70").Scan(&stats.HighConfidenceCount)
	if err != nil {
		return nil, err
	}

	err = p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collections WHERE confidence_score >= 40 AND confidence_score < 70").Scan(&stats.MediumConfidenceCount)
	if err != nil {
		return nil, err
	}

	err = p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collections WHERE confidence_score < 40").Scan(&stats.LowConfidenceCount)
	if err != nil {
		return nil, err
	}

	// Get resources by kind
	rows, err := p.db.QueryContext(ctx, "SELECT kind, COUNT(*) FROM collection_resources GROUP BY kind")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var kind string
		var count int
		if err := rows.Scan(&kind, &count); err != nil {
			return nil, err
		}
		stats.ResourcesByKind[kind] = count
	}

	// Get collections by namespace
	rows, err = p.db.QueryContext(ctx, "SELECT namespace, COUNT(*) FROM collections GROUP BY namespace")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var namespace string
		var count int
		if err := rows.Scan(&namespace, &count); err != nil {
			return nil, err
		}
		stats.CollectionsByNamespace[namespace] = count
	}

	return stats, nil
}

func (p *PostgresClient) GetEnvironmentStatistics(ctx context.Context) ([]*EnvironmentStatistics, error) {
	query := `
        SELECT 
            c.environment,
            COUNT(DISTINCT c.id) as collection_count,
            COUNT(cr.id) as resource_count
        FROM collections c
        LEFT JOIN collection_resources cr ON c.id = cr.collection_id
        GROUP BY c.environment
        ORDER BY collection_count DESC
    `

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := []*EnvironmentStatistics{}
	for rows.Next() {
		stat := &EnvironmentStatistics{}
		err := rows.Scan(&stat.Environment, &stat.CollectionCount, &stat.ResourceCount)
		if err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}

	return stats, nil
}

func (p *PostgresClient) GetTeamStatistics(ctx context.Context) ([]*TeamStatistics, error) {
	query := `
        SELECT 
            c.team,
            COUNT(DISTINCT c.id) as collection_count,
            COUNT(cr.id) as resource_count
        FROM collections c
        LEFT JOIN collection_resources cr ON c.id = cr.collection_id
        GROUP BY c.team
        ORDER BY collection_count DESC
    `

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := []*TeamStatistics{}
	for rows.Next() {
		stat := &TeamStatistics{}
		err := rows.Scan(&stat.Team, &stat.CollectionCount, &stat.ResourceCount)
		if err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}

	return stats, nil
}

// ADDED: Search result struct and method
type SearchResult struct {
	CollectionID   string
	CollectionName string
	Kind           string
	Name           string
	Namespace      string
	UID            string
	IsOrphan       bool
}

func (p *PostgresClient) SearchResources(ctx context.Context, query, kind, namespace string) ([]*SearchResult, error) {
	sqlQuery := `
        SELECT 
            cr.collection_id,
            c.name as collection_name,
            cr.kind,
            cr.name,
            cr.namespace,
            cr.uid,
            false as is_orphan
        FROM collection_resources cr
        JOIN collections c ON cr.collection_id = c.id
        WHERE cr.name ILIKE $1
    `
	args := []interface{}{"%" + query + "%"}
	argIndex := 2

	if kind != "" {
		sqlQuery += fmt.Sprintf(" AND cr.kind = $%d", argIndex)
		args = append(args, kind)
		argIndex++
	}

	if namespace != "" {
		sqlQuery += fmt.Sprintf(" AND cr.namespace = $%d", argIndex)
		args = append(args, namespace)
		argIndex++
	}

	// Also search orphans
	orphanQuery := `
        UNION ALL
        SELECT 
            '' as collection_id,
            '' as collection_name,
            kind,
            name,
            namespace,
            uid,
            true as is_orphan
        FROM orphan_resources
        WHERE name ILIKE $1
    `

	orphanArgs := []interface{}{"%" + query + "%"}
	orphanArgIndex := 2

	if kind != "" {
		orphanQuery += fmt.Sprintf(" AND kind = $%d", orphanArgIndex)
		orphanArgs = append(orphanArgs, kind)
		orphanArgIndex++
	}

	if namespace != "" {
		orphanQuery += fmt.Sprintf(" AND namespace = $%d", orphanArgIndex)
		orphanArgs = append(orphanArgs, namespace)
		orphanArgIndex++
	}

	finalQuery := sqlQuery + orphanQuery + " ORDER BY is_orphan, name LIMIT 100"
	finalArgs := append(args, orphanArgs[1:]...)

	rows, err := p.db.QueryContext(ctx, finalQuery, finalArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := []*SearchResult{}
	for rows.Next() {
		result := &SearchResult{}
		err := rows.Scan(
			&result.CollectionID,
			&result.CollectionName,
			&result.Kind,
			&result.Name,
			&result.Namespace,
			&result.UID,
			&result.IsOrphan,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}

// ADDED: Resource detail record with version tracking
type ResourceDetailRecord struct {
	ID              int
	Kind            string
	Name            string
	Namespace       string
	UID             string
	IsPrimary       bool
	ResourceVersion string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// ADDED: Get resource details by UID
func (p *PostgresClient) GetResourceByUID(ctx context.Context, uid string) (*ResourceDetailRecord, error) {
	query := `
        SELECT id, kind, name, namespace, uid, is_primary, resource_version, created_at, updated_at
        FROM collection_resources
        WHERE uid = $1
        UNION
        SELECT id, kind, name, namespace, uid, false as is_primary, resource_version, created_at, updated_at
        FROM orphan_resources
        WHERE uid = $1
        LIMIT 1
    `

	record := &ResourceDetailRecord{}
	err := p.db.QueryRowContext(ctx, query, uid).Scan(
		&record.ID,
		&record.Kind,
		&record.Name,
		&record.Namespace,
		&record.UID,
		&record.IsPrimary,
		&record.ResourceVersion,
		&record.CreatedAt,
		&record.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return record, nil
}

// ADDED: Version history record
type ResourceVersionHistoryResponse struct {
	ResourceVersion string `json:"resource_version"`
	UpdatedAt       string `json:"updated_at"`
	CollectionID    string `json:"collection_id,omitempty"`
	IsOrphan        bool   `json:"is_orphan"`
}

// ADDED: Get resource version history (requires audit table - future enhancement)
func (p *PostgresClient) GetResourceVersionHistory(ctx context.Context, uid string, limit int) ([]*ResourceVersionHistoryResponse, error) {
	// NOTE: This requires an audit/history table which we don't have yet
	// For now, return current version only
	query := `
        SELECT resource_version, updated_at, collection_id, false as is_orphan
        FROM collection_resources
        WHERE uid = $1
        UNION
        SELECT resource_version, updated_at, '' as collection_id, true as is_orphan
        FROM orphan_resources
        WHERE uid = $1
        ORDER BY updated_at DESC
        LIMIT $2
    `

	rows, err := p.db.QueryContext(ctx, query, uid, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	history := []*ResourceVersionHistoryResponse{}
	for rows.Next() {
		record := &ResourceVersionHistoryResponse{}
		var updatedAt time.Time
		var collectionID sql.NullString

		err := rows.Scan(&record.ResourceVersion, &updatedAt, &collectionID, &record.IsOrphan)
		if err != nil {
			return nil, err
		}

		record.UpdatedAt = updatedAt.Format("2006-01-02T15:04:05Z")
		if collectionID.Valid {
			record.CollectionID = collectionID.String
		}

		history = append(history, record)
	}

	return history, nil
}

func (p *PostgresClient) Ping(ctx context.Context) error {
	return p.db.PingContext(ctx)
}

// ADDED: Get collection change history with pagination
func (p *PostgresClient) GetCollectionHistory(ctx context.Context, collectionID string, page, pageSize int) ([]*CollectionHistoryRecord, int, error) {
	logger := log.FromContext(ctx)

	// ADDED: Calculate offset for pagination
	offset := (page - 1) * pageSize

	// ADDED: First, get total count of changes for this collection
	countQuery := `
		SELECT COUNT(DISTINCT rc.id)
		FROM resource_changes rc
		INNER JOIN desired_resources dr ON rc.desired_resource_id = dr.id
		INNER JOIN collection_resources cr ON 
			dr.namespace = cr.namespace AND 
			dr.name = cr.name AND 
			dr.kind = cr.kind
		WHERE cr.collection_id = $1
	`

	var total int
	err := p.db.QueryRowContext(ctx, countQuery, collectionID).Scan(&total)
	if err != nil {
		logger.Error(err, "Failed to count collection history", "collectionID", collectionID)
		return nil, 0, fmt.Errorf("failed to count history: %w", err)
	}

	// ADDED: Return empty result if no history
	if total == 0 {
		return []*CollectionHistoryRecord{}, 0, nil
	}

	// ADDED: Get paginated history entries
	query := `
		SELECT 
			rc.id,
			rc.applied_at as timestamp,
			rc.change_type,
			rc.applied_by as changed_by,
			dr.kind as resource_kind,
			dr.name as resource_name,
			rc.diff,
			rc.status
		FROM resource_changes rc
		INNER JOIN desired_resources dr ON rc.desired_resource_id = dr.id
		INNER JOIN collection_resources cr ON 
			dr.namespace = cr.namespace AND 
			dr.name = cr.name AND 
			dr.kind = cr.kind
		WHERE cr.collection_id = $1
		ORDER BY rc.applied_at DESC, rc.id DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := p.db.QueryContext(ctx, query, collectionID, pageSize, offset)
	if err != nil {
		logger.Error(err, "Failed to query collection history", "collectionID", collectionID)
		return nil, 0, fmt.Errorf("failed to query history: %w", err)
	}
	defer rows.Close()

	history := []*CollectionHistoryRecord{}
	for rows.Next() {
		record := &CollectionHistoryRecord{}
		var diffJSON []byte

		err := rows.Scan(
			&record.ID,
			&record.Timestamp,
			&record.ChangeType,
			&record.ChangedBy,
			&record.ResourceKind,
			&record.ResourceName,
			&diffJSON,
			&record.Status,
		)

		if err != nil {
			logger.Error(err, "Failed to scan history record")
			return nil, 0, fmt.Errorf("failed to scan history: %w", err)
		}

		// ADDED: Parse diff JSON and generate summary
		var diff map[string]interface{}
		if err := json.Unmarshal(diffJSON, &diff); err != nil {
			logger.Error(err, "Failed to unmarshal diff", "changeID", record.ID)
			record.DiffSummary = "Unable to parse diff"
		} else {
			record.DiffSummary = generateDiffSummary(diff)
		}

		history = append(history, record)
	}

	if err = rows.Err(); err != nil {
		logger.Error(err, "Error iterating history rows")
		return nil, 0, fmt.Errorf("error iterating history: %w", err)
	}

	logger.V(1).Info("Retrieved collection history",
		"collectionID", collectionID,
		"total", total,
		"returned", len(history),
		"page", page)

	return history, total, nil
}

// ADDED: Generate human-readable diff summary from diff JSON
func generateDiffSummary(diff map[string]interface{}) string {
	// ADDED: Check if this is an apply-only diff
	if applied, ok := diff["applied"].(bool); ok && applied {
		if action, ok := diff["action"].(string); ok {
			return fmt.Sprintf("Action: %s", action)
		}
		return "Resource applied"
	}

	// ADDED: Check for simple action-based diff
	if action, ok := diff["action"].(string); ok {
		if resource, ok := diff["resource"].(map[string]interface{}); ok {
			if kind, ok := resource["kind"].(string); ok {
				return fmt.Sprintf("%s %s", action, kind)
			}
		}
		return fmt.Sprintf("Action: %s", action)
	}

	// ADDED: Parse structured diff operations
	if ops, ok := diff["operations"].([]interface{}); ok {
		summary := []string{}
		for _, op := range ops {
			if opMap, ok := op.(map[string]interface{}); ok {
				if path, ok := opMap["path"].(string); ok {
					operation := "modified"
					if opType, ok := opMap["op"].(string); ok {
						switch opType {
						case "add":
							operation = "added"
						case "remove":
							operation = "removed"
						case "replace":
							operation = "updated"
						}
					}

					// ADDED: Extract field name from path
					fieldName := path
					if strings.HasPrefix(path, "/spec/") {
						fieldName = strings.TrimPrefix(path, "/spec/")
					} else if strings.HasPrefix(path, "/metadata/") {
						fieldName = strings.TrimPrefix(path, "/metadata/")
					}

					// ADDED: Include values for replace operations
					if operation == "updated" {
						if from, ok := opMap["from"]; ok {
							if value, ok := opMap["value"]; ok {
								summary = append(summary, fmt.Sprintf("%s: %v → %v", fieldName, from, value))
								continue
							}
						}
					}

					summary = append(summary, fmt.Sprintf("%s %s", fieldName, operation))
				}
			}
		}

		if len(summary) > 0 {
			// ADDED: Limit summary to first 3 changes
			if len(summary) > 3 {
				return fmt.Sprintf("%s, and %d more changes",
					strings.Join(summary[:3], ", "),
					len(summary)-3)
			}
			return strings.Join(summary, ", ")
		}
	}

	// ADDED: Fallback for complex diffs
	return "Multiple fields modified"
}

// ADDED: Log an audit entry
func (p *PostgresClient) LogAuditEntry(ctx context.Context, collectionID, resourceUID, action, actor string, details map[string]interface{}) error {
	logger := log.FromContext(ctx)

	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("failed to marshal details: %w", err)
	}

	query := `
		INSERT INTO audit_log (collection_id, resource_uid, action, actor, details)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err = p.db.ExecContext(ctx, query, collectionID, resourceUID, action, actor, detailsJSON)
	if err != nil {
		logger.Error(err, "Failed to log audit entry",
			"collectionID", collectionID,
			"action", action,
			"actor", actor)
		return fmt.Errorf("failed to log audit entry: %w", err)
	}

	logger.V(1).Info("Logged audit entry",
		"collectionID", collectionID,
		"action", action,
		"actor", actor)

	return nil
}

// ADDED: Get collection activity with filtering and pagination
func (p *PostgresClient) GetCollectionActivity(
	ctx context.Context,
	collectionID string,
	page, pageSize int,
	actionFilter, actorFilter string,
	fromDate, toDate *time.Time,
) ([]*AuditLogRecord, int, error) {
	logger := log.FromContext(ctx)

	// ADDED: Calculate offset
	offset := (page - 1) * pageSize

	// ADDED: Build WHERE clause with filters
	whereClause := "WHERE collection_id = $1"
	args := []interface{}{collectionID}
	argIndex := 2

	if actionFilter != "" {
		whereClause += fmt.Sprintf(" AND action = $%d", argIndex)
		args = append(args, actionFilter)
		argIndex++
	}

	if actorFilter != "" {
		whereClause += fmt.Sprintf(" AND actor = $%d", argIndex)
		args = append(args, actorFilter)
		argIndex++
	}

	if fromDate != nil {
		whereClause += fmt.Sprintf(" AND created_at >= $%d", argIndex)
		args = append(args, fromDate)
		argIndex++
	}

	if toDate != nil {
		whereClause += fmt.Sprintf(" AND created_at <= $%d", argIndex)
		args = append(args, toDate)
		argIndex++
	}

	// ADDED: Get total count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM audit_log %s", whereClause)
	var total int
	err := p.db.QueryRowContext(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		logger.Error(err, "Failed to count audit entries", "collectionID", collectionID)
		return nil, 0, fmt.Errorf("failed to count audit entries: %w", err)
	}

	// ADDED: Return empty if no entries
	if total == 0 {
		return []*AuditLogRecord{}, 0, nil
	}

	// ADDED: Get paginated entries
	dataQuery := fmt.Sprintf(`
		SELECT id, collection_id, resource_uid, action, actor, details, created_at
		FROM audit_log
		%s
		ORDER BY created_at DESC, id DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argIndex, argIndex+1)

	dataArgs := append(args, pageSize, offset)
	rows, err := p.db.QueryContext(ctx, dataQuery, dataArgs...)
	if err != nil {
		logger.Error(err, "Failed to query audit entries", "collectionID", collectionID)
		return nil, 0, fmt.Errorf("failed to query audit entries: %w", err)
	}
	defer rows.Close()

	entries := []*AuditLogRecord{}
	for rows.Next() {
		entry := &AuditLogRecord{}
		var detailsJSON []byte
		var collectionIDNull, resourceUIDNull sql.NullString

		err := rows.Scan(
			&entry.ID,
			&collectionIDNull,
			&resourceUIDNull,
			&entry.Action,
			&entry.Actor,
			&detailsJSON,
			&entry.CreatedAt,
		)

		if err != nil {
			logger.Error(err, "Failed to scan audit entry")
			return nil, 0, fmt.Errorf("failed to scan audit entry: %w", err)
		}

		if collectionIDNull.Valid {
			entry.CollectionID = collectionIDNull.String
		}
		if resourceUIDNull.Valid {
			entry.ResourceUID = resourceUIDNull.String
		}

		// ADDED: Unmarshal details JSON
		if err := json.Unmarshal(detailsJSON, &entry.Details); err != nil {
			logger.Error(err, "Failed to unmarshal details", "auditID", entry.ID)
			entry.Details = map[string]interface{}{"error": "failed to parse details"}
		}

		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		logger.Error(err, "Error iterating audit entries")
		return nil, 0, fmt.Errorf("error iterating audit entries: %w", err)
	}

	logger.V(1).Info("Retrieved collection activity",
		"collectionID", collectionID,
		"total", total,
		"returned", len(entries),
		"page", page)

	return entries, total, nil
}

// ADDED: Update collection metadata
func (p *PostgresClient) UpdateCollectionMetadata(ctx context.Context, id string, updates map[string]interface{}) error {
	logger := log.FromContext(ctx)

	// ADDED: Build dynamic UPDATE query based on provided fields
	setClauses := []string{}
	args := []interface{}{}
	argIndex := 1

	// ADDED: Check each possible update field
	if env, ok := updates["environment"]; ok {
		setClauses = append(setClauses, fmt.Sprintf("environment = $%d", argIndex))
		args = append(args, env)
		argIndex++
	}

	if team, ok := updates["team"]; ok {
		setClauses = append(setClauses, fmt.Sprintf("team = $%d", argIndex))
		args = append(args, team)
		argIndex++
	}

	if desc, ok := updates["description"]; ok {
		setClauses = append(setClauses, fmt.Sprintf("description = $%d", argIndex))
		args = append(args, desc)
		argIndex++
	}

	if tags, ok := updates["tags"]; ok {
		// ADDED: Convert []string to PostgreSQL text array
		if tagSlice, ok := tags.([]interface{}); ok {
			tagStrings := make([]string, len(tagSlice))
			for i, tag := range tagSlice {
				tagStrings[i] = fmt.Sprintf("%v", tag)
			}
			// ADDED: Use pq.Array for proper PostgreSQL array handling
			setClauses = append(setClauses, fmt.Sprintf("tags = $%d", argIndex))
			args = append(args, pq.Array(tagStrings))
			argIndex++
		}
	}

	// ADDED: Return error if no fields to update
	if len(setClauses) == 0 {
		return fmt.Errorf("no fields provided for update")
	}

	// ADDED: Always update updated_at timestamp
	setClauses = append(setClauses, fmt.Sprintf("updated_at = NOW()"))

	// ADDED: Build final query
	query := fmt.Sprintf(
		"UPDATE collections SET %s WHERE id = $%d RETURNING id",
		strings.Join(setClauses, ", "),
		argIndex,
	)
	args = append(args, id)

	// ADDED: Execute update
	var returnedID string
	err := p.db.QueryRowContext(ctx, query, args...).Scan(&returnedID)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Error(err, "Collection not found", "id", id)
			return fmt.Errorf("collection %s not found", id)
		}
		logger.Error(err, "Failed to update collection metadata", "id", id)
		return fmt.Errorf("failed to update collection: %w", err)
	}

	logger.Info("Updated collection metadata",
		"id", id,
		"fieldsUpdated", len(setClauses))

	return nil
}
