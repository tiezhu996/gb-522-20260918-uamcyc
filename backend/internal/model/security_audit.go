package model

import "time"

type User struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	Username     string    `gorm:"size:60;not null;uniqueIndex" json:"username"`
	PasswordHash string    `gorm:"size:100;not null" json:"-"`
	DisplayName  string    `gorm:"size:100;not null" json:"display_name"`
	Role         string    `gorm:"size:24;not null;index;check:user_role_allowed,role IN ('analyst','reviewer','admin')" json:"role"`
	Active       bool      `gorm:"not null;default:true" json:"active"`
	CreatedAt    time.Time `json:"created_at"`
}

type AuditLog struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	ActorID      uint      `gorm:"not null;index" json:"actor_id"`
	ActorName    string    `gorm:"size:60;not null;index" json:"actor_name"`
	Action       string    `gorm:"size:80;not null;index" json:"action"`
	ResourceType string    `gorm:"size:60;not null;index" json:"resource_type"`
	ResourceID   uint      `gorm:"not null;index" json:"resource_id"`
	RouteID      *uint     `gorm:"index" json:"route_id"`
	RequestID    string    `gorm:"size:80;not null;index" json:"request_id"`
	Before       string    `gorm:"type:text;not null" json:"before"`
	After        string    `gorm:"type:text;not null" json:"after"`
	CreatedAt    time.Time `json:"index" json:"created_at"`
}

const (
	IdempotencyStatusPending   = "pending"
	IdempotencyStatusCompleted = "completed"
)

// IdempotencyRecord is the durable authority for request deduplication.
// A committed row is always completed: pending rows only live inside the
// open transaction that owns the key, so a crashed leader rolls back and
// releases the key automatically instead of wedging it. Replays resolve the
// created resource through ResourceType/ResourceID, so request payloads are
// never duplicated into this table.
type IdempotencyRecord struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	Scope        string    `gorm:"size:64;not null;uniqueIndex:uniq_idempotency_scope_key,priority:1;index:idx_idempotency_status" json:"scope"`
	Key          string    `gorm:"size:128;not null;uniqueIndex:uniq_idempotency_scope_key,priority:2" json:"key"`
	Fingerprint  string    `gorm:"size:64;not null" json:"fingerprint"`
	Status       string    `gorm:"size:16;not null;index:idx_idempotency_status" json:"status"`
	ResourceType string    `gorm:"size:60;not null" json:"resource_type"`
	ResourceID   uint      `gorm:"not null" json:"resource_id"`
	RequestID    string    `gorm:"size:80;not null;index" json:"request_id"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}
