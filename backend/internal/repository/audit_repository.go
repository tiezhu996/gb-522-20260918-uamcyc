package repository

import (
	"errors"
	"fmt"
	"strings"

	"fiber-otdr-fault-localization/backend/internal/dto"
	"fiber-otdr-fault-localization/backend/internal/model"
	"gorm.io/gorm"
)

type AuditRepository struct{ db *gorm.DB }

func (r *AuditRepository) Create(entry *model.AuditLog) error {
	if strings.TrimSpace(entry.RequestID) == "" {
		return fmt.Errorf("audit request id is required")
	}
	if err := r.db.Create(entry).Error; err != nil {
		return fmt.Errorf("create immutable audit entry: %w", err)
	}
	return nil
}

func (r *AuditRepository) List(query dto.AuditQuery) ([]model.AuditLog, int64, error) {
	db := r.db.Model(&model.AuditLog{})
	if query.RouteID != nil {
		db = db.Where("route_id = ?", *query.RouteID)
	}
	if query.ActorName != "" {
		db = db.Where("actor_name LIKE ?", "%"+query.ActorName+"%")
	}
	if query.Action != "" {
		db = db.Where("action = ?", query.Action)
	}
	if query.From != nil {
		db = db.Where("created_at >= ?", *query.From)
	}
	if query.To != nil {
		db = db.Where("created_at <= ?", *query.To)
	}
	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count audit logs: %w", err)
	}
	var entries []model.AuditLog
	if err := db.Order("created_at DESC").Offset((query.Page - 1) * query.PageSize).Limit(query.PageSize).Find(&entries).Error; err != nil {
		return nil, 0, fmt.Errorf("list audit logs: %w", err)
	}
	return entries, total, nil
}

type IdempotencyRepository struct{ db *gorm.DB }

func (r *IdempotencyRepository) Find(scope string, actorID uint, key string) (model.IdempotencyRecord, error) {
	var record model.IdempotencyRecord
	if err := r.db.Where("scope = ? AND actor_id = ? AND idempotency_key = ?", scope, actorID, key).First(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return record, ErrNotFound
		}
		return record, fmt.Errorf("find idempotency record: %w", err)
	}
	return record, nil
}

func (r *IdempotencyRepository) Create(record *model.IdempotencyRecord) error {
	if err := r.db.Create(record).Error; err != nil {
		return fmt.Errorf("create idempotency record: %w", err)
	}
	return nil
}
