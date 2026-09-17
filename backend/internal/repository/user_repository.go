package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"fiber-otdr-fault-localization/backend/internal/config"
	"fiber-otdr-fault-localization/backend/internal/constants"
	"fiber-otdr-fault-localization/backend/internal/model"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var ErrNotFound = errors.New("record not found")

// ErrIdempotencyKeyExists is returned when an INSERT races an existing
// (scope, key) document. Callers treat it as "another request owns the key"
// and replay-by-reference or conflict instead of creating a second resource.
var ErrIdempotencyKeyExists = errors.New("idempotency key already exists")

type Store struct {
	DB            *gorm.DB
	Users         *UserRepository
	Routes        *FiberRouteRepository
	Traces        *TraceRepository
	Events        *EventRepository
	Cases         *CaseRepository
	Audits        *AuditRepository
	Idempotencies *IdempotencyRepository
}

func Open(cfg config.Config) (*gorm.DB, error) {
	var dialector gorm.Dialector
	if cfg.DBDriver == "sqlite" {
		dialector = sqlite.Open(cfg.DBDSN)
	} else {
		dialector = postgres.Open(cfg.DBDSN)
	}
	db, err := gorm.Open(dialector, &gorm.Config{Logger: logger.Default.LogMode(logger.Warn), TranslateError: true})
	if err != nil {
		return nil, fmt.Errorf("open %s database: %w", cfg.DBDriver, err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("obtain sql database: %w", err)
	}
	sqlDB.SetMaxOpenConns(20)
	sqlDB.SetMaxIdleConns(5)
	return db, nil
}

func NewStore(db *gorm.DB) *Store {
	return &Store{DB: db, Users: &UserRepository{db}, Routes: &FiberRouteRepository{db}, Traces: &TraceRepository{db}, Events: &EventRepository{db}, Cases: &CaseRepository{db}, Audits: &AuditRepository{db}, Idempotencies: &IdempotencyRepository{db}}
}

func (s *Store) Transaction(fn func(*Store) error) error {
	return s.DB.Transaction(func(tx *gorm.DB) error { return fn(NewStore(tx)) })
}

func (s *Store) Ping(ctx context.Context) error {
	sqlDB, err := s.DB.DB()
	if err != nil {
		return fmt.Errorf("obtain sql database for readiness: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("database readiness ping: %w", err)
	}
	return nil
}

func MigrateAndSeed(db *gorm.DB) error {
	if err := db.AutoMigrate(&model.User{}, &model.FiberRoute{}, &model.TraceCapture{}, &model.EventMarker{}, &model.LocalizationCase{}, &model.AuditLog{}, &model.IdempotencyRecord{}); err != nil {
		return fmt.Errorf("auto migrate: %w", err)
	}
	accounts := []struct{ username, display, role string }{{"analyst", "分析员", constants.RoleAnalyst}, {"reviewer", "复核员", constants.RoleReviewer}, {"admin", "系统管理员", constants.RoleAdmin}}
	hash, err := bcrypt.GenerateFromPassword([]byte("DemoPass123!"), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash seed password: %w", err)
	}
	for _, account := range accounts {
		var existing model.User
		err := db.Where("username = ?", account.username).First(&existing).Error
		if err == nil {
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("find seed user: %w", err)
		}
		user := model.User{Username: account.username, DisplayName: account.display, Role: account.role, PasswordHash: string(hash), Active: true}
		if err := db.Create(&user).Error; err != nil {
			return fmt.Errorf("create seed user %s: %w", account.username, err)
		}
	}
	return nil
}

type UserRepository struct{ db *gorm.DB }

func (r *UserRepository) FindByUsername(username string) (model.User, error) {
	var user model.User
	if err := r.db.Where("username = ? AND active = ?", username, true).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return user, ErrNotFound
		}
		return user, fmt.Errorf("find user by username: %w", err)
	}
	return user, nil
}

func (r *UserRepository) FindByID(id uint) (model.User, error) {
	var user model.User
	if err := r.db.Where("id = ? AND active = ?", id, true).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return user, ErrNotFound
		}
		return user, fmt.Errorf("find user by id: %w", err)
	}
	return user, nil
}

// IdempotencyRepository owns the durable request-deduplication documents.
type IdempotencyRepository struct{ db *gorm.DB }

// CreatePending inserts the pending ownership row. A unique-index violation
// is normalized to ErrIdempotencyKeyExists so the core never depends on
// driver-specific error strings.
func (r *IdempotencyRepository) CreatePending(record *model.IdempotencyRecord) error {
	record.Status = model.IdempotencyStatusPending
	if err := r.db.Create(record).Error; err != nil {
		if isDuplicateKeyError(err) {
			return ErrIdempotencyKeyExists
		}
		return fmt.Errorf("create idempotency record: %w", err)
	}
	return nil
}

// Complete finalizes the owned row inside the same transaction.
func (r *IdempotencyRepository) Complete(id uint, resourceType string, resourceID uint) error {
	result := r.db.Model(&model.IdempotencyRecord{}).Where("id = ?", id).Updates(map[string]any{
		"status":        model.IdempotencyStatusCompleted,
		"resource_type": resourceType,
		"resource_id":   resourceID,
	})
	if result.Error != nil {
		return fmt.Errorf("complete idempotency record: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("complete idempotency record: %w", ErrNotFound)
	}
	return nil
}

// Get retrieves a record by scope and key.
func (r *IdempotencyRepository) Get(scope, key string) (model.IdempotencyRecord, error) {
	var record model.IdempotencyRecord
	if err := r.db.Where("scope = ? AND key = ?", scope, key).First(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return record, ErrNotFound
		}
		return record, fmt.Errorf("get idempotency record: %w", err)
	}
	return record, nil
}

func isDuplicateKeyError(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	message := strings.ToLower(err.Error())
	// PostgreSQL unique_violation 23505 and mattn/go-sqlite3 UNIQUE constraint.
	return strings.Contains(message, "duplicate key value") ||
		strings.Contains(message, "unique constraint failed") ||
		strings.Contains(message, "23505")
}
