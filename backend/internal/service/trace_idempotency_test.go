package service

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"fiber-otdr-fault-localization/backend/internal/dto"
	"fiber-otdr-fault-localization/backend/internal/idempotency"
	"fiber-otdr-fault-localization/backend/internal/model"
	"fiber-otdr-fault-localization/backend/internal/repository"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newImportService(t *testing.T) (*TraceService, *gorm.DB, Actor) {
	t.Helper()
	dsn := fmt.Sprintf("file:import-service-%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if sqlDB, err := db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := db.AutoMigrate(&model.User{}, &model.FiberRoute{}, &model.TraceCapture{}, &model.AuditLog{}, &model.IdempotencyRecord{}); err != nil {
		t.Fatal(err)
	}
	user := model.User{Username: "analyst", DisplayName: "分析员", Role: "analyst", PasswordHash: "x", Active: true}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	route := model.FiberRoute{RouteCode: "R-1", Name: "测试线路", LengthM: 1000, RefractiveIndex: 1.46, LaunchConnector: "SC/APC", RouteStatus: "active"}
	if err := db.Create(&route).Error; err != nil {
		t.Fatal(err)
	}
	store := repository.NewStore(db)
	svc := NewTraceService(store, 20000, idempotency.NewProtector(store))
	actor := Actor{ID: user.ID, Username: user.Username, Role: user.Role, RequestID: "req-import-1"}
	return svc, db, actor
}

func validImportRequest(routeID uint, seed float64) dto.ImportTraceRequest {
	points := make([]float64, 64)
	for i := range points {
		points[i] = -12.0 - float64(i%5)*0.2 + seed
	}
	return dto.ImportTraceRequest{
		RouteID:          routeID,
		WavelengthNM:     1550,
		PulseWidthNS:     100,
		SampleIntervalNS: 10,
		Points:           points,
		CapturedAt:       time.Date(2026, 9, 17, 8, 0, 0, 0, time.UTC),
	}
}

func countAudits(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var count int64
	if err := db.Model(&model.AuditLog{}).Where("action = ?", "trace.imported").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func countTracesRows(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var count int64
	if err := db.Model(&model.TraceCapture{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func TestImportIdempotentReplayAndConflict(t *testing.T) {
	svc, db, actor := newImportService(t)
	request := validImportRequest(1, 0)

	first, err := svc.Import(request, actor, "client-key-1")
	if err != nil {
		t.Fatalf("first import: %v", err)
	}
	if first.Replayed {
		t.Fatal("first import must not be marked as replay")
	}
	if got := countTracesRows(t, db); got != 1 {
		t.Fatalf("expected 1 trace, got %d", got)
	}
	if got := countAudits(t, db); got != 1 {
		t.Fatalf("expected 1 audit entry, got %d", got)
	}

	// Same key + same sample resubmission reads back the first trace.
	replay, err := svc.Import(request, actor, "client-key-1")
	if err != nil {
		t.Fatalf("replay import: %v", err)
	}
	if !replay.Replayed || replay.Trace.ID != first.Trace.ID {
		t.Fatalf("replay must return first trace id=%d, got %+v", first.Trace.ID, replay)
	}
	if got := countTracesRows(t, db); got != 1 {
		t.Fatalf("replay created a trace: %d", got)
	}
	if got := countAudits(t, db); got != 1 {
		t.Fatalf("replay must not write another audit entry, got %d", got)
	}

	// Same key with different samples conflicts and changes nothing.
	changed := validImportRequest(1, 3.5)
	if _, err := svc.Import(changed, actor, "client-key-1"); err == nil {
		t.Fatal("changed samples under same key must conflict")
	} else if appErr, ok := err.(*AppError); !ok || appErr.Code != idempotency.CodeConflict || appErr.Status != 409 {
		t.Fatalf("expected IDEMPOTENCY_CONFLICT/409, got %v", err)
	}
	if got := countTracesRows(t, db); got != 1 {
		t.Fatalf("conflict created a trace: %d", got)
	}
	if got := countAudits(t, db); got != 1 {
		t.Fatalf("conflict altered the audit trail: %d", got)
	}
}

func TestImportConcurrentSameKeyCreatesOnce(t *testing.T) {
	svc, db, base := newImportService(t)
	request := validImportRequest(1, 0)

	const n = 16
	var wg sync.WaitGroup
	ids := make([]uint, n)
	replayed := make([]bool, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			actor := base
			actor.RequestID = fmt.Sprintf("req-concurrent-%d", index)
			<-start
			result, err := svc.Import(request, actor, "client-hot-key")
			if err == nil {
				ids[index] = result.Trace.ID
				replayed[index] = result.Replayed
			}
		}(i)
	}
	close(start)
	wg.Wait()

	var winner uint
	leaders := 0
	for i := range ids {
		if winner == 0 {
			winner = ids[i]
		}
		if ids[i] != winner {
			t.Fatalf("request %d produced id %d, expected %d", i, ids[i], winner)
		}
		if !replayed[i] {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("exactly one import may lead, got %d", leaders)
	}
	if got := countTracesRows(t, db); got != 1 {
		t.Fatalf("expected one trace after concurrent submit, got %d", got)
	}
	if got := countAudits(t, db); got != 1 {
		t.Fatalf("expected one audit entry after concurrent submit, got %d", got)
	}
}
