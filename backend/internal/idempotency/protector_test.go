package idempotency

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"fiber-otdr-fault-localization/backend/internal/model"
	"fiber-otdr-fault-localization/backend/internal/repository"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type fakeTrace struct {
	ID      uint   `gorm:"primaryKey"`
	Payload string `gorm:"size:120;not null"`
}

type traceResult struct {
	ID   uint   `json:"id"`
	Name string `json:"name"`
}

func newTestProtector(t *testing.T) (*Protector, *gorm.DB) {
	t.Helper()
	dsn := fmt.Sprintf("file:idempotency-%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	if err := db.AutoMigrate(&model.IdempotencyRecord{}, &fakeTrace{}); err != nil {
		t.Fatal(err)
	}
	store := repository.NewStore(db)
	p := &Protector{store: store, locks: newKeyedLocks(), waitTimeout: 5 * time.Second, waitTick: 5 * time.Millisecond}
	return p, db
}

func makeBusiness(db *gorm.DB, payload string, failOnce *bool) BusinessFunc[traceResult] {
	return func(tx *repository.Store) (traceResult, ResourceRef, error) {
		if failOnce != nil && *failOnce {
			*failOnce = false
			return traceResult{}, ResourceRef{}, errors.New("simulated import failure")
		}
		created := fakeTrace{Payload: payload}
		if err := tx.DB.Create(&created).Error; err != nil {
			return traceResult{}, ResourceRef{}, err
		}
		return traceResult{ID: created.ID, Name: payload}, NewResourceRef("TraceCapture", created.ID), nil
	}
}

// replayTrace resolves stored resources the same way the trace service does:
// read back the row the first request created.
func replayTrace(store *repository.Store, ref ResourceRef) (traceResult, error) {
	var row fakeTrace
	if err := store.DB.First(&row, ref.ID()).Error; err != nil {
		return traceResult{}, err
	}
	return traceResult{ID: row.ID, Name: row.Payload}, nil
}

func countTraces(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var count int64
	if err := db.Model(&fakeTrace{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func TestExecuteAppliesThenReplaysSamePayload(t *testing.T) {
	p, db := newTestProtector(t)
	business := makeBusiness(db, "sample-set-A", nil)
	payload := map[string]any{"route_id": float64(1), "points": []any{float64(-12), float64(-13)}}

	first, err := Execute(p, ScopeTraceImport, "key-1", payload, "req-1", business, replayTrace)
	if err != nil {
		t.Fatalf("first import: %v", err)
	}
	if first.Replayed || first.Result.ID == 0 {
		t.Fatalf("first call must create, got %+v", first)
	}
	second, err := Execute(p, ScopeTraceImport, "key-1", payload, "req-2", business, replayTrace)
	if err != nil {
		t.Fatalf("replay import: %v", err)
	}
	if !second.Replayed || second.Result.ID != first.Result.ID || second.Result.Name != "sample-set-A" {
		t.Fatalf("replay must return the first result, got %+v want id %d", second, first.Result.ID)
	}
	if count := countTraces(t, db); count != 1 {
		t.Fatalf("exactly one trace may exist, got %d", count)
	}
	var record model.IdempotencyRecord
	if err := db.Where("scope = ? AND key = ?", ScopeTraceImport, "key-1").First(&record).Error; err != nil {
		t.Fatal(err)
	}
	if record.Status != model.IdempotencyStatusCompleted || record.RequestID != "req-1" || record.ResourceID != first.Result.ID || record.ResourceType != "TraceCapture" {
		t.Fatalf("record must retain the original request and resource: %+v", record)
	}
}

func TestExecuteConflictsWhenPayloadChanges(t *testing.T) {
	p, db := newTestProtector(t)
	business := makeBusiness(db, "sample-set-A", nil)
	original := map[string]any{"route_id": float64(1), "points": []any{float64(-12), float64(-13)}}
	if _, err := Execute(p, ScopeTraceImport, "key-2", original, "req-1", business, replayTrace); err != nil {
		t.Fatalf("first import: %v", err)
	}
	changed := map[string]any{"route_id": float64(1), "points": []any{float64(-12), float64(-99)}}
	_, err := Execute(p, ScopeTraceImport, "key-2", changed, "req-2", business, replayTrace)
	coreErr, ok := AsError(err)
	if !ok || coreErr.Code != CodeConflict {
		t.Fatalf("expected IDEMPOTENCY_CONFLICT, got %v", err)
	}
	if count := countTraces(t, db); count != 1 {
		t.Fatalf("conflict must not create a second trace, got %d", count)
	}
	var recordCount int64
	if err := db.Model(&model.IdempotencyRecord{}).Where("scope = ? AND key = ?", ScopeTraceImport, "key-2").Count(&recordCount).Error; err != nil {
		t.Fatal(err)
	}
	if recordCount != 1 {
		t.Fatalf("conflict must not add an idempotency record, got %d", recordCount)
	}
}

func TestExecuteConcurrentDuplicatesCollapseToOne(t *testing.T) {
	p, db := newTestProtector(t)
	payload := map[string]any{"route_id": float64(3), "points": []any{float64(-1), float64(-2), float64(-3)}}

	const goroutines = 24
	var wg sync.WaitGroup
	results := make([]uint, goroutines)
	replayed := make([]bool, goroutines)
	errs := make([]error, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			outcome, err := Execute(p, ScopeTraceImport, "hot-key", payload, fmt.Sprintf("req-%d", index), makeBusiness(db, "concurrent", nil), replayTrace)
			if err == nil {
				results[index] = outcome.Result.ID
				replayed[index] = outcome.Replayed
			}
			errs[index] = err
		}(i)
	}
	close(start)
	wg.Wait()

	var winnerID uint
	leaders, replays := 0, 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d failed: %v", i, err)
		}
		if winnerID == 0 {
			winnerID = results[i]
		}
		if results[i] != winnerID {
			t.Fatalf("goroutine %d got id %d, all must read %d", i, results[i], winnerID)
		}
		if replayed[i] {
			replays++
		} else {
			leaders++
		}
	}
	if leaders != 1 || replays != goroutines-1 {
		t.Fatalf("expected exactly one leader and %d replays, got leaders=%d replays=%d", goroutines-1, leaders, replays)
	}
	if count := countTraces(t, db); count != 1 {
		t.Fatalf("concurrent duplicates created %d traces", count)
	}
}

func TestExecuteRollsBackAndReleasesKeyOnBusinessError(t *testing.T) {
	p, db := newTestProtector(t)
	fail := true
	payload := map[string]any{"route_id": float64(9), "points": []any{float64(-5), float64(-6)}}
	if _, err := Execute(p, ScopeTraceImport, "retry-key", payload, "req-1", makeBusiness(db, "later", &fail), replayTrace); err == nil {
		t.Fatal("expected the business failure to surface")
	}
	if count := countTraces(t, db); count != 0 {
		t.Fatalf("failed leader must not leave a trace, got %d", count)
	}
	var pending int64
	if err := db.Model(&model.IdempotencyRecord{}).Where("scope = ? AND key = ?", ScopeTraceImport, "retry-key").Count(&pending).Error; err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("rolled-back transaction must not retain the key row, got %d", pending)
	}
	outcome, err := Execute(p, ScopeTraceImport, "retry-key", payload, "req-2", makeBusiness(db, "later", nil), replayTrace)
	if err != nil {
		t.Fatalf("key must be reusable after rollback: %v", err)
	}
	if outcome.Replayed || outcome.Result.ID == 0 {
		t.Fatalf("retry must create fresh, got %+v", outcome)
	}
	if count := countTraces(t, db); count != 1 {
		t.Fatalf("retry must create exactly one trace, got %d", count)
	}
}

func TestExecuteRejectsInvalidKeyBeforeBusiness(t *testing.T) {
	p, _ := newTestProtector(t)
	called := false
	business := func(tx *repository.Store) (traceResult, ResourceRef, error) {
		called = true
		return traceResult{}, ResourceRef{}, nil
	}
	if _, err := Execute(p, ScopeTraceImport, "", map[string]any{"x": 1}, "req-1", business, replayTrace); err == nil {
		t.Fatal("empty key must be rejected")
	}
	if called {
		t.Fatal("business must not run for an invalid key")
	}
}

func TestRepositoryCreatePendingNormalizesDuplicate(t *testing.T) {
	p, _ := newTestProtector(t)
	first := &model.IdempotencyRecord{Scope: ScopeTraceImport, Key: "dup", Fingerprint: "fp", RequestID: "req-1"}
	if err := p.store.Idempotencies.CreatePending(first); err != nil {
		t.Fatal(err)
	}
	second := &model.IdempotencyRecord{Scope: ScopeTraceImport, Key: "dup", Fingerprint: "fp", RequestID: "req-2"}
	if err := p.store.Idempotencies.CreatePending(second); !errors.Is(err, repository.ErrIdempotencyKeyExists) {
		t.Fatalf("expected ErrIdempotencyKeyExists, got %v", err)
	}
}

func TestReplayWaitsForInFlightCompletion(t *testing.T) {
	p, db := newTestProtector(t)
	payload := map[string]any{"anything": float64(1)}
	fingerprint, err := Fingerprint(payload)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate another process that owns the key and resolves it to an
	// already-existing resource once it finishes.
	foreign := fakeTrace{Payload: "foreign"}
	if err := db.Create(&foreign).Error; err != nil {
		t.Fatal(err)
	}
	pending := &model.IdempotencyRecord{Scope: ScopeTraceImport, Key: "in-flight", Fingerprint: fingerprint, Status: model.IdempotencyStatusPending, RequestID: "req-other"}
	if err := db.Create(pending).Error; err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(30 * time.Millisecond)
		store := repository.NewStore(db)
		_ = store.Idempotencies.Complete(pending.ID, "TraceCapture", foreign.ID)
	}()
	outcome, err := Execute(p, ScopeTraceImport, "in-flight", payload, "req-me", makeBusiness(db, "unused", nil), replayTrace)
	if err != nil {
		t.Fatalf("waiter should observe completion: %v", err)
	}
	if !outcome.Replayed || outcome.Result.ID != foreign.ID {
		t.Fatalf("waiter must resolve the foreign resource, got %+v", outcome)
	}
}
