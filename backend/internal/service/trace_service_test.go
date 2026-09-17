package service

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fiber-otdr-fault-localization/backend/internal/dto"
	"fiber-otdr-fault-localization/backend/internal/model"
	"fiber-otdr-fault-localization/backend/internal/repository"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var idemTestDBSeq atomic.Int64

func newIdempotencyTestStore(t *testing.T) (*repository.Store, uint) {
	t.Helper()
	dsn := fmt.Sprintf("file:idem-%d?mode=memory&cache=shared", idemTestDBSeq.Add(1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatal(err)
	}
	// Serialize access like repository.Open does for sqlite, otherwise the
	// shared-cache connection pool fails concurrent writers with SQLITE_LOCKED.
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	if err := db.AutoMigrate(&model.User{}, &model.FiberRoute{}, &model.TraceCapture{}, &model.EventMarker{}, &model.LocalizationCase{}, &model.AuditLog{}, &model.IdempotencyRecord{}); err != nil {
		t.Fatal(err)
	}
	route := model.FiberRoute{RouteCode: "RT-IDEM", Name: "idempotency route", LengthM: 5000, RefractiveIndex: 1.468, LaunchConnector: "SC/APC", RouteStatus: "active"}
	if err := db.Create(&route).Error; err != nil {
		t.Fatal(err)
	}
	return repository.NewStore(db), route.ID
}

func idemImportRequest(routeID uint, shift float64) dto.ImportTraceRequest {
	points := make([]float64, 240)
	for i := range points {
		points[i] = 28 - float64(i)*0.035 - shift
	}
	return dto.ImportTraceRequest{RouteID: routeID, WavelengthNM: 1550, PulseWidthNS: 100, SampleIntervalNS: 100, Points: points, CapturedAt: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)}
}

func idemTestActor(id uint) Actor {
	return Actor{ID: id, Username: fmt.Sprintf("analyst-%d", id), Role: "analyst", RequestID: fmt.Sprintf("req-%d", id)}
}

func countRows(t *testing.T, store *repository.Store, modelValue any) int64 {
	t.Helper()
	var count int64
	if err := store.DB.Model(modelValue).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func TestTraceImportIdempotencyReplaysFirstTrace(t *testing.T) {
	store, routeID := newIdempotencyTestStore(t)
	svc := NewTraceService(store, 20000, NewIdempotencyGuard(store))
	actor := idemTestActor(1)
	request := idemImportRequest(routeID, 0)
	first, replayed, err := svc.Import(request, actor, "idem-key-1")
	if err != nil || replayed {
		t.Fatalf("first import: replayed=%v err=%v", replayed, err)
	}
	second, replayed, err := svc.Import(request, actor, "idem-key-1")
	if err != nil {
		t.Fatalf("replay import failed: %v", err)
	}
	if !replayed || second.ID != first.ID {
		t.Fatalf("expected replay of trace %d, got id=%d replayed=%v", first.ID, second.ID, replayed)
	}
	if !second.CreatedAt.Equal(first.CreatedAt) || second.NoiseFloorDB != first.NoiseFloorDB {
		t.Fatalf("replay must read back the original trace, got %+v", second)
	}
	if count := countRows(t, store, &model.TraceCapture{}); count != 1 {
		t.Fatalf("expected exactly one trace, got %d", count)
	}
	if count := countRows(t, store, &model.AuditLog{}); count != 1 {
		t.Fatalf("expected exactly one audit entry, got %d", count)
	}
	if count := countRows(t, store, &model.IdempotencyRecord{}); count != 1 {
		t.Fatalf("expected exactly one idempotency record, got %d", count)
	}
}

func TestTraceImportIdempotencyConflictKeepsOriginal(t *testing.T) {
	store, routeID := newIdempotencyTestStore(t)
	svc := NewTraceService(store, 20000, NewIdempotencyGuard(store))
	actor := idemTestActor(1)
	first, _, err := svc.Import(idemImportRequest(routeID, 0), actor, "idem-key-2")
	if err != nil {
		t.Fatalf("first import failed: %v", err)
	}
	_, _, err = svc.Import(idemImportRequest(routeID, 1.5), actor, "idem-key-2")
	var appErr *AppError
	if !errors.As(err, &appErr) || appErr.Status != 409 || appErr.Code != CodeConflict {
		t.Fatalf("expected 409 %s, got %v", CodeConflict, err)
	}
	if count := countRows(t, store, &model.TraceCapture{}); count != 1 {
		t.Fatalf("conflict must not create a trace, got %d", count)
	}
	if count := countRows(t, store, &model.AuditLog{}); count != 1 {
		t.Fatalf("conflict must not add audit entries, got %d", count)
	}
	reloaded, err := store.Traces.Get(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.NoiseFloorDB != first.NoiseFloorDB || string(reloaded.RawPointsJSON) != string(first.RawPointsJSON) {
		t.Fatalf("original trace was modified by the conflicting submission")
	}
}

func TestTraceImportIdempotencyConcurrentSingleOutcome(t *testing.T) {
	store, routeID := newIdempotencyTestStore(t)
	svc := NewTraceService(store, 20000, NewIdempotencyGuard(store))
	actor := idemTestActor(1)
	request := idemImportRequest(routeID, 0)
	const workers = 8
	var wg sync.WaitGroup
	ids := make([]uint, workers)
	replays := make([]bool, workers)
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			trace, replayed, err := svc.Import(request, actor, "idem-key-shared")
			ids[i], replays[i], errs[i] = trace.ID, replayed, err
		}(i)
	}
	wg.Wait()
	executed := 0
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("concurrent import %d failed: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("concurrent imports returned different traces: %d vs %d", ids[i], ids[0])
		}
		if !replays[i] {
			executed++
		}
	}
	if executed != 1 {
		t.Fatalf("exactly one import may execute, got %d", executed)
	}
	if count := countRows(t, store, &model.TraceCapture{}); count != 1 {
		t.Fatalf("expected exactly one trace, got %d", count)
	}
	if count := countRows(t, store, &model.AuditLog{}); count != 1 {
		t.Fatalf("expected exactly one audit entry, got %d", count)
	}
	if count := countRows(t, store, &model.IdempotencyRecord{}); count != 1 {
		t.Fatalf("expected exactly one idempotency record, got %d", count)
	}
}

func TestTraceImportWithoutKeyCreatesEachTime(t *testing.T) {
	store, routeID := newIdempotencyTestStore(t)
	svc := NewTraceService(store, 20000, NewIdempotencyGuard(store))
	actor := idemTestActor(1)
	request := idemImportRequest(routeID, 0)
	first, replayed, err := svc.Import(request, actor, "")
	if err != nil || replayed {
		t.Fatalf("first import: replayed=%v err=%v", replayed, err)
	}
	second, replayed, err := svc.Import(request, actor, "")
	if err != nil || replayed {
		t.Fatalf("second import: replayed=%v err=%v", replayed, err)
	}
	if first.ID == second.ID {
		t.Fatalf("imports without a key must stay independent, both got id=%d", first.ID)
	}
	if count := countRows(t, store, &model.TraceCapture{}); count != 2 {
		t.Fatalf("expected two traces, got %d", count)
	}
	if count := countRows(t, store, &model.AuditLog{}); count != 2 {
		t.Fatalf("expected two audit entries, got %d", count)
	}
	if count := countRows(t, store, &model.IdempotencyRecord{}); count != 0 {
		t.Fatalf("keyless imports must not write idempotency records, got %d", count)
	}
}

func TestTraceImportIdempotencyKeyScopedPerActor(t *testing.T) {
	store, routeID := newIdempotencyTestStore(t)
	svc := NewTraceService(store, 20000, NewIdempotencyGuard(store))
	request := idemImportRequest(routeID, 0)
	first, _, err := svc.Import(request, idemTestActor(1), "idem-key-3")
	if err != nil {
		t.Fatalf("first import failed: %v", err)
	}
	second, replayed, err := svc.Import(request, idemTestActor(2), "idem-key-3")
	if err != nil || replayed {
		t.Fatalf("another actor's import must execute: replayed=%v err=%v", replayed, err)
	}
	if second.ID == first.ID {
		t.Fatalf("actor scoping leaked trace %d across accounts", first.ID)
	}
	if count := countRows(t, store, &model.IdempotencyRecord{}); count != 2 {
		t.Fatalf("expected one record per actor, got %d", count)
	}
}

func TestFingerprintRequest(t *testing.T) {
	a, err := FingerprintRequest(idemImportRequest(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	b, err := FingerprintRequest(idemImportRequest(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	c, err := FingerprintRequest(idemImportRequest(1, 0.5))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("identical payloads must share a fingerprint: %s vs %s", a, b)
	}
	if a == c {
		t.Fatalf("different payloads must not share a fingerprint: %s", a)
	}
	if len(a) != 64 {
		t.Fatalf("expected a hex sha-256 fingerprint, got %q", a)
	}
}
