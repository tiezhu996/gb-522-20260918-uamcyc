package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"fiber-otdr-fault-localization/backend/internal/algorithm"
	"fiber-otdr-fault-localization/backend/internal/dto"
	"fiber-otdr-fault-localization/backend/internal/model"
	"fiber-otdr-fault-localization/backend/internal/repository"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// ScopeTraceImport namespaces idempotency records that guard trace imports.
const ScopeTraceImport = "trace.import"

// maxIdempotencyAttempts bounds how often a guarded execution retries after
// losing a commit race on the idempotency key before giving up.
const maxIdempotencyAttempts = 4

// IdempotencyGuard executes a unit of work at most once per idempotency key.
// It is storage-agnostic about the resource being created: work runs inside
// the guard's transaction and returns the new resource id, which is journaled
// together with the key in the same commit.
type IdempotencyGuard struct{ store *repository.Store }

func NewIdempotencyGuard(store *repository.Store) *IdempotencyGuard {
	return &IdempotencyGuard{store}
}

// FingerprintRequest hashes the canonical JSON encoding of an already
// validated request payload. Two payloads with the same fingerprint are
// considered the same client intent for idempotency purposes.
func FingerprintRequest(payload any) (string, error) {
	canonical, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("fingerprint request payload: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// Execute runs work exactly once for the (scope, actor, key) triple:
//   - first sight of the key: work and its idempotency record commit in one
//     transaction, so a crash can never leave a resource without its record;
//   - same key and fingerprint: nothing is written and the original resource
//     id is returned with replayed=true;
//   - same key with a different fingerprint: a conflict AppError and no write.
//
// An empty key disables deduplication and simply runs work in a transaction.
// Concurrent first submissions are serialized by the unique index on the
// record; the loser retries and observes the winner's committed record, so a
// race can never produce two resources or two audit trails. work must not
// raise duplicate-key errors of its own, as they are treated as commit races.
func (g *IdempotencyGuard) Execute(scope string, actor Actor, key, fingerprint string, work func(tx *repository.Store) (uint, error)) (uint, bool, error) {
	if key == "" {
		var id uint
		err := g.store.Transaction(func(tx *repository.Store) error {
			var err error
			id, err = work(tx)
			return err
		})
		return id, false, err
	}
	for attempt := 1; ; attempt++ {
		resourceID, replayed, err := g.attempt(scope, actor, key, fingerprint, work)
		if err == nil {
			return resourceID, replayed, nil
		}
		var appErr *AppError
		if errors.As(err, &appErr) {
			return 0, false, err
		}
		if attempt >= maxIdempotencyAttempts || !isIdempotencyRace(err) {
			return 0, false, err
		}
		time.Sleep(time.Duration(attempt*15) * time.Millisecond)
	}
}

func (g *IdempotencyGuard) attempt(scope string, actor Actor, key, fingerprint string, work func(tx *repository.Store) (uint, error)) (uint, bool, error) {
	var resourceID uint
	var replayed bool
	err := g.store.Transaction(func(tx *repository.Store) error {
		existing, err := tx.Idempotency.Find(scope, actor.ID, key)
		if err == nil {
			if existing.RequestHash != fingerprint {
				return conflict("idempotency key was already used with a different payload", nil)
			}
			resourceID, replayed = existing.ResourceID, true
			return nil
		}
		if !errors.Is(err, repository.ErrNotFound) {
			return err
		}
		id, err := work(tx)
		if err != nil {
			return err
		}
		record := model.IdempotencyRecord{Scope: scope, ActorID: actor.ID, IdempotencyKey: key, RequestHash: fingerprint, ResourceID: id}
		if err := tx.Idempotency.Create(&record); err != nil {
			return err
		}
		resourceID = id
		return nil
	})
	return resourceID, replayed, err
}

// isIdempotencyRace reports whether err is a transient commit race on the
// idempotency record (unique-key collision, lock contention, deadlock) that
// disappears once the competing transaction commits and is safe to retry.
func isIdempotencyRace(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "database is locked") || strings.Contains(message, "database table is locked") || strings.Contains(message, "deadlock detected") {
		return true
	}
	return strings.Contains(message, "unique constraint") && strings.Contains(message, "idempotency")
}

type TraceService struct {
	store     *repository.Store
	maxPoints int
	guard     *IdempotencyGuard
}

func NewTraceService(store *repository.Store, maxPoints int, guard *IdempotencyGuard) *TraceService {
	return &TraceService{store, maxPoints, guard}
}

// Import stores one offline capture. When the client supplies an idempotency
// key, resubmitting the same payload with the same key reads back the trace
// created by the first submission (replayed=true) instead of importing again,
// while the same key with a different payload is rejected as a conflict; in
// both cases the original trace and its audit entry stay untouched.
func (s *TraceService) Import(request dto.ImportTraceRequest, actor Actor, idemKey string) (model.TraceCapture, bool, error) {
	if len(request.Points) > s.maxPoints {
		return model.TraceCapture{}, false, invalid(fmt.Sprintf("trace exceeds the %d-point limit", s.maxPoints), nil)
	}
	for i, point := range request.Points {
		if math.IsNaN(point) || math.IsInf(point, 0) || point < -200 || point > 100 {
			return model.TraceCapture{}, false, invalid(fmt.Sprintf("sample %d is outside the supported dB range", i), nil)
		}
	}
	route, err := s.store.Routes.Get(request.RouteID)
	if errors.Is(err, repository.ErrNotFound) {
		return model.TraceCapture{}, false, notFound("route")
	}
	if err != nil {
		return model.TraceCapture{}, false, internal("load route failed", err)
	}
	window := request.DenoiseWindow
	if window == 0 {
		window = 5
	}
	threshold := request.PeakThresholdDB
	if threshold == 0 {
		threshold = 0.8
	}
	merge := request.MergeWindow
	if merge == 0 {
		merge = 3
	}
	filtered, err := algorithm.MovingMedian(request.Points, window)
	if err != nil {
		return model.TraceCapture{}, false, invalid("trace denoising failed", err)
	}
	noise, err := algorithm.EstimateNoiseFloor(filtered)
	if err != nil {
		return model.TraceCapture{}, false, &AppError{CodeAlgorithmInput, 422, "not enough trace samples for noise estimation", err}
	}
	lastDistance, err := algorithm.SampleDistance(len(request.Points)-1, request.SampleIntervalNS, route.RefractiveIndex)
	if err != nil {
		return model.TraceCapture{}, false, invalid("distance conversion failed", err)
	}
	if lastDistance < route.LengthM*0.05 {
		return model.TraceCapture{}, false, invalid("trace sampling range covers less than five percent of the route", nil)
	}
	fingerprint := ""
	if idemKey != "" {
		fingerprint, err = FingerprintRequest(request)
		if err != nil {
			return model.TraceCapture{}, false, internal("fingerprint import request failed", err)
		}
	}
	raw, _ := json.Marshal(request.Points)
	processed, _ := json.Marshal(filtered)
	var trace model.TraceCapture
	resourceID, replayed, err := s.guard.Execute(ScopeTraceImport, actor, idemKey, fingerprint, func(tx *repository.Store) (uint, error) {
		trace = model.TraceCapture{RouteID: request.RouteID, WavelengthNM: request.WavelengthNM, PulseWidthNS: request.PulseWidthNS, SampleIntervalNS: request.SampleIntervalNS, RawPointsJSON: datatypes.JSON(raw), ProcessedJSON: datatypes.JSON(processed), NoiseFloorDB: noise, CapturedAt: request.CapturedAt, UploadedBy: actor.ID, DenoiseWindow: window, PeakThresholdDB: threshold, MergeWindow: merge}
		if err := tx.Traces.Create(&trace); err != nil {
			return 0, err
		}
		params := map[string]any{"point_count": len(request.Points), "wavelength_nm": request.WavelengthNM, "denoise_window": window, "peak_threshold_db": threshold, "merge_window": merge}
		if err := tx.Audits.Create(audit(actor, "trace.imported", "TraceCapture", trace.ID, &route.ID, "{}", snapshot(params))); err != nil {
			return 0, err
		}
		return trace.ID, nil
	})
	if err != nil {
		var appErr *AppError
		if errors.As(err, &appErr) {
			return model.TraceCapture{}, false, err
		}
		return model.TraceCapture{}, false, internal("import trace failed", err)
	}
	if replayed {
		original, err := s.store.Traces.Get(resourceID)
		if err != nil {
			return model.TraceCapture{}, false, internal("reload idempotent trace failed", err)
		}
		return original, true, nil
	}
	return trace, false, nil
}

func (s *TraceService) List(query dto.TraceQuery) ([]model.TraceCapture, dto.Pagination, error) {
	normalizePage(&query.Page, &query.PageSize)
	items, total, err := s.store.Traces.List(query)
	if err != nil {
		return nil, dto.Pagination{}, internal("list traces failed", err)
	}
	return items, dto.Pagination{Page: query.Page, PageSize: query.PageSize, Total: total}, nil
}

func (s *TraceService) Get(id uint) (dto.TraceDetail, []model.EventMarker, error) {
	trace, err := s.store.Traces.Get(id)
	if errors.Is(err, repository.ErrNotFound) {
		return dto.TraceDetail{}, nil, notFound("trace")
	}
	if err != nil {
		return dto.TraceDetail{}, nil, internal("get trace failed", err)
	}
	var raw, processed []float64
	if err := json.Unmarshal(trace.RawPointsJSON, &raw); err != nil {
		return dto.TraceDetail{}, nil, internal("decode raw trace points failed", err)
	}
	if len(trace.ProcessedJSON) > 0 {
		if err := json.Unmarshal(trace.ProcessedJSON, &processed); err != nil {
			return dto.TraceDetail{}, nil, internal("decode processed trace points failed", err)
		}
	}
	events, err := s.store.Events.ForTrace(id)
	if err != nil {
		return dto.TraceDetail{}, nil, internal("list trace events failed", err)
	}
	detail := dto.TraceDetail{ID: trace.ID, RouteID: trace.RouteID, WavelengthNM: trace.WavelengthNM, PulseWidthNS: trace.PulseWidthNS, SampleIntervalNS: trace.SampleIntervalNS, Points: raw, ProcessedPoints: processed, NoiseFloorDB: trace.NoiseFloorDB, DenoiseWindow: trace.DenoiseWindow, PeakThresholdDB: trace.PeakThresholdDB, MergeWindow: trace.MergeWindow, CapturedAt: trace.CapturedAt, UploadedBy: trace.UploadedBy}
	return detail, events, nil
}
