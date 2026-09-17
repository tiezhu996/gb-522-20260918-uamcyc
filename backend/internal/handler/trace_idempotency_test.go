package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fiber-otdr-fault-localization/backend/internal/constants"
	"fiber-otdr-fault-localization/backend/internal/idempotency"
	appmw "fiber-otdr-fault-localization/backend/internal/middleware"
	"fiber-otdr-fault-localization/backend/internal/model"
	"fiber-otdr-fault-localization/backend/internal/repository"
	"fiber-otdr-fault-localization/backend/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type importEnvelope struct {
	Data struct {
		ID uint `json:"id"`
	} `json:"data"`
	RequestID string `json:"request_id"`
	Error     *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func importBodyJSON(seed float64) []byte {
	points := make([]float64, 64)
	for i := range points {
		points[i] = -12 - float64(i%5)*0.2 + seed
	}
	body := map[string]any{
		"route_id":           1,
		"wavelength_nm":      1550,
		"pulse_width_ns":     100,
		"sample_interval_ns": 10,
		"points":             points,
		"captured_at":        time.Date(2026, 9, 17, 8, 0, 0, 0, time.UTC).Format(time.RFC3339),
	}
	raw, _ := json.Marshal(body)
	return raw
}

func newImportRouter(t *testing.T) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	dsn := fmt.Sprintf("file:handler-import-%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&model.User{}, &model.FiberRoute{}, &model.TraceCapture{}, &model.EventMarker{}, &model.LocalizationCase{}, &model.AuditLog{}, &model.IdempotencyRecord{}); err != nil {
		t.Fatal(err)
	}
	user := model.User{Username: "analyst", DisplayName: "分析员", Role: constants.RoleAnalyst, PasswordHash: "x", Active: true}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	route := model.FiberRoute{RouteCode: "R-H1", Name: "集成线路", LengthM: 1000, RefractiveIndex: 1.46, LaunchConnector: "SC/APC", RouteStatus: "active"}
	if err := db.Create(&route).Error; err != nil {
		t.Fatal(err)
	}
	store := repository.NewStore(db)
	traceService := service.NewTraceService(store, 20000, idempotency.NewProtector(store))
	validate := validator.New(validator.WithRequiredStructEnabled())
	traceHandler := NewTraceHandler(traceService, validate)

	engine := gin.New()
	engine.Use(appmw.RequestIDMiddleware())
	engine.POST("/traces/import",
		appmw.RateLimitMiddleware(appmw.NewRateLimiter(1000, time.Minute), "trace_import"),
		func(c *gin.Context) { // stand-in for AuthMiddleware
			c.Set("user_id", user.ID)
			c.Set("username", user.Username)
			c.Set("role", c.GetHeader("X-Test-Role"))
			c.Next()
		},
		appmw.RBACMiddleware(constants.RoleAnalyst, constants.RoleAdmin),
		appmw.IdempotencyKeyMiddleware(),
		traceHandler.Import)
	return engine
}

func doImport(t *testing.T, engine *gin.Engine, key string, body []byte, role string) (int, importEnvelope, http.Header) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/traces/import", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(idempotency.HeaderIdempotencyKey, key)
	}
	req.Header.Set("X-Test-Role", role)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	var envelope importEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return rec.Code, envelope, rec.Header()
}

func TestImportHTTPIdempotencyFlow(t *testing.T) {
	engine := newImportRouter(t)
	body := importBodyJSON(0)

	// Missing key is rejected before validation work, envelope unchanged.
	status, envelope, _ := doImport(t, engine, "", body, constants.RoleAnalyst)
	if status != http.StatusBadRequest || envelope.Error == nil || envelope.Error.Code != service.CodeInvalidInput {
		t.Fatalf("missing key must be 400 INVALID_INPUT, got %d %+v", status, envelope.Error)
	}

	// First create.
	status, first, header := doImport(t, engine, "http-key-1", body, constants.RoleAnalyst)
	if status != http.StatusCreated || first.Data.ID == 0 {
		t.Fatalf("first import must be 201 with trace id, got %d %+v", status, first)
	}
	if header.Get(idempotency.HeaderReplay) != "" {
		t.Fatal("first import must not carry a replay marker")
	}

	// Same key + same sample replays the first trace.
	status, replay, header := doImport(t, engine, "http-key-1", body, constants.RoleAnalyst)
	if status != http.StatusCreated || replay.Data.ID != first.Data.ID {
		t.Fatalf("replay must read back first trace %d, got %d %+v", first.Data.ID, replay.Data.ID, replay)
	}
	if header.Get(idempotency.HeaderReplay) != "true" {
		t.Fatalf("replay must set %s=true, got %q", idempotency.HeaderReplay, header.Get(idempotency.HeaderReplay))
	}

	// Same key, changed samples conflicts.
	status, conflict, _ := doImport(t, engine, "http-key-1", importBodyJSON(4.25), constants.RoleAnalyst)
	if status != http.StatusConflict || conflict.Error == nil || conflict.Error.Code != idempotency.CodeConflict {
		t.Fatalf("changed samples must be 409 %s, got %d %+v", idempotency.CodeConflict, status, conflict.Error)
	}

	// A fresh key creates independently.
	status, second, _ := doImport(t, engine, "http-key-2", body, constants.RoleAnalyst)
	if status != http.StatusCreated || second.Data.ID == first.Data.ID {
		t.Fatalf("new key must create a new trace, got %d first=%d second=%d", status, first.Data.ID, second.Data.ID)
	}
}

func TestImportHTTPKeepsValidationAndRBAC(t *testing.T) {
	engine := newImportRouter(t)

	// Field validation still applies after the key gate.
	invalid := bytes.Replace(importBodyJSON(0), []byte(`"wavelength_nm":1550`), []byte(`"wavelength_nm":850`), 1)
	status, envelope, _ := doImport(t, engine, "http-key-invalid", invalid, constants.RoleAnalyst)
	if status != http.StatusBadRequest || envelope.Error == nil || envelope.Error.Code != service.CodeInvalidInput {
		t.Fatalf("bad wavelength must stay 400 INVALID_INPUT, got %d %+v", status, envelope.Error)
	}

	// RBAC is unchanged: reviewer cannot import.
	status, envelope, _ = doImport(t, engine, "http-key-forbidden", importBodyJSON(0), constants.RoleReviewer)
	if status != http.StatusForbidden || envelope.Error == nil || envelope.Error.Code != service.CodeForbidden {
		t.Fatalf("reviewer must be 403 FORBIDDEN, got %d %+v", status, envelope.Error)
	}
}
