// Package idempotency provides a reusable, storage-backed idempotency core.
//
// The protector guarantees that a request carrying an idempotency key is
// applied at most once: concurrent duplicates are serialized, the created
// resource is re-read from its origin (never a stored payload copy), and a
// key reused with a different payload fingerprint conflicts without touching
// the original resource or audit trail.
//
// The durable unique index on (scope, key) is the cross-process authority;
// the in-process keyed lock additionally collapses same-process races
// without spending a failed INSERT.
package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
	"unicode"

	"fiber-otdr-fault-localization/backend/internal/model"
	"fiber-otdr-fault-localization/backend/internal/repository"
)

const (
	HeaderIdempotencyKey = "Idempotency-Key"
	ContextKey           = "idempotency_key"
	HeaderReplay         = "Idempotency-Replay"

	maxKeyLength = 128

	// ScopeTraceImport guards offline trace imports.
	ScopeTraceImport = "trace.import"

	CodeConflict = "IDEMPOTENCY_CONFLICT"
	CodePending  = "IDEMPOTENCY_PENDING"
	CodeInvalid  = "INVALID_INPUT"
)

// Error is the transport-agnostic error contract of the core. Callers at the
// edge (HTTP handlers/services) translate it into their own error envelope
// without the core depending on any framework or service package.
type Error struct {
	Code    string
	Status  int
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Err }

// ValidationError reports a malformed idempotency key.
func ValidationError(message string) *Error {
	return &Error{Code: CodeInvalid, Status: http.StatusBadRequest, Message: message}
}

func conflictError(err error) *Error {
	return &Error{Code: CodeConflict, Status: http.StatusConflict, Message: "idempotency key was already used with a different request payload", Err: err}
}

func pendingError(err error) *Error {
	return &Error{Code: CodePending, Status: http.StatusConflict, Message: "idempotency key is still being processed by an earlier request", Err: err}
}

// ValidateKey enforces the client contract for the key header: present,
// 1..128 characters, and free of control characters.
func ValidateKey(key string) error {
	if key == "" {
		return ValidationError(HeaderIdempotencyKey + " header is required for this operation")
	}
	if len(key) > maxKeyLength {
		return ValidationError(fmt.Sprintf("%s header must not exceed %d bytes", HeaderIdempotencyKey, maxKeyLength))
	}
	for _, r := range key {
		if unicode.IsControl(r) {
			return ValidationError(HeaderIdempotencyKey + " header must not contain control characters")
		}
	}
	return nil
}

// Fingerprint returns the deterministic SHA-256 fingerprint of a canonical
// JSON encoding of the payload. Two requests with the same sampled data and
// parameters fingerprint identically; any changed field does not.
func Fingerprint(payload any) (string, error) {
	encoded, err := canonicalJSON(payload)
	if err != nil {
		return "", fmt.Errorf("encode idempotency fingerprint: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// canonicalJSON encodes through encoding/json. Unmarshalling to any sorts
// object keys, so the digest is stable regardless of DTO vs map origin.
func canonicalJSON(payload any) ([]byte, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var decoded any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	return json.Marshal(decoded)
}

// AsError unwraps err to a core *Error when present.
func AsError(err error) (*Error, bool) {
	var coreErr *Error
	if errors.As(err, &coreErr) {
		return coreErr, true
	}
	return nil, false
}

// ---------------------------------------------------------------------------
// In-process keyed locks
// ---------------------------------------------------------------------------

// keyedLocks gives concurrent callers sharing an idempotency key one mutex
// per key without growing unbounded: an entry is removed on the last unlock
// of the moment, under the registry lock.
type keyedLocks struct {
	mu      sync.Mutex
	entries map[string]*lockEntry
}

type lockEntry struct {
	lock  sync.Mutex
	users int
}

func newKeyedLocks() *keyedLocks {
	return &keyedLocks{entries: make(map[string]*lockEntry)}
}

func (l *keyedLocks) lock(key string) {
	l.mu.Lock()
	entry, ok := l.entries[key]
	if !ok {
		entry = &lockEntry{}
		l.entries[key] = entry
	}
	entry.users++
	l.mu.Unlock()
	entry.lock.Lock()
}

func (l *keyedLocks) unlock(key string) {
	l.mu.Lock()
	if entry, ok := l.entries[key]; ok {
		entry.users--
		if entry.users == 0 {
			delete(l.entries, key)
		}
		l.mu.Unlock()
		entry.lock.Unlock()
		return
	}
	l.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Protector
// ---------------------------------------------------------------------------

const (
	defaultWaitTimeout = 10 * time.Second
	defaultWaitTick    = 25 * time.Millisecond
)

// Outcome describes what Execute did for the current caller.
type Outcome[T any] struct {
	// Result is the business value, both for a freshly applied request and
	// for a replay (loaded from the resource created by the first request).
	Result T
	// Replayed is false for the single leader that applied the request and
	// true for every duplicate resolving the stored resource.
	Replayed bool
	// RecordID and RequestID identify the durable record and the first
	// request that owns it.
	RecordID  uint
	RequestID string
}

// BusinessFunc applies the guarded operation inside the leader transaction.
type BusinessFunc[T any] func(tx *repository.Store) (T, ResourceRef, error)

// ReplayFunc resolves the stored resource for a duplicate request. It runs
// outside the leader transaction and must return the value the first caller
// received, read straight from the resource itself.
type ReplayFunc[T any] func(store *repository.Store, ref ResourceRef) (T, error)

// ResourceRef describes the resource created by a guarded operation.
type ResourceRef struct {
	kind string
	id   uint
}

// NewResourceRef builds a ResourceRef from the resource type and id.
func NewResourceRef(kind string, id uint) ResourceRef {
	return ResourceRef{kind: kind, id: id}
}

// Kind and ID expose the reference to the replay resolver.
func (r ResourceRef) Kind() string { return r.kind }
func (r ResourceRef) ID() uint     { return r.id }

// Protector serializes keyed requests and stores the canonical resource ref.
type Protector struct {
	store       *repository.Store
	locks       *keyedLocks
	waitTimeout time.Duration
	waitTick    time.Duration
}

// NewProtector builds the reusable core with production wait settings.
func NewProtector(store *repository.Store) *Protector {
	return &Protector{store: store, locks: newKeyedLocks(), waitTimeout: defaultWaitTimeout, waitTick: defaultWaitTick}
}

// Execute returns the one and only result for (scope, key). The first caller
// applies business and records the resource reference; concurrent or later
// callers with the same fingerprint resolve that reference; a different
// fingerprint conflicts. A business error rolls the whole transaction
// (resource and audit) back and releases the key.
func Execute[T any](p *Protector, scope, key string, payload any, requestID string, business BusinessFunc[T], replay ReplayFunc[T]) (Outcome[T], error) {
	if err := ValidateKey(key); err != nil {
		return Outcome[T]{}, err
	}
	fingerprint, err := Fingerprint(payload)
	if err != nil {
		return Outcome[T]{}, ValidationError("idempotency payload could not be fingerprinted")
	}
	lockToken := scope + "\x00" + key
	p.locks.lock(lockToken)
	defer p.locks.unlock(lockToken)

	record, err := applyAsLeader[T](p, fingerprint, scope, key, requestID, business)
	if err == nil {
		return record, nil
	}
	if !errors.Is(err, repository.ErrIdempotencyKeyExists) {
		return Outcome[T]{}, err
	}
	return replayOrConflict[T](p, scope, key, fingerprint, replay)
}

// applyAsLeader owns the key inside a single transaction: the pending row,
// the resource, its audit entry and the completion update commit together.
func applyAsLeader[T any](p *Protector, fingerprint, scope, key, requestID string, business BusinessFunc[T]) (Outcome[T], error) {
	var outcome Outcome[T]
	err := p.store.Transaction(func(tx *repository.Store) error {
		record := &model.IdempotencyRecord{
			Scope:       scope,
			Key:         key,
			Fingerprint: fingerprint,
			RequestID:   requestID,
		}
		if err := tx.Idempotencies.CreatePending(record); err != nil {
			return err
		}
		result, ref, err := business(tx)
		if err != nil {
			return err
		}
		if err := tx.Idempotencies.Complete(record.ID, ref.kind, ref.id); err != nil {
			return err
		}
		outcome = Outcome[T]{Result: result, Replayed: false, RecordID: record.ID, RequestID: requestID}
		return nil
	})
	return outcome, err
}

// replayOrConflict serves a key already owned by another request. A committed
// row is resolved through the replay function; an in-flight row is awaited.
func replayOrConflict[T any](p *Protector, scope, key, fingerprint string, replay ReplayFunc[T]) (Outcome[T], error) {
	deadline := time.Now().Add(p.waitTimeout)
	for {
		record, err := p.store.Idempotencies.Get(scope, key)
		if errors.Is(err, repository.ErrNotFound) {
			// The leader rolled back (cross-process race): its key is free.
			return Outcome[T]{}, pendingError(nil)
		}
		if err != nil {
			return Outcome[T]{}, fmt.Errorf("load idempotency record: %w", err)
		}
		if record.Fingerprint != fingerprint {
			return Outcome[T]{}, conflictError(nil)
		}
		if record.Status == model.IdempotencyStatusCompleted {
			result, err := replay(p.store, NewResourceRef(record.ResourceType, record.ResourceID))
			if err != nil {
				return Outcome[T]{}, fmt.Errorf("resolve idempotent resource: %w", err)
			}
			return Outcome[T]{Result: result, Replayed: true, RecordID: record.ID, RequestID: record.RequestID}, nil
		}
		if time.Now().After(deadline) {
			return Outcome[T]{}, pendingError(nil)
		}
		time.Sleep(p.waitTick)
	}
}
