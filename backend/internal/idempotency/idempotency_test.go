package idempotency

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestValidateKey(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{name: "uuid style", key: "0c97c2e2-7d9e-4d2a-9b3a-1f2a3b4c5d6e"},
		{name: "opaque token", key: "Y2xpZW50LTQyLTg4MjM"},
		{name: "empty rejected", key: "", wantErr: true},
		{name: "control character rejected", key: "abc\x00def", wantErr: true},
		{name: "newline rejected", key: "abc\n", wantErr: true},
		{name: "overlong rejected", key: strings.Repeat("k", maxKeyLength+1), wantErr: true},
		{name: "max length accepted", key: strings.Repeat("k", maxKeyLength)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateKey(tc.key)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for key %q", tc.key)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

type fingerprintBody struct {
	RouteID uint      `json:"route_id"`
	Points  []float64 `json:"points"`
	Wave    int       `json:"wavelength_nm"`
	Ignored string    `json:"-"`
}

func TestFingerprintStableAndSensitive(t *testing.T) {
	body := fingerprintBody{RouteID: 7, Points: []float64{-12.1, -12.4, -30.2}, Wave: 1550}
	first, err := Fingerprint(body)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Fingerprint(body)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("fingerprint must be deterministic: %s != %s", first, second)
	}
	if len(first) != 64 {
		t.Fatalf("expected sha256 hex digest, got %q", first)
	}
	// Same data in a structurally equivalent value fingerprints identically.
	equivalent := map[string]any{"route_id": float64(7), "points": []any{float64(-12.1), float64(-12.4), float64(-30.2)}, "wavelength_nm": float64(1550)}
	other, err := Fingerprint(equivalent)
	if err != nil {
		t.Fatal(err)
	}
	if other != first {
		t.Fatalf("canonical JSON must yield equal fingerprints: %s != %s", other, first)
	}
	changed := body
	changed.Points = []float64{-12.1, -12.4, -29.9}
	conflicting, err := Fingerprint(changed)
	if err != nil {
		t.Fatal(err)
	}
	if conflicting == first {
		t.Fatal("changed samples must change the fingerprint")
	}
}

func TestKeyedLocksSerializesSameKey(t *testing.T) {
	locks := newKeyedLocks()
	var mu sync.Mutex
	inside := 0
	maxConcurrent := 0
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			locks.lock("scope\x00key")
			mu.Lock()
			inside++
			if inside > maxConcurrent {
				maxConcurrent = inside
			}
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
			mu.Lock()
			inside--
			mu.Unlock()
			locks.unlock("scope\x00key")
		}()
	}
	wg.Wait()
	if maxConcurrent != 1 {
		t.Fatalf("same key must be mutually exclusive, observed %d concurrent", maxConcurrent)
	}
	if len(locks.entries) != 0 {
		t.Fatalf("idle lock entries must be cleaned up, got %d", len(locks.entries))
	}
}

func TestKeyedLocksAllowsDistinctKeys(t *testing.T) {
	locks := newKeyedLocks()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for _, key := range []string{"a", "b"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			locks.lock(key)
			started <- struct{}{}
			<-release
			locks.unlock(key)
		}(key)
	}
	<-started
	<-started
	close(release)
	wg.Wait()
}
