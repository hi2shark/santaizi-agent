package identity

import (
	"encoding/hex"
	"sync"
	"testing"
)

func TestLoadOrCreateIsStable(t *testing.T) {
	dir := t.TempDir()
	first, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("identity changed: %x != %x", first, second)
	}
}

func TestEventIDUsesBinaryTuple(t *testing.T) {
	var node, session ID
	for i := range node {
		node[i] = byte(i)
		session[i] = byte(16 + i)
	}
	got := EventID(node, session, 0x0102030405060708)
	const want = "9e6cef1d125d09ba0c8e74e4983c5562"
	if hex.EncodeToString(got[:]) != want {
		t.Fatalf("EventID() = %x, want %s", got, want)
	}
}

func TestSessionSequenceIsStrictlyIncreasing(t *testing.T) {
	session, err := NewSession()
	if err != nil {
		t.Fatal(err)
	}
	const count = 64
	seen := make(map[uint64]bool, count)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sequence, _ := session.Next(ID{1})
			mu.Lock()
			seen[sequence] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	for sequence := uint64(1); sequence <= count; sequence++ {
		if !seen[sequence] {
			t.Fatalf("sequence %d missing", sequence)
		}
	}
}
