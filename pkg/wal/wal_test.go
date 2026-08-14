package wal

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/hi2shark/santaizi-agent/proto"
)

func testRecord(sequence uint64) *pb.TelemetryRecord {
	return &pb.TelemetryRecord{Record: &pb.TelemetryRecord_Event{Event: &pb.TelemetryEvent{
		EventId:   []byte{byte(sequence)},
		NodeUuid:  make([]byte, 16),
		SessionId: append(make([]byte, 15), 1),
		Sequence:  sequence,
		EventType: pb.TelemetryEventType_TELEMETRY_EVENT_TYPE_HEARTBEAT,
		Priority:  pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL,
		Payload:   &pb.TelemetryEvent_Heartbeat{Heartbeat: &pb.HeartbeatPayload{}},
	}}}
}

func TestAppendSyncReadAndRecover(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, SegmentSize: 256, MaxSize: 1 << 20, FsyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 5; sequence++ {
		if err := w.Append(testRecord(sequence), pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	records, err := w.ReadDurable()
	if err != nil || len(records) != 5 {
		t.Fatalf("ReadDurable() count=%d err=%v", len(records), err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = Open(Config{Dir: dir, SegmentSize: 256, MaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	records, err = w.ReadDurable()
	if err != nil || len(records) != 5 {
		t.Fatalf("recovered count=%d err=%v", len(records), err)
	}
}

func TestRecoveryTruncatesPartialTail(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, MaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(testRecord(1), pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, segmentName(1))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.Write([]byte{0x53, 0x54, 0x5a})
	_ = file.Close()

	w, err = Open(Config{Dir: dir, MaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(w.RecoveryIssues()) != 1 || w.RecoveryIssues()[0].Reason != "truncated_partial_tail" {
		t.Fatalf("issues = %#v", w.RecoveryIssues())
	}
}

func TestRecoveryQuarantineCarriesManifestRangesForExplicitGap(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, MaxSize: 1 << 20, FsyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(7); sequence <= 9; sequence++ {
		if err := w.Append(testRecord(sequence), pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, segmentName(1))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	w, err = Open(Config{Dir: dir, MaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	issues := w.RecoveryIssues()
	if len(issues) != 1 || len(issues[0].Ranges) != 1 {
		t.Fatalf("issues=%#v", issues)
	}
	sequenceRange := issues[0].Ranges[0]
	if sequenceRange.StartSequence != 7 || sequenceRange.EndSequence != 9 {
		t.Fatalf("range=%#v", sequenceRange)
	}
}

func TestPressureRejectsOptionalRecords(t *testing.T) {
	w, err := Open(Config{Dir: t.TempDir(), SegmentSize: 4096, MaxSize: 512, ReserveBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var got error
	for i := 0; i < 20; i++ {
		got = w.Append(testRecord(uint64(i+1)), pb.TelemetryPriority_TELEMETRY_PRIORITY_P3_OPTIONAL)
		if got != nil {
			break
		}
	}
	if !errors.Is(got, ErrDownsampled) && !errors.Is(got, ErrNeedsRollup) && !errors.Is(got, ErrHardLimit) {
		t.Fatalf("expected pressure error, got %v", got)
	}
}

func TestEmergencyReserveRejectsP1BeforeP0(t *testing.T) {
	w, err := Open(Config{Dir: t.TempDir(), SegmentSize: 4096, MaxSize: 2048, ReserveBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for sequence := uint64(1); ; sequence++ {
		err = w.Append(testRecord(sequence), pb.TelemetryPriority_TELEMETRY_PRIORITY_P1_IMPORTANT)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, ErrHardLimit) {
		t.Fatalf("P1 should stop at emergency reserve: %v", err)
	}
	if err := w.Append(testRecord(1000), pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL); err != nil {
		t.Fatalf("P0 could not use emergency reserve: %v", err)
	}
}

func TestStateStorePersistsIndependentCursors(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertEndpoint(EndpointState{EndpointID: "primary", Generation: 1, Reliable: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertEndpoint(EndpointState{EndpointID: "hk", Generation: 2, Reliable: true}); err != nil {
		t.Fatal(err)
	}
	session := []byte{1, 2, 3}
	if err := store.UpdateAck("primary", 1, session, 100); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAck("hk", 2, session, 50); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.Ack("primary", session); got != 100 {
		t.Fatalf("primary ack=%d", got)
	}
	if got := reopened.Ack("hk", session); got != 50 {
		t.Fatalf("hk ack=%d", got)
	}
}

func TestEndpointActivationSurvivesAddressRefresh(t *testing.T) {
	store, err := OpenStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session := []byte{1, 2, 3}
	key := hex.EncodeToString(session)
	if err := store.UpsertEndpoint(EndpointState{
		EndpointID: "collector-a", Generation: 4, Reliable: true,
		ActivationSession: key, ActivationSequence: 20,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAck("collector-a", 4, session, 25); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertEndpoint(EndpointState{EndpointID: "collector-a", Generation: 4, Reliable: true}); err != nil {
		t.Fatal(err)
	}
	snapshot := store.Snapshot().Endpoints["collector-a"]
	if snapshot.Activations[key] != 20 || snapshot.Cursors[key] != 25 {
		t.Fatalf("activation/cursor lost after same-generation refresh: %#v", snapshot)
	}
}

func TestDeletedEndpointReaddedWithNewGenerationStartsFreshObligation(t *testing.T) {
	store, err := OpenStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session := []byte{7, 8, 9}
	key := hex.EncodeToString(session)
	if err := store.UpsertEndpoint(EndpointState{
		EndpointID: "collector-a", Generation: 1, Reliable: true,
		ActivationSession: key, ActivationSequence: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAck("collector-a", 1, session, 100); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveEndpoint("collector-a"); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertEndpoint(EndpointState{
		EndpointID: "collector-a", Generation: 2, Reliable: true,
		ActivationSession: key, ActivationSequence: 150,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := store.Snapshot().Endpoints["collector-a"]
	if snapshot.Generation != 2 || snapshot.Activations[key] != 150 || snapshot.Cursors[key] != 149 {
		t.Fatalf("readded endpoint retained old obligation: %#v", snapshot)
	}
}

func TestSnapshotCloneDoesNotShareMaps(t *testing.T) {
	store, err := OpenStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session := []byte{1, 2, 3}
	key := hex.EncodeToString(session)
	if err := store.UpsertEndpoint(EndpointState{EndpointID: "primary", Generation: 1, Reliable: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAck("primary", 1, session, 7); err != nil {
		t.Fatal(err)
	}
	snapshot := store.Snapshot()
	snapshot.Endpoints["primary"].Cursors[key] = 99
	if got := store.Ack("primary", session); got != 7 {
		t.Fatalf("snapshot mutation leaked into store: %d", got)
	}
	cloned := store.Endpoint("primary")
	cloned.Cursors[key] = 100
	if got := store.Ack("primary", session); got != 7 {
		t.Fatalf("endpoint clone leaked into store: %d", got)
	}
}

func TestCollectPendingStopsAtLimit(t *testing.T) {
	w, err := Open(Config{Dir: t.TempDir(), MaxSize: 1 << 20, FsyncInterval: time.Hour, FsyncRecords: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for sequence := uint64(1); sequence <= 6; sequence++ {
		if err := w.Append(testRecord(sequence), pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	got := w.CollectPending(func(record *pb.TelemetryRecord) bool {
		return record.GetEvent().GetSequence() >= 4
	}, 2)
	if len(got) != 2 || got[0].GetEvent().GetSequence() != 4 || got[1].GetEvent().GetSequence() != 5 {
		t.Fatalf("pending=%#v", got)
	}
	if !w.HasPending(func(session string, maxSeq uint64) bool { return maxSeq > 3 }) {
		t.Fatal("heads should report pending")
	}
	if w.HasPending(func(session string, maxSeq uint64) bool { return maxSeq > 100 }) {
		t.Fatal("caught-up predicate should be empty")
	}
}

func TestPressureWatermarks(t *testing.T) {
	tests := []struct {
		size int64
		want Pressure
	}{
		{0, PressureHealthy}, {699, PressureHealthy}, {700, PressureDownsample},
		{850, PressureRollup}, {950, PressureCritical}, {1000, PressureHardLimit},
	}
	for _, test := range tests {
		if got := pressureFor(test.size, 1000); got != test.want {
			t.Fatalf("size=%d pressure=%v want=%v", test.size, got, test.want)
		}
	}
}

func TestReclaimDeletesOnlyFullyAcknowledgedInactiveSegments(t *testing.T) {
	w, err := Open(Config{Dir: t.TempDir(), SegmentSize: 180, MaxSize: 1 << 20, FsyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for sequence := uint64(1); sequence <= 12; sequence++ {
		if err := w.Append(testRecord(sequence), pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	before := w.Size()
	reclaimed, err := w.Reclaim(func(record *pb.TelemetryRecord) bool {
		return record.GetEvent().GetSequence() <= 8
	})
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed == 0 || w.Size() >= before {
		t.Fatalf("reclaimed=%d before=%d after=%d", reclaimed, before, w.Size())
	}
	for _, record := range mustRead(t, w) {
		if record.GetEvent().GetSequence() <= 8 {
			t.Fatalf("acknowledged record %d remained in a reclaimable segment", record.GetEvent().GetSequence())
		}
	}
}

func TestReadDurableOmitsUnsyncedAndCachesAfterSync(t *testing.T) {
	w, err := Open(Config{Dir: t.TempDir(), MaxSize: 1 << 20, FsyncInterval: time.Hour, FsyncRecords: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(testRecord(1), pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL); err != nil {
		t.Fatal(err)
	}
	if records := mustRead(t, w); len(records) != 0 {
		t.Fatalf("unsynced records leaked: %d", len(records))
	}
	if len(w.DurableHeads()) != 0 {
		t.Fatalf("heads before sync = %#v", w.DurableHeads())
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if records := mustRead(t, w); len(records) != 1 {
		t.Fatalf("synced count=%d", len(records))
	}
	session, _, end := RecordRange(mustRead(t, w)[0])
	if heads := w.DurableHeads(); heads[session] != end || end != 1 {
		t.Fatalf("heads=%#v session=%s end=%d", w.DurableHeads(), session, end)
	}
}

func TestReadDurableAfterRecoverMatchesDisk(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, MaxSize: 1 << 20, FsyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 4; sequence++ {
		if err := w.Append(testRecord(sequence), pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = Open(Config{Dir: dir, MaxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	records := mustRead(t, w)
	if len(records) != 4 {
		t.Fatalf("recovered count=%d", len(records))
	}
	session, _, end := RecordRange(records[3])
	if w.DurableHeads()[session] != end {
		t.Fatalf("recovered heads=%#v", w.DurableHeads())
	}
}

func BenchmarkReadDurable(b *testing.B) {
	w, err := Open(Config{Dir: b.TempDir(), SegmentSize: 1 << 20, MaxSize: 32 << 20, FsyncInterval: time.Hour, FsyncRecords: 100000})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	const n = 2000
	for sequence := uint64(1); sequence <= n; sequence++ {
		if err := w.Append(testRecord(sequence), pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records, err := w.ReadDurable()
		if err != nil || len(records) != n {
			b.Fatalf("count=%d err=%v", len(records), err)
		}
	}
}

func mustRead(t *testing.T, w *WAL) []*pb.TelemetryRecord {
	t.Helper()
	records, err := w.ReadDurable()
	if err != nil {
		t.Fatal(err)
	}
	return records
}
