package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/hi2shark/santaizi-agent/proto"
	"google.golang.org/protobuf/proto"
)

const (
	frameMagic      uint32 = 0x53545a32 // STZ2
	frameVersion    uint16 = 1
	frameHeaderSize        = 16

	DefaultSegmentSize = int64(8 << 20)
	DefaultMaxSize     = int64(256 << 20)
	defaultReserve     = int64(1 << 20)
)

var (
	ErrDownsampled = errors.New("wal pressure requires optional telemetry downsampling")
	ErrNeedsRollup = errors.New("wal pressure requires telemetry rollup")
	ErrHardLimit   = errors.New("wal hard limit reached")
)

type Pressure int

const (
	PressureHealthy Pressure = iota
	PressureDownsample
	PressureRollup
	PressureCritical
	PressureHardLimit
)

type Config struct {
	Dir           string
	SegmentSize   int64
	MaxSize       int64
	FsyncInterval time.Duration
	FsyncRecords  int
	ReserveBytes  int64
}

func (c *Config) normalize() {
	if c.SegmentSize <= 0 {
		c.SegmentSize = DefaultSegmentSize
	}
	if c.MaxSize <= 0 {
		c.MaxSize = DefaultMaxSize
	}
	if c.FsyncInterval <= 0 {
		c.FsyncInterval = time.Second
	}
	if c.FsyncRecords <= 0 {
		c.FsyncRecords = 64
	}
	if c.ReserveBytes <= 0 {
		c.ReserveBytes = defaultReserve
	}
	if c.ReserveBytes >= c.MaxSize {
		c.ReserveBytes = c.MaxSize / 100
	}
}

type RecoveryIssue struct {
	Segment string
	Reason  string
	Ranges  []SequenceRange
}

type SequenceRange struct {
	NodeUUID      []byte `json:"node_uuid"`
	SessionID     []byte `json:"session_id"`
	StartSequence uint64 `json:"start_sequence"`
	EndSequence   uint64 `json:"end_sequence"`
}

type segmentManifest struct {
	Ranges map[string]SequenceRange `json:"ranges"`
}

type segment struct {
	id          uint64
	path        string
	size        int64
	durableSize int64
	ranges      map[string]SequenceRange
}

type WAL struct {
	mu sync.Mutex

	cfg      Config
	segments []*segment
	active   *os.File
	buffer   *bufio.Writer
	unsynced int
	total    int64
	issues   []RecoveryIssue

	stopCh chan struct{}
	doneCh chan struct{}
}

func Open(cfg Config) (*WAL, error) {
	cfg.normalize()
	if cfg.Dir == "" {
		return nil, errors.New("wal directory is empty")
	}
	if err := os.MkdirAll(cfg.Dir, 0700); err != nil {
		return nil, fmt.Errorf("create wal directory: %w", err)
	}
	w := &WAL{cfg: cfg, stopCh: make(chan struct{}), doneCh: make(chan struct{})}
	if err := w.recover(); err != nil {
		return nil, err
	}
	if err := w.openActive(); err != nil {
		return nil, err
	}
	go w.syncLoop()
	return w, nil
}

func (w *WAL) RecoveryIssues() []RecoveryIssue {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]RecoveryIssue(nil), w.issues...)
}

func (w *WAL) Pressure() Pressure {
	w.mu.Lock()
	defer w.mu.Unlock()
	return pressureFor(w.total, w.cfg.MaxSize)
}

func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.total
}

func pressureFor(size, max int64) Pressure {
	if max <= 0 || size >= max {
		return PressureHardLimit
	}
	ratio := float64(size) / float64(max)
	switch {
	case ratio >= .95:
		return PressureCritical
	case ratio >= .85:
		return PressureRollup
	case ratio >= .70:
		return PressureDownsample
	default:
		return PressureHealthy
	}
}

func (w *WAL) Append(record *pb.TelemetryRecord, priority pb.TelemetryPriority) error {
	payload, err := proto.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal wal record: %w", err)
	}
	frameSize := int64(frameHeaderSize + len(payload))

	w.mu.Lock()
	defer w.mu.Unlock()
	pressure := pressureFor(w.total+frameSize, w.cfg.MaxSize)
	if pressure == PressureHardLimit {
		return ErrHardLimit
	}
	if w.total+frameSize > w.cfg.MaxSize-w.cfg.ReserveBytes && priority > pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL {
		return ErrHardLimit
	}
	if pressure >= PressureRollup && priority >= pb.TelemetryPriority_TELEMETRY_PRIORITY_P2_NORMAL {
		return ErrNeedsRollup
	}
	if pressure >= PressureDownsample && priority >= pb.TelemetryPriority_TELEMETRY_PRIORITY_P3_OPTIONAL {
		return ErrDownsampled
	}
	if w.segments[len(w.segments)-1].size > 0 && w.segments[len(w.segments)-1].size+frameSize > w.cfg.SegmentSize {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}

	var header [frameHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], frameMagic)
	binary.BigEndian.PutUint16(header[4:6], frameVersion)
	binary.BigEndian.PutUint16(header[6:8], uint16(priority))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[12:16], crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
	if _, err := w.buffer.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.buffer.Write(payload); err != nil {
		return err
	}
	active := w.segments[len(w.segments)-1]
	active.size += frameSize
	w.total += frameSize
	w.unsynced++
	addRecordRange(active, record)
	if w.unsynced >= w.cfg.FsyncRecords {
		return w.syncLocked()
	}
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

func (w *WAL) syncLocked() error {
	if w.unsynced == 0 {
		return nil
	}
	if err := w.buffer.Flush(); err != nil {
		return err
	}
	if err := w.active.Sync(); err != nil {
		return err
	}
	active := w.segments[len(w.segments)-1]
	active.durableSize = active.size
	if err := writeManifest(active.path+".meta", active.ranges); err != nil {
		return err
	}
	w.unsynced = 0
	return nil
}

func (w *WAL) ReadDurable() ([]*pb.TelemetryRecord, error) {
	w.mu.Lock()
	segments := make([]segment, len(w.segments))
	for i, item := range w.segments {
		segments[i] = *item
	}
	w.mu.Unlock()

	var records []*pb.TelemetryRecord
	for _, item := range segments {
		if item.durableSize == 0 {
			continue
		}
		part, _, err := scanFile(item.path, item.durableSize)
		if err != nil {
			return nil, err
		}
		records = append(records, part...)
	}
	return records, nil
}

// Reclaim removes only fully acknowledged, inactive segments. It never rewrites
// records, so a crash cannot turn garbage collection into a sequence hole.
func (w *WAL) Reclaim(acknowledged func(*pb.TelemetryRecord) bool) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.syncLocked(); err != nil {
		return 0, err
	}
	var reclaimed int64
	kept := make([]*segment, 0, len(w.segments))
	for index, item := range w.segments {
		if index == len(w.segments)-1 {
			kept = append(kept, item)
			continue
		}
		records, _, err := scanFile(item.path, item.durableSize)
		if err != nil {
			return reclaimed, err
		}
		allAcknowledged := len(records) > 0
		for _, record := range records {
			if !acknowledged(record) {
				allAcknowledged = false
				break
			}
		}
		if !allAcknowledged {
			kept = append(kept, item)
			continue
		}
		if err := os.Remove(item.path); err != nil {
			return reclaimed, err
		}
		if err := os.Remove(item.path + ".meta"); err != nil && !errors.Is(err, os.ErrNotExist) {
			return reclaimed, err
		}
		reclaimed += item.size
	}
	w.segments = kept
	w.total -= reclaimed
	return reclaimed, nil
}

func (w *WAL) Close() error {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}
	<-w.doneCh
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.syncLocked(); err != nil {
		_ = w.active.Close()
		return err
	}
	return w.active.Close()
}

func (w *WAL) syncLoop() {
	ticker := time.NewTicker(w.cfg.FsyncInterval)
	defer func() {
		ticker.Stop()
		close(w.doneCh)
	}()
	for {
		select {
		case <-ticker.C:
			_ = w.Sync()
		case <-w.stopCh:
			return
		}
	}
}

func (w *WAL) recover() error {
	entries, err := os.ReadDir(w.cfg.Dir)
	if err != nil {
		return err
	}
	var paths []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".seg") {
			continue
		}
		paths = append(paths, filepath.Join(w.cfg.Dir, entry.Name()))
	}
	sort.Strings(paths)
	for i, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		records, validSize, scanErr := scanFile(path, info.Size())
		ranges := rangesFromRecords(records)
		if scanErr != nil {
			if errors.Is(scanErr, io.ErrUnexpectedEOF) && i == len(paths)-1 {
				if err := os.Truncate(path, validSize); err != nil {
					return err
				}
				w.issues = append(w.issues, RecoveryIssue{Segment: filepath.Base(path), Reason: "truncated_partial_tail"})
				info, _ = os.Stat(path)
			} else {
				if manifestRanges, err := readManifest(path + ".meta"); err == nil {
					ranges = manifestRanges
				}
				quarantine := path + ".corrupt-" + strconv.FormatInt(time.Now().UnixNano(), 10)
				if err := os.Rename(path, quarantine); err != nil {
					return err
				}
				if err := os.Rename(path+".meta", quarantine+".meta"); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				w.issues = append(w.issues, RecoveryIssue{Segment: filepath.Base(path), Reason: scanErr.Error(), Ranges: rangeValues(ranges)})
				continue
			}
		}
		id, err := segmentID(filepath.Base(path))
		if err != nil {
			return err
		}
		item := &segment{id: id, path: path, size: info.Size(), durableSize: info.Size(), ranges: ranges}
		if err := writeManifest(path+".meta", ranges); err != nil {
			return err
		}
		w.segments = append(w.segments, item)
		w.total += item.size
	}
	return nil
}

func (w *WAL) openActive() error {
	var item *segment
	if len(w.segments) == 0 {
		item = &segment{id: 1, path: filepath.Join(w.cfg.Dir, segmentName(1)), ranges: make(map[string]SequenceRange)}
		w.segments = append(w.segments, item)
	} else {
		item = w.segments[len(w.segments)-1]
	}
	file, err := os.OpenFile(item.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	w.active = file
	w.buffer = bufio.NewWriterSize(file, 64<<10)
	return nil
}

func (w *WAL) rotateLocked() error {
	if err := w.syncLocked(); err != nil {
		return err
	}
	if err := w.active.Close(); err != nil {
		return err
	}
	id := w.segments[len(w.segments)-1].id + 1
	item := &segment{id: id, path: filepath.Join(w.cfg.Dir, segmentName(id)), ranges: make(map[string]SequenceRange)}
	w.segments = append(w.segments, item)
	file, err := os.OpenFile(item.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	w.active = file
	w.buffer = bufio.NewWriterSize(file, 64<<10)
	return nil
}

func addRecordRange(item *segment, record *pb.TelemetryRecord) {
	if item.ranges == nil {
		item.ranges = make(map[string]SequenceRange)
	}
	var nodeUUID, sessionID []byte
	var start, end uint64
	if event := record.GetEvent(); event != nil {
		nodeUUID, sessionID, start, end = event.GetNodeUuid(), event.GetSessionId(), event.GetSequence(), event.GetSequence()
	} else if gap := record.GetGap(); gap != nil {
		nodeUUID, sessionID, start, end = gap.GetNodeUuid(), gap.GetSessionId(), gap.GetStartSequence(), gap.GetEndSequence()
	}
	if len(nodeUUID) != 16 || len(sessionID) != 16 || start == 0 || end < start {
		return
	}
	key := hex.EncodeToString(nodeUUID) + "/" + hex.EncodeToString(sessionID)
	current, ok := item.ranges[key]
	if !ok {
		item.ranges[key] = SequenceRange{NodeUUID: append([]byte(nil), nodeUUID...), SessionID: append([]byte(nil), sessionID...), StartSequence: start, EndSequence: end}
		return
	}
	if start < current.StartSequence {
		current.StartSequence = start
	}
	if end > current.EndSequence {
		current.EndSequence = end
	}
	item.ranges[key] = current
}

func rangesFromRecords(records []*pb.TelemetryRecord) map[string]SequenceRange {
	item := &segment{ranges: make(map[string]SequenceRange)}
	for _, record := range records {
		addRecordRange(item, record)
	}
	return item.ranges
}

func rangeValues(ranges map[string]SequenceRange) []SequenceRange {
	keys := make([]string, 0, len(ranges))
	for key := range ranges {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]SequenceRange, 0, len(keys))
	for _, key := range keys {
		values = append(values, ranges[key])
	}
	return values
}

func writeManifest(path string, ranges map[string]SequenceRange) error {
	data, err := json.Marshal(segmentManifest{Ranges: ranges})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".manifest-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func readManifest(path string) (map[string]SequenceRange, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var manifest segmentManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if manifest.Ranges == nil {
		manifest.Ranges = make(map[string]SequenceRange)
	}
	return manifest.Ranges, nil
}

func segmentName(id uint64) string {
	return fmt.Sprintf("%012d.seg", id)
}

func segmentID(name string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSuffix(name, ".seg"), 10, 64)
}

func scanFile(path string, limit int64) ([]*pb.TelemetryRecord, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()
	reader := io.LimitReader(file, limit)
	var records []*pb.TelemetryRecord
	var offset int64
	for offset < limit {
		var header [frameHeaderSize]byte
		n, err := io.ReadFull(reader, header[:])
		if err != nil {
			if errors.Is(err, io.EOF) && n == 0 {
				return records, offset, nil
			}
			return records, offset, io.ErrUnexpectedEOF
		}
		if binary.BigEndian.Uint32(header[0:4]) != frameMagic || binary.BigEndian.Uint16(header[4:6]) != frameVersion {
			return records, offset, fmt.Errorf("invalid wal frame header at offset %d", offset)
		}
		length := binary.BigEndian.Uint32(header[8:12])
		if length == 0 || int64(length) > DefaultSegmentSize*2 {
			return records, offset, fmt.Errorf("invalid wal frame length %d", length)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return records, offset, io.ErrUnexpectedEOF
		}
		if crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)) != binary.BigEndian.Uint32(header[12:16]) {
			return records, offset, fmt.Errorf("wal checksum mismatch at offset %d", offset)
		}
		record := new(pb.TelemetryRecord)
		if err := proto.Unmarshal(payload, record); err != nil {
			return records, offset, fmt.Errorf("decode wal record at offset %d: %w", offset, err)
		}
		records = append(records, record)
		offset += int64(frameHeaderSize) + int64(length)
	}
	return records, offset, nil
}
