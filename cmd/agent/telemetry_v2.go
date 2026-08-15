package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hi2shark/santaizi-agent/model"
	"github.com/hi2shark/santaizi-agent/pkg/dialcache"
	"github.com/hi2shark/santaizi-agent/pkg/identity"
	"github.com/hi2shark/santaizi-agent/pkg/monitor"
	"github.com/hi2shark/santaizi-agent/pkg/pki"
	"github.com/hi2shark/santaizi-agent/pkg/util"
	"github.com/hi2shark/santaizi-agent/pkg/wal"
	pb "github.com/hi2shark/santaizi-agent/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const telemetryProtocolVersion = "2"
const sinkRTTInterval = 15 * time.Second

type endpointWorker struct {
	endpoint        *pb.TelemetryEndpoint
	cancel          context.CancelFunc
	pingDisabled    bool
	lastRTTAt       time.Time
	lastSnapshotSeq uint64
}

type telemetryManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	auth    model.AuthHandler
	nodeID  identity.ID
	session *identity.Session
	started time.Time
	wal     *wal.WAL
	state   *wal.StateStore

	mu           sync.RWMutex
	credential   *pb.SignedAgentCredential
	assignment   *pb.EndpointAssignment
	workers      map[string]*endpointWorker
	sinkRuntime  map[string]*pb.SinkRuntime
	latestHost   *pb.Host
	latestState  *pb.State
	latestSeq    uint64
	latestAt     time.Time
	closing      bool
	rollup       *pressureStateRollup
	pkiStore     *pki.Store
	dialCache    *dialcache.Store
	legacyAuth   bool
	renewBackoff time.Duration
	nextRenew    time.Time
}

func startV2Telemetry(parent context.Context, auth model.AuthHandler) (*telemetryManager, error) {
	dataDir := agentConfig.Telemetry.DataDir
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("create telemetry data directory: %w", err)
	}
	nodeID, err := identity.LoadOrCreate(dataDir)
	if err != nil {
		return nil, err
	}
	session, err := identity.NewSession()
	if err != nil {
		return nil, err
	}
	journal, err := wal.Open(wal.Config{
		Dir:           filepath.Join(dataDir, "wal"),
		SegmentSize:   agentConfig.Telemetry.WAL.SegmentSizeBytes,
		MaxSize:       agentConfig.Telemetry.WAL.MaxSizeBytes,
		ReserveBytes:  agentConfig.Telemetry.WAL.ReserveBytes,
		FsyncInterval: agentConfig.Telemetry.WAL.FsyncInterval,
		FsyncRecords:  agentConfig.Telemetry.WAL.FsyncRecords,
	})
	if err != nil {
		return nil, err
	}
	state, err := wal.OpenStateStore(filepath.Join(dataDir, "wal"))
	if err != nil {
		_ = journal.Close()
		return nil, err
	}
	pkiStore, err := pki.Open(filepath.Join(dataDir, "pki"))
	if err != nil {
		_ = journal.Close()
		return nil, err
	}
	cache, err := dialcache.Open(dataDir)
	if err != nil {
		_ = journal.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	m := &telemetryManager{
		ctx: ctx, cancel: cancel, auth: auth, nodeID: nodeID, session: session,
		started: time.Now(), wal: journal, state: state, workers: make(map[string]*endpointWorker),
		sinkRuntime: make(map[string]*pb.SinkRuntime), pkiStore: pkiStore, dialCache: cache,
	}
	if credential, err := readCredential(filepath.Join(dataDir, "credential.pb")); err == nil {
		m.credential = credential
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = journal.Close()
		return nil, err
	}

	host := collectHost()
	m.mu.Lock()
	m.latestHost = host
	m.mu.Unlock()
	if _, err := m.appendEvent(pb.TelemetryEventType_TELEMETRY_EVENT_TYPE_LIFECYCLE, pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL,
		eventPayload(&pb.TelemetryEvent_Lifecycle{Lifecycle: &pb.LifecyclePayload{Kind: pb.LifecycleKind_LIFECYCLE_KIND_AGENT_STARTED}})); err != nil {
		_ = journal.Close()
		return nil, err
	}
	if _, err := m.appendEvent(pb.TelemetryEventType_TELEMETRY_EVENT_TYPE_HOST, pb.TelemetryPriority_TELEMETRY_PRIORITY_P1_IMPORTANT,
		eventPayload(&pb.TelemetryEvent_Host{Host: host})); err != nil {
		_ = journal.Close()
		return nil, err
	}
	for _, issue := range journal.RecoveryIssues() {
		detail := issue.Segment + ": " + issue.Reason
		var lostRecords uint64
		for _, sequenceRange := range issue.Ranges {
			lostRecords += sequenceRange.EndSequence - sequenceRange.StartSequence + 1
		}
		var intentionalGap uint64
		if len(issue.Ranges) == 0 {
			intentionalGap, _ = m.session.Next(m.nodeID)
			lostRecords = 1
		}
		replacement, appendErr := m.appendEvent(pb.TelemetryEventType_TELEMETRY_EVENT_TYPE_DATA_LOSS, pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL,
			eventPayload(&pb.TelemetryEvent_DataLoss{DataLoss: &pb.DataLossPayload{
				Reason: pb.GapReason_GAP_REASON_CORRUPTION, Component: "agent_wal", Detail: detail, LostRecords: lostRecords,
			}}))
		if appendErr != nil || replacement == nil {
			printf("追加 WAL 损坏事实失败: %v", appendErr)
			continue
		}
		if intentionalGap != 0 {
			m.appendExplicitGap(m.nodeID[:], m.session.ID[:], intentionalGap, intentionalGap, pb.GapReason_GAP_REASON_CORRUPTION, replacement.GetEventId())
		}
		for _, sequenceRange := range issue.Ranges {
			m.appendExplicitGap(sequenceRange.NodeUUID, sequenceRange.SessionID, sequenceRange.StartSequence, sequenceRange.EndSequence,
				pb.GapReason_GAP_REASON_CORRUPTION, replacement.GetEventId())
		}
	}

	m.wg.Add(2)
	go m.collectLoop()
	go m.controlLoop()
	return m, nil
}

func (m *telemetryManager) Close() error {
	m.cancel()
	m.mu.Lock()
	m.closing = true
	for _, worker := range m.workers {
		worker.cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
	m.flushPressureRollup(time.Now())
	return m.wal.Close()
}

func (m *telemetryManager) collectLoop() {
	defer m.wg.Done()
	stateTicker := time.NewTicker(agentConfig.Telemetry.StateInterval)
	heartbeatTicker := time.NewTicker(agentConfig.Telemetry.HeartbeatInterval)
	hostTicker := time.NewTicker(agentConfig.Telemetry.HostInterval)
	gcTicker := time.NewTicker(time.Minute)
	defer stateTicker.Stop()
	defer heartbeatTicker.Stop()
	defer hostTicker.Stop()
	defer gcTicker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-stateTicker.C:
			m.flushCompletedPressureRollup(time.Now())
			state := collectState()
			m.mu.Lock()
			m.latestState = state
			m.mu.Unlock()
			if event, err := m.appendEvent(pb.TelemetryEventType_TELEMETRY_EVENT_TYPE_STATE, pb.TelemetryPriority_TELEMETRY_PRIORITY_P2_NORMAL,
				eventPayload(&pb.TelemetryEvent_State{State: state})); errors.Is(err, wal.ErrNeedsRollup) || errors.Is(err, wal.ErrHardLimit) {
				m.appendPressureReplacement(state, event.GetSequence(), err)
			} else if err != nil && !errors.Is(err, wal.ErrDownsampled) {
				printf("追加状态探测失败: %v", err)
			}
			m.flushHostIfLocationChanged()
		case <-heartbeatTicker.C:
			host := m.snapshotHost()
			event, err := m.appendEvent(pb.TelemetryEventType_TELEMETRY_EVENT_TYPE_HEARTBEAT, pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL,
				eventPayload(&pb.TelemetryEvent_Heartbeat{Heartbeat: &pb.HeartbeatPayload{BootTimeUnix: host.GetBootTime()}}))
			if err != nil {
				printf("追加心跳探测失败: %v", err)
				m.appendFailedEventGap(event, "heartbeat WAL append failed")
			}
		case <-hostTicker.C:
			m.publishHost(m.nextHost())
		case <-gcTicker.C:
			snapshot := m.state.Snapshot()
			if _, err := m.wal.Reclaim(func(record *pb.TelemetryRecord) bool {
				return recordAckedBy(snapshot, record)
			}); err != nil {
				printf("回收探测 WAL 失败: %v", err)
			}
		}
	}
}

func (m *telemetryManager) appendEvent(eventType pb.TelemetryEventType, priority pb.TelemetryPriority, payload isTelemetryPayload) (*pb.TelemetryEvent, error) {
	sequence, eventID := m.session.Next(m.nodeID)
	now := time.Now()
	event := &pb.TelemetryEvent{
		EventId: eventID[:], NodeUuid: m.nodeID[:], SessionId: m.session.ID[:], Sequence: sequence,
		EventType: eventType, Priority: priority, CollectedAtUnixNano: now.UnixNano(),
		SessionElapsedNano: uint64(time.Since(m.started)), ProtocolVersion: 2,
		SourceProtocol: pb.SourceProtocol_SOURCE_PROTOCOL_SANTAIZI_V2,
		Reliability:    pb.Reliability_RELIABILITY_RELIABLE_REPLAY,
	}
	payload.apply(event)
	if state := event.GetState(); state != nil {
		event.AgentUptimeNano = state.GetUptime() * uint64(time.Second)
	} else {
		event.AgentUptimeNano = uint64(time.Since(m.started))
	}
	if err := m.wal.Append(&pb.TelemetryRecord{Record: &pb.TelemetryRecord_Event{Event: event}}, priority); err != nil {
		return event, err
	}
	m.mu.Lock()
	m.latestSeq = sequence
	m.latestAt = now
	m.mu.Unlock()
	return event, nil
}

type isTelemetryPayload interface{ apply(*pb.TelemetryEvent) }
type telemetryPayloadFunc func(*pb.TelemetryEvent)

func (f telemetryPayloadFunc) apply(event *pb.TelemetryEvent) { f(event) }

func eventPayload(body any) isTelemetryPayload {
	return telemetryPayloadFunc(func(event *pb.TelemetryEvent) {
		switch body := body.(type) {
		case *pb.TelemetryEvent_Heartbeat:
			event.Payload = body
		case *pb.TelemetryEvent_State:
			event.Payload = body
		case *pb.TelemetryEvent_Host:
			event.Payload = body
		case *pb.TelemetryEvent_Lifecycle:
			event.Payload = body
		case *pb.TelemetryEvent_StateRollup:
			event.Payload = body
		case *pb.TelemetryEvent_DataLoss:
			event.Payload = body
		default:
			panic(fmt.Sprintf("unsupported telemetry payload %T", body))
		}
	})
}

func (m *telemetryManager) appendPressureReplacement(state *pb.State, skipped uint64, cause error) {
	now := time.Now()
	if errors.Is(cause, wal.ErrNeedsRollup) {
		windowStart := now.Truncate(time.Minute)
		if m.rollup != nil && !m.rollup.windowStart.Equal(windowStart) {
			m.flushPressureRollup(m.rollup.windowStart.Add(time.Minute))
		}
		if m.rollup == nil {
			m.rollup = newPressureStateRollup(windowStart)
		}
		m.rollup.add(state)
		m.appendGap(skipped, skipped, pb.GapReason_GAP_REASON_COMPACTED, nil)
		return
	}
	replacement, err := m.appendEvent(pb.TelemetryEventType_TELEMETRY_EVENT_TYPE_DATA_LOSS, pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL,
		eventPayload(&pb.TelemetryEvent_DataLoss{DataLoss: &pb.DataLossPayload{
			Reason: pb.GapReason_GAP_REASON_HARD_LIMIT_DATA_LOSS, Component: "agent_wal", Detail: "state sample rejected at hard limit", LostRecords: 1,
		}}))
	if err == nil {
		m.appendGap(skipped, skipped, pb.GapReason_GAP_REASON_HARD_LIMIT_DATA_LOSS, replacement.GetEventId())
	} else {
		end := skipped
		if replacement != nil && replacement.GetSequence() > end {
			end = replacement.GetSequence()
		}
		m.appendGap(skipped, end, pb.GapReason_GAP_REASON_HARD_LIMIT_DATA_LOSS, nil)
	}
}

func (m *telemetryManager) appendFailedEventGap(failed *pb.TelemetryEvent, detail string) {
	if failed == nil || failed.GetSequence() == 0 {
		return
	}
	replacement, err := m.appendEvent(pb.TelemetryEventType_TELEMETRY_EVENT_TYPE_DATA_LOSS, pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL,
		eventPayload(&pb.TelemetryEvent_DataLoss{DataLoss: &pb.DataLossPayload{
			Reason: pb.GapReason_GAP_REASON_HARD_LIMIT_DATA_LOSS, Component: "agent_wal", Detail: detail, LostRecords: 1,
		}}))
	if err != nil {
		end := failed.GetSequence()
		if replacement != nil && replacement.GetSequence() > end {
			end = replacement.GetSequence()
		}
		m.appendGap(failed.GetSequence(), end, pb.GapReason_GAP_REASON_HARD_LIMIT_DATA_LOSS, nil)
		return
	}
	m.appendGap(failed.GetSequence(), failed.GetSequence(), pb.GapReason_GAP_REASON_HARD_LIMIT_DATA_LOSS, replacement.GetEventId())
}

func (m *telemetryManager) flushCompletedPressureRollup(now time.Time) {
	if m.rollup != nil && !m.rollup.windowStart.Equal(now.Truncate(time.Minute)) {
		m.flushPressureRollup(m.rollup.windowStart.Add(time.Minute))
	}
}

func (m *telemetryManager) flushPressureRollup(end time.Time) {
	if m.rollup == nil || m.rollup.count == 0 {
		m.rollup = nil
		return
	}
	rollup := m.rollup
	m.rollup = nil
	if end.After(rollup.windowStart.Add(time.Minute)) {
		end = rollup.windowStart.Add(time.Minute)
	}
	if end.Before(rollup.windowStart) {
		end = rollup.windowStart
	}
	event, err := m.appendEvent(pb.TelemetryEventType_TELEMETRY_EVENT_TYPE_STATE_ROLLUP, pb.TelemetryPriority_TELEMETRY_PRIORITY_P1_IMPORTANT,
		eventPayload(&pb.TelemetryEvent_StateRollup{StateRollup: rollup.payload(end)}))
	if err != nil {
		m.appendFailedEventGap(event, "one-minute pressure rollup WAL append failed")
	}
}

func (m *telemetryManager) appendGap(start, end uint64, reason pb.GapReason, replacement []byte) {
	m.appendExplicitGap(m.nodeID[:], m.session.ID[:], start, end, reason, replacement)
}

func (m *telemetryManager) appendExplicitGap(nodeUUID, sessionID []byte, start, end uint64, reason pb.GapReason, replacement []byte) {
	gapID, err := identity.New()
	if err != nil {
		return
	}
	gap := &pb.SequenceGap{
		GapId: gapID[:], NodeUuid: append([]byte(nil), nodeUUID...), SessionId: append([]byte(nil), sessionID...), StartSequence: start,
		EndSequence: end, Reason: reason, ReplacementEventId: append([]byte(nil), replacement...), CreatedAtUnixNano: time.Now().UnixNano(),
	}
	if err := m.wal.Append(&pb.TelemetryRecord{Record: &pb.TelemetryRecord_Gap{Gap: gap}}, pb.TelemetryPriority_TELEMETRY_PRIORITY_P0_CRITICAL); err != nil {
		printf("追加探测缺口事实失败: %v", err)
	}
}

func (m *telemetryManager) snapshotHost() *pb.Host {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneHost(m.latestHost)
}

func (m *telemetryManager) previousHostIP() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latestHost.GetIp()
}

func (m *telemetryManager) previousHostCountry() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latestHost.GetCountryCode()
}

func (m *telemetryManager) nextHost() *pb.Host {
	return retainKnownHostIP(collectHost(), m.previousHostIP())
}

func (m *telemetryManager) publishHost(host *pb.Host) {
	m.mu.Lock()
	m.latestHost = host
	m.mu.Unlock()
	if event, err := m.appendEvent(pb.TelemetryEventType_TELEMETRY_EVENT_TYPE_HOST, pb.TelemetryPriority_TELEMETRY_PRIORITY_P1_IMPORTANT,
		eventPayload(&pb.TelemetryEvent_Host{Host: host})); err != nil {
		printf("追加主机探测失败: %v", err)
		m.appendFailedEventGap(event, "host WAL append failed")
	}
}

func (m *telemetryManager) flushHostIfLocationChanged() {
	if !agentConfig.Capabilities.IPReport {
		return
	}
	previous := &pb.Host{Ip: m.previousHostIP(), CountryCode: m.previousHostCountry()}
	if !hostLocationChanged(monitor.CachedIP, monitor.CachedCountryCode, previous) {
		return
	}
	m.publishHost(m.nextHost())
}

func hostLocationChanged(cachedIP, cachedCountry string, previous *pb.Host) bool {
	cachedIP = strings.TrimSpace(cachedIP)
	cachedCountry = strings.TrimSpace(cachedCountry)
	if cachedIP == "" && cachedCountry == "" {
		return false
	}
	prevIP := strings.TrimSpace(previous.GetIp())
	prevCountry := strings.TrimSpace(previous.GetCountryCode())
	return (cachedIP != "" && cachedIP != prevIP) || (cachedCountry != "" && cachedCountry != prevCountry)
}

func retainKnownHostIP(host *pb.Host, previousIP string) *pb.Host {
	if host == nil {
		return nil
	}
	if strings.TrimSpace(host.GetIp()) != "" || strings.TrimSpace(previousIP) == "" {
		return host
	}
	host.Ip = previousIP
	return host
}

type pressureStateRollup struct {
	windowStart time.Time
	count       uint32
	minimum     *pb.State
	average     *pb.State
	maximum     *pb.State
	previous    *pb.State
	netIn       uint64
	netOut      uint64
}

func newPressureStateRollup(windowStart time.Time) *pressureStateRollup {
	return &pressureStateRollup{windowStart: windowStart}
}

func (r *pressureStateRollup) add(state *pb.State) {
	if state == nil {
		return
	}
	if r.count == 0 {
		r.minimum, r.average, r.maximum = cloneState(state), cloneState(state), cloneState(state)
	} else {
		updateStateMinimum(r.minimum, state)
		updateStateMaximum(r.maximum, state)
		updateStateAverage(r.average, state, uint64(r.count)+1)
		r.netIn += agentCounterDelta(r.previous.GetNetInTransfer(), state.GetNetInTransfer(), state.GetUptime() >= r.previous.GetUptime())
		r.netOut += agentCounterDelta(r.previous.GetNetOutTransfer(), state.GetNetOutTransfer(), state.GetUptime() >= r.previous.GetUptime())
	}
	r.previous = cloneState(state)
	r.count++
}

func (r *pressureStateRollup) payload(end time.Time) *pb.StateRollupPayload {
	return &pb.StateRollupPayload{
		WindowStartUnixNano: r.windowStart.UnixNano(), WindowEndUnixNano: end.UnixNano(), SampleCount: r.count,
		Minimum: cloneState(r.minimum), Average: cloneState(r.average), Maximum: cloneState(r.maximum),
		NetInTotal: r.netIn, NetOutTotal: r.netOut,
	}
}

func updateStateMinimum(target, sample *pb.State) {
	target.Cpu = math.Min(target.GetCpu(), sample.GetCpu())
	target.MemUsed = minUint64(target.GetMemUsed(), sample.GetMemUsed())
	target.SwapUsed = minUint64(target.GetSwapUsed(), sample.GetSwapUsed())
	target.DiskUsed = minUint64(target.GetDiskUsed(), sample.GetDiskUsed())
	target.Load1 = math.Min(target.GetLoad1(), sample.GetLoad1())
	target.Load5 = math.Min(target.GetLoad5(), sample.GetLoad5())
	target.Load15 = math.Min(target.GetLoad15(), sample.GetLoad15())
	target.TcpConnCount = minUint64(target.GetTcpConnCount(), sample.GetTcpConnCount())
	target.UdpConnCount = minUint64(target.GetUdpConnCount(), sample.GetUdpConnCount())
	target.ProcessCount = minUint64(target.GetProcessCount(), sample.GetProcessCount())
}

func updateStateMaximum(target, sample *pb.State) {
	target.Cpu = math.Max(target.GetCpu(), sample.GetCpu())
	target.MemUsed = maxUint64(target.GetMemUsed(), sample.GetMemUsed())
	target.SwapUsed = maxUint64(target.GetSwapUsed(), sample.GetSwapUsed())
	target.DiskUsed = maxUint64(target.GetDiskUsed(), sample.GetDiskUsed())
	target.Load1 = math.Max(target.GetLoad1(), sample.GetLoad1())
	target.Load5 = math.Max(target.GetLoad5(), sample.GetLoad5())
	target.Load15 = math.Max(target.GetLoad15(), sample.GetLoad15())
	target.TcpConnCount = maxUint64(target.GetTcpConnCount(), sample.GetTcpConnCount())
	target.UdpConnCount = maxUint64(target.GetUdpConnCount(), sample.GetUdpConnCount())
	target.ProcessCount = maxUint64(target.GetProcessCount(), sample.GetProcessCount())
}

func minUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}

func maxUint64(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}

func updateStateAverage(target, sample *pb.State, count uint64) {
	previousCount := count - 1
	target.Cpu = (target.GetCpu()*float64(previousCount) + sample.GetCpu()) / float64(count)
	target.MemUsed = (target.GetMemUsed()*previousCount + sample.GetMemUsed()) / count
	target.SwapUsed = (target.GetSwapUsed()*previousCount + sample.GetSwapUsed()) / count
	target.DiskUsed = (target.GetDiskUsed()*previousCount + sample.GetDiskUsed()) / count
	target.Load1 = (target.GetLoad1()*float64(previousCount) + sample.GetLoad1()) / float64(count)
	target.Load5 = (target.GetLoad5()*float64(previousCount) + sample.GetLoad5()) / float64(count)
	target.Load15 = (target.GetLoad15()*float64(previousCount) + sample.GetLoad15()) / float64(count)
	target.TcpConnCount = (target.GetTcpConnCount()*previousCount + sample.GetTcpConnCount()) / count
	target.UdpConnCount = (target.GetUdpConnCount()*previousCount + sample.GetUdpConnCount()) / count
	target.ProcessCount = (target.GetProcessCount()*previousCount + sample.GetProcessCount()) / count
}

func agentCounterDelta(previous, current uint64, continuousBoot bool) uint64 {
	const maximumPlausibleDelta = uint64(1 << 50)
	if !continuousBoot {
		return 0
	}
	if current >= previous {
		delta := current - previous
		if delta <= maximumPlausibleDelta {
			return delta
		}
		return 0
	}
	if previous > ^uint64(0)-(1<<32) {
		delta := ^uint64(0) - previous + current + 1
		if delta <= maximumPlausibleDelta {
			return delta
		}
	}
	return 0
}

func (m *telemetryManager) controlLoop() {
	defer m.wg.Done()
	for {
		if m.ctx.Err() != nil {
			return
		}
		if err := m.controlOnce(); err != nil && !errors.Is(err, context.Canceled) {
			printf("V2 控制流断开: %v", err)
		}
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(delayWhenError):
		}
	}
}

func (m *telemetryManager) controlOnce() error {
	if err := m.ensureDeviceCredentials(); err != nil {
		return err
	}
	options := m.dialOptions(agentCliParam.TLS, agentCliParam.InsecureTLS, m.legacyAuth, serverNameOf(agentCliParam.Server))
	return m.tryDials(m.ctx, dialcache.PrimaryKey, agentCliParam.Server, options, func(conn *grpc.ClientConn, attempt *dialAttempt) error {
		return m.controlOnConn(conn, attempt)
	})
}

func (m *telemetryManager) controlOnConn(conn *grpc.ClientConn, attempt *dialAttempt) error {
	stream, err := pb.NewSantaiziControlServiceClient(conn).Control(m.ctx)
	if err != nil {
		return err
	}
	snapshot := m.state.Snapshot()
	var sendMu sync.Mutex
	send := func(request *pb.AgentControlRequest) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(request)
	}
	if err := send(&pb.AgentControlRequest{Body: &pb.AgentControlRequest_Hello{Hello: &pb.AgentControlHello{
		NodeUuid: m.nodeID[:], SessionId: m.session.ID[:], CurrentConfigVersion: snapshot.ConfigVersion,
		AgentVersion: version, Host: m.snapshotHost(), Capabilities: capabilitiesPB(),
	}}}); err != nil {
		return err
	}
	attempt.Remember()
	recv := make(chan *pb.PrimaryControlResponse)
	recvErr := make(chan error, 1)
	asyncErr := make(chan error, 1)
	go func() {
		for {
			response, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case recv <- response:
			case <-m.ctx.Done():
				return
			}
		}
	}()
	runtimeTicker := time.NewTicker(15 * time.Second)
	credentialRefreshTicker := time.NewTicker(24 * time.Hour)
	defer runtimeTicker.Stop()
	defer credentialRefreshTicker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case err := <-recvErr:
			return err
		case err := <-asyncErr:
			return err
		case response := <-recv:
			switch body := response.GetBody().(type) {
			case *pb.PrimaryControlResponse_Credential:
				if err := writeCredential(filepath.Join(agentConfig.Telemetry.DataDir, "credential.pb"), body.Credential); err != nil {
					return err
				}
				m.mu.Lock()
				m.credential = proto.Clone(body.Credential).(*pb.SignedAgentCredential)
				m.mu.Unlock()
				m.reconcileWorkers()
			case *pb.PrimaryControlResponse_Assignment:
				if err := m.state.SetConfigVersion(body.Assignment.GetConfigVersion()); err != nil {
					return err
				}
				m.mu.Lock()
				m.assignment = proto.Clone(body.Assignment).(*pb.EndpointAssignment)
				m.mu.Unlock()
				m.reconcileWorkers()
			case *pb.PrimaryControlResponse_ProbeRequest:
				request := proto.Clone(body.ProbeRequest).(*pb.ProbeRequest)
				m.wg.Add(1)
				go func() {
					defer m.wg.Done()
					result := executeProbe(request)
					if err := send(&pb.AgentControlRequest{Body: &pb.AgentControlRequest_ProbeResult{ProbeResult: result}}); err != nil {
						select {
						case asyncErr <- err:
						default:
						}
					}
				}()
			case *pb.PrimaryControlResponse_NatOpenRequest:
				request := proto.Clone(body.NatOpenRequest).(*pb.NATOpenRequest)
				m.wg.Add(1)
				go func() {
					defer m.wg.Done()
					result := &pb.NATOpenResult{StreamId: request.GetStreamId()}
					if err := m.startNAT(request); err != nil {
						result.Error = err.Error()
					} else {
						result.Accepted = true
					}
					if err := send(&pb.AgentControlRequest{Body: &pb.AgentControlRequest_NatOpenResult{NatOpenResult: result}}); err != nil {
						select {
						case asyncErr <- err:
						default:
						}
					}
				}()
			}
		case <-runtimeTicker.C:
			if err := send(&pb.AgentControlRequest{Body: &pb.AgentControlRequest_Runtime{Runtime: m.runtimeSnapshot()}}); err != nil {
				return err
			}
		case <-credentialRefreshTicker.C:
			return nil
		}
	}
}

func (m *telemetryManager) reconcileWorkers() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || m.ctx.Err() != nil || m.credential == nil || m.assignment == nil {
		return
	}
	disabled := make(map[string]bool, len(agentConfig.Telemetry.DisabledRemoteIDs))
	for _, id := range agentConfig.Telemetry.DisabledRemoteIDs {
		disabled[id] = true
	}
	local := make(map[string]model.TelemetryEndpointConfig, len(agentConfig.Telemetry.Collectors))
	for _, endpoint := range agentConfig.Telemetry.Collectors {
		local[endpoint.ID] = endpoint
	}
	desired := make(map[string]*pb.TelemetryEndpoint)
	for _, assigned := range m.assignment.GetEndpoints() {
		if disabled[assigned.GetEndpointId()] {
			continue
		}
		endpoint := proto.Clone(assigned).(*pb.TelemetryEndpoint)
		if endpoint.GetKind() == pb.EndpointKind_ENDPOINT_KIND_PRIMARY {
			endpoint.Address = agentCliParam.Server
			endpoint.Tls = agentCliParam.TLS
			endpoint.InsecureTls = agentCliParam.InsecureTLS
		} else if override, ok := local[endpoint.GetEndpointId()]; ok {
			endpoint.Address = override.Address
			endpoint.Tls = override.TLS
			endpoint.InsecureTls = override.InsecureTLS
		}
		if endpoint.GetAddress() == "" {
			continue
		}
		desired[endpoint.GetEndpointId()] = endpoint
	}
	for id, worker := range m.workers {
		endpoint, ok := desired[id]
		if ok && endpoint.GetGeneration() == worker.endpoint.GetGeneration() && endpoint.GetAddress() == worker.endpoint.GetAddress() && endpoint.GetTls() == worker.endpoint.GetTls() && endpoint.GetInsecureTls() == worker.endpoint.GetInsecureTls() {
			delete(desired, id)
			continue
		}
		worker.cancel()
		delete(m.workers, id)
		delete(m.sinkRuntime, id)
		if !ok {
			_ = m.state.RemoveEndpoint(id)
		}
	}
	for id, endpoint := range desired {
		activationSession := endpoint.GetActivationSessionId()
		if len(activationSession) == 0 {
			activationSession = m.session.ID[:]
		}
		activation := endpoint.GetActivationSequence()
		if activation == 0 {
			activation = 1
		}
		_ = m.state.UpsertEndpoint(wal.EndpointState{
			EndpointID: id, Generation: endpoint.GetGeneration(), Reliable: endpoint.GetReliable(),
			ActivationSession: hex.EncodeToString(activationSession), ActivationSequence: activation,
		})
		ctx, cancel := context.WithCancel(m.ctx)
		worker := &endpointWorker{endpoint: endpoint, cancel: cancel}
		m.workers[id] = worker
		m.sinkRuntime[id] = &pb.SinkRuntime{EndpointId: id, Generation: endpoint.GetGeneration()}
		m.wg.Add(1)
		go m.sinkLoop(ctx, worker)
	}
}

func (m *telemetryManager) sinkLoop(ctx context.Context, worker *endpointWorker) {
	defer m.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := m.sinkOnce(ctx, worker); err != nil && !errors.Is(err, context.Canceled) {
			m.setSinkError(worker.endpoint, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delayWhenError):
		}
	}
}

func (m *telemetryManager) sinkOnce(ctx context.Context, worker *endpointWorker) error {
	endpoint := worker.endpoint
	key := dialcache.CollectorKey(endpoint.GetEndpointId())
	if endpoint.GetKind() == pb.EndpointKind_ENDPOINT_KIND_PRIMARY {
		key = dialcache.PrimaryKey
	}
	options := m.dialOptions(endpoint.GetTls(), endpoint.GetInsecureTls(), false, serverNameOf(endpoint.GetAddress()))
	return m.tryDials(ctx, key, endpoint.GetAddress(), options, func(conn *grpc.ClientConn, attempt *dialAttempt) error {
		return m.sinkOnConn(ctx, worker, conn, attempt)
	})
}

func (m *telemetryManager) sinkOnConn(ctx context.Context, worker *endpointWorker, conn *grpc.ClientConn, attempt *dialAttempt) error {
	endpoint := worker.endpoint
	stream, err := pb.NewSantaiziTelemetryServiceClient(conn).Ingest(ctx)
	if err != nil {
		return err
	}
	m.mu.RLock()
	credential := proto.Clone(m.credential).(*pb.SignedAgentCredential)
	m.mu.RUnlock()
	if err := stream.Send(&pb.TelemetryRequest{Body: &pb.TelemetryRequest_Hello{Hello: &pb.TelemetryHello{
		NodeUuid: m.nodeID[:], EndpointId: endpoint.GetEndpointId(), AssignmentGeneration: endpoint.GetGeneration(),
		Credential: credential, ProtocolVersion: telemetryProtocolVersion, AgentRuntime: m.runtimeSnapshot(),
	}}}); err != nil {
		return err
	}
	attempt.Remember()
	m.setSinkConnected(endpoint, true)
	defer m.setSinkConnected(endpoint, false)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			records, err := m.pendingFor(endpoint.GetEndpointId(), agentConfig.Telemetry.BatchSize)
			if err != nil {
				return err
			}
			if len(records) == 0 {
				if seq := m.latestSequence(); shouldSendRealtimeSnapshot(worker, seq) {
					if snapshot := m.realtimeSnapshot(); snapshot != nil {
						if err := stream.Send(&pb.TelemetryRequest{Body: &pb.TelemetryRequest_RealtimeSnapshot{RealtimeSnapshot: snapshot}}); err != nil {
							return err
						}
						worker.lastSnapshotSeq = seq
					}
				}
				if err := m.maybePingSink(stream, worker); err != nil {
					return err
				}
				continue
			}
			sent := time.Now()
			if err := stream.Send(&pb.TelemetryRequest{Body: &pb.TelemetryRequest_Batch{Batch: &pb.TelemetryBatch{Records: records}}}); err != nil {
				return err
			}
			response, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return io.ErrUnexpectedEOF
				}
				return err
			}
			if response.GetError() != "" {
				return errors.New(response.GetError())
			}
			m.recordSinkRTT(worker, time.Since(sent))
			for _, ack := range response.GetAcks() {
				if !bytes.Equal(ack.GetNodeUuid(), m.nodeID[:]) {
					continue
				}
				if err := m.state.UpdateAck(endpoint.GetEndpointId(), endpoint.GetGeneration(), ack.GetSessionId(), ack.GetAckThrough()); err != nil {
					return err
				}
				m.setSinkAck(endpoint, ack.GetAckThrough())
			}
		}
	}
}

func (m *telemetryManager) maybePingSink(stream grpc.BidiStreamingClient[pb.TelemetryRequest, pb.TelemetryResponse], worker *endpointWorker) error {
	if worker.pingDisabled || time.Since(worker.lastRTTAt) < sinkRTTInterval {
		return nil
	}
	sent := time.Now()
	if err := stream.Send(&pb.TelemetryRequest{Body: &pb.TelemetryRequest_Ping{Ping: &pb.TelemetryPing{}}}); err != nil {
		return err
	}
	response, err := stream.Recv()
	if err != nil {
		if pingUnsupported(err) {
			worker.pingDisabled = true
		}
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if response.GetError() != "" {
		return errors.New(response.GetError())
	}
	m.recordSinkRTT(worker, time.Since(sent))
	return nil
}

func (m *telemetryManager) recordSinkRTT(worker *endpointWorker, rtt time.Duration) {
	worker.lastRTTAt = time.Now()
	m.setSinkRTT(worker.endpoint, rtt)
}

func pingUnsupported(err error) bool {
	code := status.Code(err)
	return code == codes.InvalidArgument || code == codes.Unimplemented
}

func durationMilliseconds(d time.Duration) float64 {
	if d < 0 {
		return 0
	}
	return float64(d) / float64(time.Millisecond)
}

func (m *telemetryManager) pendingFor(endpointID string, limit int) ([]*pb.TelemetryRecord, error) {
	endpoint := m.state.Endpoint(endpointID)
	if endpoint == nil || !endpoint.Reliable {
		return nil, nil
	}
	if !m.wal.HasPending(func(session string, maxSeq uint64) bool {
		return recordVisibleTo(endpoint, session, 1, maxSeq)
	}) {
		return nil, nil
	}
	return m.wal.CollectPending(func(record *pb.TelemetryRecord) bool {
		sessionID, start, end := wal.RecordRange(record)
		return recordVisibleTo(endpoint, sessionID, start, end)
	}, limit), nil
}

func (m *telemetryManager) latestSequence() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latestSeq
}

func shouldSendRealtimeSnapshot(worker *endpointWorker, latestSeq uint64) bool {
	return latestSeq != 0 && latestSeq != worker.lastSnapshotSeq
}

func hasPendingRecords(heads map[string]uint64, endpoint *wal.EndpointState) bool {
	if endpoint == nil || !endpoint.Reliable {
		return false
	}
	for session, maxSeq := range heads {
		if recordVisibleTo(endpoint, session, 1, maxSeq) {
			return true
		}
	}
	return false
}

func recordVisibleTo(endpoint *wal.EndpointState, sessionID string, start, end uint64) bool {
	if endpoint == nil || !endpoint.Reliable || sessionID == "" || start > end {
		return false
	}
	activation, obligated := endpoint.Activations[sessionID]
	if !obligated {
		if _, known := endpoint.Cursors[sessionID]; !known {
			return false
		}
		activation = 1
	}
	return end >= activation && end > endpoint.Cursors[sessionID]
}

func recordAckedBy(snapshot wal.CursorState, record *pb.TelemetryRecord) bool {
	sessionID, _, end := wal.RecordRange(record)
	if sessionID == "" {
		return false
	}
	obligated := false
	for _, endpoint := range snapshot.Endpoints {
		if !endpoint.Reliable {
			continue
		}
		activation, active := endpoint.Activations[sessionID]
		if !active {
			if _, active = endpoint.Cursors[sessionID]; active {
				activation = 1
			}
		}
		if !active || end < activation {
			continue
		}
		obligated = true
		if endpoint.Cursors[sessionID] < end {
			return false
		}
	}
	return obligated
}

func (m *telemetryManager) realtimeSnapshot() *pb.RealtimeSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.latestAt.IsZero() {
		return nil
	}
	return &pb.RealtimeSnapshot{
		NodeUuid: m.nodeID[:], SessionId: m.session.ID[:], LatestSequence: m.latestSeq,
		CollectedAtUnixNano: m.latestAt.UnixNano(), Host: cloneHost(m.latestHost), State: cloneState(m.latestState),
		AgentRuntime: m.runtimeSnapshotLocked(),
	}
}

func (m *telemetryManager) runtimeSnapshot() *pb.AgentRuntime {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.runtimeSnapshotLocked()
}

func (m *telemetryManager) runtimeSnapshotLocked() *pb.AgentRuntime {
	pressure := pb.WalPressure_WAL_PRESSURE_HEALTHY
	switch m.wal.Pressure() {
	case wal.PressureDownsample:
		pressure = pb.WalPressure_WAL_PRESSURE_P3_DOWNSAMPLED
	case wal.PressureRollup:
		pressure = pb.WalPressure_WAL_PRESSURE_ROLLUP
	case wal.PressureCritical:
		pressure = pb.WalPressure_WAL_PRESSURE_CRITICAL
	case wal.PressureHardLimit:
		pressure = pb.WalPressure_WAL_PRESSURE_DATA_LOSS
	}
	sinks := make([]*pb.SinkRuntime, 0, len(m.sinkRuntime))
	for _, item := range m.sinkRuntime {
		sinks = append(sinks, proto.Clone(item).(*pb.SinkRuntime))
	}
	sort.Slice(sinks, func(i, j int) bool { return sinks[i].GetEndpointId() < sinks[j].GetEndpointId() })
	return &pb.AgentRuntime{
		WalPressure: pressure, WalBytes: uint64(m.wal.Size()), Sinks: sinks,
		ProtocolVersion: telemetryProtocolVersion, Capabilities: capabilitiesPB(),
	}
}

func capabilitiesPB() *pb.AgentCapabilities {
	caps := agentConfig.Capabilities
	enabled := make([]pb.AgentCapability, 0, 14)
	appendIf := func(active bool, capability pb.AgentCapability) {
		if active {
			enabled = append(enabled, capability)
		}
	}
	appendIf(caps.CPU, pb.AgentCapability_AGENT_CAPABILITY_METRIC_CPU)
	appendIf(caps.Memory, pb.AgentCapability_AGENT_CAPABILITY_METRIC_MEMORY)
	appendIf(caps.Disk, pb.AgentCapability_AGENT_CAPABILITY_METRIC_DISK)
	appendIf(caps.Network, pb.AgentCapability_AGENT_CAPABILITY_METRIC_NETWORK)
	appendIf(caps.Connections, pb.AgentCapability_AGENT_CAPABILITY_METRIC_CONNECTIONS)
	appendIf(caps.Processes, pb.AgentCapability_AGENT_CAPABILITY_METRIC_PROCESSES)
	appendIf(caps.Temperature, pb.AgentCapability_AGENT_CAPABILITY_METRIC_TEMPERATURE)
	appendIf(caps.GPU, pb.AgentCapability_AGENT_CAPABILITY_METRIC_GPU)
	appendIf(caps.HostInfo, pb.AgentCapability_AGENT_CAPABILITY_HOST_INFO)
	appendIf(caps.IPReport, pb.AgentCapability_AGENT_CAPABILITY_IP_REPORT)
	appendIf(caps.HTTPProbe, pb.AgentCapability_AGENT_CAPABILITY_PROBE_HTTP)
	appendIf(caps.ICMPProbe, pb.AgentCapability_AGENT_CAPABILITY_PROBE_ICMP)
	appendIf(caps.TCPProbe, pb.AgentCapability_AGENT_CAPABILITY_PROBE_TCP)
	appendIf(caps.NAT, pb.AgentCapability_AGENT_CAPABILITY_NAT)
	return &pb.AgentCapabilities{Enabled: enabled}
}

func collectHost() *pb.Host {
	host := monitor.GetHost().PB()
	if !agentConfig.Capabilities.HostInfo {
		host.Platform = ""
		host.PlatformVersion = ""
		host.Cpu = nil
		host.MemTotal = 0
		host.DiskTotal = 0
		host.SwapTotal = 0
		host.Arch = ""
		host.Virtualization = ""
		host.Gpu = nil
	}
	if !agentConfig.Capabilities.IPReport {
		host.Ip = ""
		host.CountryCode = ""
	}
	return host
}

func collectState() *pb.State {
	caps := agentConfig.Capabilities
	if caps.Network {
		monitor.TrackNetworkSpeed()
	}
	state := monitor.GetState().PB()
	if !caps.CPU {
		state.Cpu = 0
		state.Load1 = 0
		state.Load5 = 0
		state.Load15 = 0
	}
	if !caps.Memory {
		state.MemUsed = 0
		state.SwapUsed = 0
	}
	if !caps.Disk {
		state.DiskUsed = 0
	}
	if !caps.Network {
		state.NetInTransfer = 0
		state.NetOutTransfer = 0
		state.NetInSpeed = 0
		state.NetOutSpeed = 0
	}
	return state
}

func (m *telemetryManager) setSinkConnected(endpoint *pb.TelemetryEndpoint, connected bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if runtime := m.sinkRuntime[endpoint.GetEndpointId()]; runtime != nil && runtime.GetGeneration() == endpoint.GetGeneration() {
		runtime.Connected = connected
		if connected {
			runtime.LastError = ""
		}
	}
}

func (m *telemetryManager) setSinkAck(endpoint *pb.TelemetryEndpoint, ack uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if runtime := m.sinkRuntime[endpoint.GetEndpointId()]; runtime != nil && ack > runtime.AckThrough {
		runtime.AckThrough = ack
	}
}

func (m *telemetryManager) setSinkRTT(endpoint *pb.TelemetryEndpoint, rtt time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if runtime := m.sinkRuntime[endpoint.GetEndpointId()]; runtime != nil && runtime.GetGeneration() == endpoint.GetGeneration() {
		runtime.LastRttMs = durationMilliseconds(rtt)
		runtime.RttSampledAtUnixNano = time.Now().UnixNano()
	}
}

func (m *telemetryManager) setSinkError(endpoint *pb.TelemetryEndpoint, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if runtime := m.sinkRuntime[endpoint.GetEndpointId()]; runtime != nil {
		runtime.Connected = false
		runtime.LastError = err.Error()
	}
}

func (m *telemetryManager) dialOptions(useTLS, insecureTLS, withAuth bool, serverName string) []grpc.DialOption {
	var transport grpc.DialOption
	if useTLS {
		tlsCfg, err := pki.NewClientTLSConfig(pki.ClientTLSOptions{
			CAFile:               agentConfig.TLSCAFile,
			ExtraCAPEM:           m.pkiCAPEM(),
			ServerName:           serverName,
			InsecureSkipVerify:   insecureTLS,
			GetClientCertificate: m.pkiGetClientCertificate(),
		})
		if err != nil {
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName, InsecureSkipVerify: insecureTLS}
		}
		transport = grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))
	} else {
		transport = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	options := []grpc.DialOption{transport}
	if withAuth {
		options = append(options, grpc.WithPerRPCCredentials(&m.auth))
	}
	return options
}

func (m *telemetryManager) pkiCAPEM() []byte {
	if m == nil || m.pkiStore == nil {
		return nil
	}
	return m.pkiStore.CAPEM()
}

func (m *telemetryManager) pkiGetClientCertificate() func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	if m == nil || m.pkiStore == nil || m.legacyAuth {
		return nil
	}
	return m.pkiStore.GetClientCertificate
}

func (m *telemetryManager) renewWindow() time.Duration {
	days := agentConfig.Telemetry.CertRenewDays
	if days <= 0 {
		days = 7
	}
	return time.Duration(days) * 24 * time.Hour
}

func (m *telemetryManager) ensureDeviceCredentials() error {
	if m.pkiStore == nil {
		m.legacyAuth = true
		return nil
	}
	bundle, err := m.pkiStore.Load()
	if err != nil && !errors.Is(err, pki.ErrNotFound) {
		return err
	}
	now := time.Now()
	if bundle != nil && !bundle.Expired(now) {
		if bundle.NeedsRenew(now, m.renewWindow()) && !now.Before(m.nextRenew) {
			if renewErr := m.renewDeviceCertificate(); renewErr != nil {
				printf("设备证书续期失败，继续使用现有证书: %v", renewErr)
				if m.renewBackoff == 0 {
					m.renewBackoff = time.Second
				} else if m.renewBackoff < 10*time.Second {
					m.renewBackoff *= 2
				}
				m.nextRenew = now.Add(m.renewBackoff)
			} else {
				m.renewBackoff = 0
				m.nextRenew = time.Time{}
			}
		}
		m.legacyAuth = false
		return nil
	}
	if bundle != nil && bundle.Expired(now) && agentCliParam.ClientSecret == "" {
		return errors.New("device certificate expired; bootstrap client_secret is required")
	}
	if agentCliParam.TLS {
		enrollErr := m.enrollDeviceCertificate()
		if enrollErr == nil {
			m.legacyAuth = false
			return nil
		}
		if pki.IsLegacyPanel(enrollErr) {
			printf("面板不支持 Enrollment，回退到密钥认证")
			m.legacyAuth = true
			return nil
		}
		return enrollErr
	}
	m.legacyAuth = true
	return nil
}

func (m *telemetryManager) enrollmentDialOptions() ([]grpc.DialOption, error) {
	tlsCfg, err := pki.NewClientTLSConfig(pki.ClientTLSOptions{
		CAFile:             agentConfig.TLSCAFile,
		ServerName:         serverNameOf(agentCliParam.Server),
		InsecureSkipVerify: agentCliParam.InsecureTLS,
	})
	if err != nil {
		return nil, err
	}
	return []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithPerRPCCredentials(&pki.EnrollmentCredential{ClientSecret: agentCliParam.ClientSecret}),
	}, nil
}

func (m *telemetryManager) enrollDeviceCertificate() error {
	key, err := pki.GenerateKey()
	if err != nil {
		return err
	}
	csr, err := pki.CreateCSR(key, m.nodeID[:])
	if err != nil {
		return err
	}
	options, err := m.enrollmentDialOptions()
	if err != nil {
		return err
	}
	return m.tryDials(m.ctx, dialcache.PrimaryKey, agentCliParam.Server, options, func(conn *grpc.ClientConn, attempt *dialAttempt) error {
		response, err := pb.NewSantaiziEnrollmentServiceClient(conn).Enroll(m.ctx, &pb.AgentEnrollRequest{
			NodeUuid: m.nodeID[:], CsrDer: csr, AgentVersion: version,
		})
		if err != nil {
			return err
		}
		attempt.Remember()
		return m.saveIssuedCertificate(key, response.GetCertificatePem(), response.GetCaCertificatePem())
	})
}

func (m *telemetryManager) renewDeviceCertificate() error {
	key, err := pki.GenerateKey()
	if err != nil {
		return err
	}
	csr, err := pki.CreateCSR(key, m.nodeID[:])
	if err != nil {
		return err
	}
	options := m.dialOptions(true, agentCliParam.InsecureTLS, false, serverNameOf(agentCliParam.Server))
	return m.tryDials(m.ctx, dialcache.PrimaryKey, agentCliParam.Server, options, func(conn *grpc.ClientConn, attempt *dialAttempt) error {
		response, err := pb.NewSantaiziEnrollmentServiceClient(conn).Renew(m.ctx, &pb.AgentRenewRequest{
			NodeUuid: m.nodeID[:], CsrDer: csr, AgentVersion: version,
		})
		if err != nil {
			return err
		}
		attempt.Remember()
		return m.saveIssuedCertificate(key, response.GetCertificatePem(), response.GetCaCertificatePem())
	})
}

func (m *telemetryManager) saveIssuedCertificate(key ed25519.PrivateKey, certPEM, caPEM string) error {
	cert, err := pki.ParseCertificatePEM([]byte(certPEM))
	if err != nil {
		return err
	}
	return m.pkiStore.Save(&pki.Bundle{Key: key, Cert: cert, CertPEM: []byte(certPEM), CAPEM: []byte(caPEM)})
}

func cloneHost(host *pb.Host) *pb.Host {
	if host == nil {
		return nil
	}
	return proto.Clone(host).(*pb.Host)
}

func cloneState(state *pb.State) *pb.State {
	if state == nil {
		return nil
	}
	return proto.Clone(state).(*pb.State)
}

func readCredential(path string) (*pb.SignedAgentCredential, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	credential := new(pb.SignedAgentCredential)
	if err := proto.Unmarshal(data, credential); err != nil {
		return nil, fmt.Errorf("decode telemetry credential: %w", err)
	}
	return credential, nil
}

func writeCredential(path string, credential *pb.SignedAgentCredential) error {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(credential)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".credential-*.tmp")
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
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return util.SyncDir(filepath.Dir(path))
}
