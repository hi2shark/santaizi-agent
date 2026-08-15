package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/hi2shark/santaizi-agent/pkg/dialcache"
	pb "github.com/hi2shark/santaizi-agent/proto"
	"google.golang.org/grpc"
)

func (m *telemetryManager) startNAT(request *pb.NATOpenRequest) error {
	if !agentConfig.Capabilities.NAT {
		return errors.New("NAT capability is disabled")
	}
	if request.GetStreamId() == "" {
		return errors.New("stream_id is required")
	}
	if strings.TrimSpace(request.GetTargetHost()) == "" || request.GetTargetPort() == 0 || request.GetTargetPort() > 65535 {
		return errors.New("NAT target requires a host and valid port")
	}
	if expires := request.GetExpiresAtUnix(); expires != 0 && time.Now().Unix() >= expires {
		return errors.New("NAT request has expired")
	}

	ctx, cancel := context.WithCancel(m.ctx)
	targetAddress := net.JoinHostPort(request.GetTargetHost(), fmt.Sprint(request.GetTargetPort()))
	target, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "tcp", targetAddress)
	if err != nil {
		cancel()
		return err
	}
	options := m.dialOptions(agentCliParam.TLS, agentCliParam.InsecureTLS, m.legacyAuth, serverNameOf(agentCliParam.Server))
	err = m.tryDials(ctx, dialcache.PrimaryKey, agentCliParam.Server, options, func(rpcConnection *grpc.ClientConn, attempt *dialAttempt) error {
		stream, err := pb.NewSantaiziNATServiceClient(rpcConnection).NATStream(ctx)
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.NATFrame{StreamId: request.GetStreamId(), Kind: pb.NATFrameKind_NAT_FRAME_KIND_OPEN}); err != nil {
			return err
		}
		attempt.Remember()
		attempt.Detach()
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			defer cancel()
			defer rpcConnection.Close()
			defer target.Close()
			if err := relayNAT(ctx, request.GetStreamId(), target, stream); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
				printf("NAT 流 %s 已结束: %v", request.GetStreamId(), err)
			}
		}()
		return nil
	})
	if err != nil {
		_ = target.Close()
		cancel()
		return err
	}
	return nil
}

type natClientStream interface {
	Send(*pb.NATFrame) error
	Recv() (*pb.NATFrame, error)
	CloseSend() error
}

func relayNAT(ctx context.Context, streamID string, target net.Conn, stream natClientStream) error {
	errorsCh := make(chan error, 2)
	go func() {
		buffer := make([]byte, 32*1024)
		for {
			count, err := target.Read(buffer)
			if count > 0 {
				payload := append([]byte(nil), buffer[:count]...)
				if sendErr := stream.Send(&pb.NATFrame{StreamId: streamID, Kind: pb.NATFrameKind_NAT_FRAME_KIND_DATA, Data: payload}); sendErr != nil {
					errorsCh <- sendErr
					return
				}
			}
			if err != nil {
				kind := pb.NATFrameKind_NAT_FRAME_KIND_CLOSE
				message := ""
				if !errors.Is(err, io.EOF) {
					kind = pb.NATFrameKind_NAT_FRAME_KIND_ERROR
					message = err.Error()
				}
				_ = stream.Send(&pb.NATFrame{StreamId: streamID, Kind: kind, Error: message})
				_ = stream.CloseSend()
				errorsCh <- err
				return
			}
		}
	}()
	go func() {
		for {
			frame, err := stream.Recv()
			if err != nil {
				errorsCh <- err
				return
			}
			if frame.GetStreamId() != streamID {
				errorsCh <- errors.New("NAT stream identifier mismatch")
				return
			}
			switch frame.GetKind() {
			case pb.NATFrameKind_NAT_FRAME_KIND_DATA:
				if _, err := target.Write(frame.GetData()); err != nil {
					errorsCh <- err
					return
				}
			case pb.NATFrameKind_NAT_FRAME_KIND_CLOSE:
				errorsCh <- io.EOF
				return
			case pb.NATFrameKind_NAT_FRAME_KIND_ERROR:
				errorsCh <- errors.New(frame.GetError())
				return
			default:
				errorsCh <- errors.New("unexpected NAT frame kind")
				return
			}
		}
	}()
	select {
	case err := <-errorsCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
