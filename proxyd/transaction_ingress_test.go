package proxyd

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	txingressv1 "github.com/ethereum-optimism/infra/proxyd/base/tx_ingress/v1"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recordingTransactionIngressServer struct {
	txingressv1.UnimplementedTransactionIngressServiceServer

	mu       sync.Mutex
	requests []*txingressv1.SubmitRequest
	streams  int
}

func (s *recordingTransactionIngressServer) Submit(stream grpc.BidiStreamingServer[txingressv1.SubmitRequest, txingressv1.SubmitResponse]) error {
	s.mu.Lock()
	s.streams++
	s.mu.Unlock()

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	second, err := stream.Recv()
	if err != nil {
		return err
	}
	s.record(first, second)
	var rejected, accepted *txingressv1.SubmitRequest
	if first.RawTransaction[0] == 0x01 {
		rejected, accepted = first, second
	} else {
		rejected, accepted = second, first
	}

	if err := stream.Send(&txingressv1.SubmitResponse{
		RequestId: accepted.RequestId,
		Outcome: &txingressv1.SubmitResponse_TransactionHash{
			TransactionHash: common.Hash{31: 0x22}.Bytes(),
		},
	}); err != nil {
		return err
	}
	if err := stream.Send(&txingressv1.SubmitResponse{
		RequestId: rejected.RequestId,
		Outcome: &txingressv1.SubmitResponse_Error{Error: &txingressv1.SubmitError{
			Code:     -32000,
			Message:  "rejected",
			JsonData: []byte(`{"reason":"invalid"}`),
		}},
	}); err != nil {
		return err
	}

	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		s.record(request)
		if err := stream.Send(&txingressv1.SubmitResponse{
			RequestId: request.RequestId,
			Outcome: &txingressv1.SubmitResponse_TransactionHash{
				TransactionHash: common.Hash{31: 0x33}.Bytes(),
			},
		}); err != nil {
			return err
		}
	}
}

func (s *recordingTransactionIngressServer) record(requests ...*txingressv1.SubmitRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, requests...)
}

type reconnectingTransactionIngressServer struct {
	txingressv1.UnimplementedTransactionIngressServiceServer

	mu         sync.Mutex
	requestIDs []uint64
}

func (s *reconnectingTransactionIngressServer) Submit(stream grpc.BidiStreamingServer[txingressv1.SubmitRequest, txingressv1.SubmitResponse]) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.requestIDs = append(s.requestIDs, request.RequestId)
	streamNumber := len(s.requestIDs)
	s.mu.Unlock()
	if streamNumber == 1 {
		return status.Error(codes.Unavailable, "stream lost")
	}

	return stream.Send(&txingressv1.SubmitResponse{
		RequestId: request.RequestId,
		Outcome: &txingressv1.SubmitResponse_TransactionHash{
			TransactionHash: common.Hash{31: 0x44}.Bytes(),
		},
	})
}

func TestTransactionIngressForwardsBatchOverPersistentStream(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	service := new(recordingTransactionIngressServer)
	txingressv1.RegisterTransactionIngressServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	backend := NewBackend(
		"mempool",
		"http://unused",
		"",
		semaphore.NewWeighted(10),
		WithTransactionIngress(listener.Addr().String()),
	)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	responses, err := backend.Forward(ctx, []*RPCReq{
		{JSONRPC: JSONRPCVersion, Method: "eth_sendRawTransaction", Params: json.RawMessage(`["0x01"]`), ID: json.RawMessage(`41`)},
		{JSONRPC: JSONRPCVersion, Method: "eth_sendRawTransaction", Params: json.RawMessage(`["0x02"]`), ID: json.RawMessage(`7`)},
	}, true)
	require.NoError(t, err)
	require.Equal(t, -32000, responses[0].Error.Code)
	require.Equal(t, "rejected", responses[0].Error.Message)
	require.JSONEq(t, `{"reason":"invalid"}`, string(responses[0].Error.Data))
	require.Equal(t, common.HexToHash("0x22").Hex(), responses[1].Result)

	responses, err = backend.Forward(ctx, []*RPCReq{
		{JSONRPC: JSONRPCVersion, Method: "eth_sendRawTransaction", Params: json.RawMessage(`["0x03"]`), ID: json.RawMessage(`9`)},
	}, false)
	require.NoError(t, err)
	require.Equal(t, common.HexToHash("0x33").Hex(), responses[0].Result)

	service.mu.Lock()
	defer service.mu.Unlock()
	require.Equal(t, 1, service.streams)
	require.ElementsMatch(t, []uint64{0, 1}, []uint64{
		service.requests[0].RequestId,
		service.requests[1].RequestId,
	})
	require.Equal(t, uint64(2), service.requests[2].RequestId)
	require.ElementsMatch(t, [][]byte{{0x01}, {0x02}}, [][]byte{
		service.requests[0].RawTransaction,
		service.requests[1].RawTransaction,
	})
	require.Equal(t, []byte{0x03}, service.requests[2].RawTransaction)
}

func TestTransactionIngressReconnectResetsRequestIDs(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	service := new(reconnectingTransactionIngressServer)
	txingressv1.RegisterTransactionIngressServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client := NewTransactionIngressClient(listener.Addr().String())
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Submit(ctx, []byte{0x01})
	require.Error(t, err)
	response, err := client.Submit(ctx, []byte{0x02})
	require.NoError(t, err)
	require.Equal(t, common.Hash{31: 0x44}.Bytes(), response.GetTransactionHash())

	service.mu.Lock()
	defer service.mu.Unlock()
	require.Equal(t, []uint64{0, 0}, service.requestIDs)
}
