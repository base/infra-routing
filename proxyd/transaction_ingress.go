package proxyd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	txingressv1 "github.com/ethereum-optimism/infra/proxyd/base/tx_ingress/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var errTransactionIngressClosed = errors.New("transaction ingress client is closed")

type transactionIngressResult struct {
	response *txingressv1.SubmitResponse
	err      error
}

type transactionIngressSession struct {
	connection *grpc.ClientConn
	stream     txingressv1.TransactionIngressService_SubmitClient
	send       chan *txingressv1.SubmitRequest
	done       chan struct{}
	pending    map[uint64]chan transactionIngressResult
	nextID     uint64
}

// TransactionIngressClient submits transactions over one persistent bidirectional gRPC stream.
type TransactionIngressClient struct {
	endpoint string
	ctx      context.Context
	cancel   context.CancelFunc

	mu      sync.Mutex
	session *transactionIngressSession
	closed  bool
}

// NewTransactionIngressClient creates a transaction ingress client for endpoint.
func NewTransactionIngressClient(endpoint string) *TransactionIngressClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &TransactionIngressClient{endpoint: endpoint, ctx: ctx, cancel: cancel}
}

// Submit sends one raw transaction and waits for its correlated admission result.
func (c *TransactionIngressClient) Submit(ctx context.Context, rawTransaction []byte) (*txingressv1.SubmitResponse, error) {
	c.mu.Lock()
	session, err := c.sessionLocked()
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	requestID := session.nextID
	session.nextID++
	result := make(chan transactionIngressResult, 1)
	session.pending[requestID] = result
	c.mu.Unlock()

	request := &txingressv1.SubmitRequest{
		RequestId:      requestID,
		RawTransaction: rawTransaction,
	}
	select {
	case session.send <- request:
	case response := <-result:
		return response.response, response.err
	case <-ctx.Done():
		c.cancelRequest(session, requestID)
		return nil, ctx.Err()
	}

	select {
	case response := <-result:
		return response.response, response.err
	case <-ctx.Done():
		c.cancelRequest(session, requestID)
		return nil, ctx.Err()
	}
}

// Close terminates the stream and fails pending submissions.
func (c *TransactionIngressClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.cancel()
	session := c.session
	c.mu.Unlock()

	if session != nil {
		c.failSession(session, errTransactionIngressClosed)
	}
	return nil
}

func (c *TransactionIngressClient) sessionLocked() (*transactionIngressSession, error) {
	if c.closed {
		return nil, errTransactionIngressClosed
	}
	if c.session != nil {
		return c.session, nil
	}

	connection, err := grpc.NewClient(
		c.endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("create transaction ingress connection: %w", err)
	}
	stream, err := txingressv1.NewTransactionIngressServiceClient(connection).Submit(c.ctx)
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("open transaction ingress stream: %w", err)
	}

	session := &transactionIngressSession{
		connection: connection,
		stream:     stream,
		send:       make(chan *txingressv1.SubmitRequest),
		done:       make(chan struct{}),
		pending:    make(map[uint64]chan transactionIngressResult),
	}
	c.session = session
	go c.sendLoop(session)
	go c.receiveLoop(session)
	return session, nil
}

func (c *TransactionIngressClient) sendLoop(session *transactionIngressSession) {
	for {
		select {
		case request := <-session.send:
			if err := session.stream.Send(request); err != nil {
				c.failSession(session, fmt.Errorf("send transaction ingress request: %w", err))
				return
			}
		case <-session.done:
			return
		}
	}
}

func (c *TransactionIngressClient) receiveLoop(session *transactionIngressSession) {
	for {
		response, err := session.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = errors.New("transaction ingress stream closed")
			}
			c.failSession(session, fmt.Errorf("receive transaction ingress response: %w", err))
			return
		}

		c.mu.Lock()
		result := session.pending[response.GetRequestId()]
		delete(session.pending, response.GetRequestId())
		c.mu.Unlock()
		if result != nil {
			result <- transactionIngressResult{response: response}
		}
	}
}

func (c *TransactionIngressClient) cancelRequest(session *transactionIngressSession, requestID uint64) {
	c.mu.Lock()
	delete(session.pending, requestID)
	c.mu.Unlock()
}

func (c *TransactionIngressClient) failSession(session *transactionIngressSession, err error) {
	c.mu.Lock()
	if c.session != session {
		c.mu.Unlock()
		return
	}
	c.session = nil
	pending := session.pending
	session.pending = nil
	close(session.done)
	c.mu.Unlock()

	_ = session.connection.Close()
	for _, result := range pending {
		result <- transactionIngressResult{err: err}
	}
}
