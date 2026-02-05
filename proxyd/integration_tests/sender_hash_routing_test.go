package integration_tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/ethereum-optimism/infra/proxyd"
	"github.com/stretchr/testify/require"
)

const senderHashTxHex1 = "0x02f8b28201a406849502f931849502f931830147f9948f3ddd0fbf3e78ca1d6c" +
	"d17379ed88e261249b5280b84447e7ef2400000000000000000000000089c8b1" +
	"b2774201bac50f627403eac1b732459cf7000000000000000000000000000000" +
	"0000000000000000056bc75e2d63100000c080a0473c95566026c312c9664cd6" +
	"1145d2f3e759d49209fe96011ac012884ec5b017a0763b58f6fa6096e6ba28ee" +
	"08bfac58f58fb3b8bcef5af98578bdeaddf40bde42"

const senderHashTxHex2 = "0x02f8758201a48217fd84773594008504a817c80082520894be53e587975603" +
	"a13d0923d0aa6d37c5233dd750865af3107a400080c080a04aefbd5819c35729" +
	"138fe26b6ae1783ebf08d249b356c2f920345db97877f3f7a008d5ae92560a3c" +
	"65f723439887205713af7ce7d7f6b24fba198f2afa03435867"

const senderHashTxAccepted = `{"jsonrpc": "2.0","result": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef","id": 1}`
const senderHashDummyRes = `{"id": 123, "jsonrpc": "2.0", "result": "dummy"}`

// trackingBackend is a backend that tracks the number of requests it has served.
type trackingBackend struct {
	*MockBackend
	requestCount int32
}

// newTrackingBackend creates a new tracking backend with the given handler.
func newTrackingBackend(handler http.Handler) *trackingBackend {
	return &trackingBackend{
		MockBackend: NewMockBackend(handler),
	}
}

// Handler returns a handler that tracks the number of requests it has served.
func (t *trackingBackend) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&t.requestCount, 1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(senderHashTxAccepted))
	})
}

// GetRequestCount returns the number of requests it has served.
func (t *trackingBackend) GetRequestCount() int {
	return int(atomic.LoadInt32(&t.requestCount))
}

// ResetRequestCount resets the number of requests it has served.
func (t *trackingBackend) ResetRequestCount() {
	atomic.StoreInt32(&t.requestCount, 0)
}

func setupSenderHash(t *testing.T) (map[string]*trackingBackend, *proxyd.BackendGroup, *proxyd.Server, func()) {
	node1 := newTrackingBackend(nil)
	node2 := newTrackingBackend(nil)
	node3 := newTrackingBackend(nil)

	node1.SetHandler(node1.Handler())
	node2.SetHandler(node2.Handler())
	node3.SetHandler(node3.Handler())

	require.NoError(t, os.Setenv("NODE1_URL", node1.URL()))
	require.NoError(t, os.Setenv("NODE2_URL", node2.URL()))
	require.NoError(t, os.Setenv("NODE3_URL", node3.URL()))
	require.NoError(t, os.Setenv("SENDER_HASH_SALT", "test-salt-12345"))

	config := ReadConfig("sender_hash")
	svr, shutdown, err := proxyd.Start(config)
	require.NoError(t, err)

	bg := svr.BackendGroups["node"]
	require.NotNil(t, bg)
	require.Equal(t, 3, len(bg.Backends))
	require.Equal(t, proxyd.SenderHashRoutingStrategy, bg.GetRoutingStrategy())

	nodes := map[string]*trackingBackend{
		"node1": node1,
		"node2": node2,
		"node3": node3,
	}

	return nodes, bg, svr, func() {
		shutdown()
		node1.Close()
		node2.Close()
		node3.Close()
	}
}

func makeSenderHashRawTx(dataHex string) []byte {
	return []byte(`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["` + dataHex + `"],"id":1}`)
}

func makeEthCall() []byte {
	return []byte(`{"jsonrpc":"2.0","method":"eth_call","params":[{"to":"0x1234567890123456789012345678901234567890","data":"0x"},"latest"],"id":1}`)
}

func TestSenderHashRouting(t *testing.T) {
	t.Run("Same sender routes to same backend deterministically", func(t *testing.T) {
		nodes, _, svr, shutdown := setupSenderHash(t)
		defer shutdown()

		// Send 5 requests from the same sender to proxyd.
		for i := 0; i < 5; i++ {
			body := makeSenderHashRawTx(senderHashTxHex1)
			req, _ := http.NewRequest("POST", "https://1.1.1.1:8080", bytes.NewReader(body))
			req.Header.Set("X-Forwarded-For", "203.0.113.1")
			rr := httptest.NewRecorder()
			svr.HandleRPC(rr, req)

			resp := rr.Result()
			defer resp.Body.Close()
			require.Equal(t, 200, resp.StatusCode)
		}

		var receivingNode string
		for name, node := range nodes {
			count := node.GetRequestCount()
			// If there was a request, then that backend should have received all 5 requests.
			if count > 0 {
				require.Equal(t, 5, count, "receiving backend should get all 5 requests")
				require.Empty(t, receivingNode, "only one backend should receive requests")
				receivingNode = name
			} else {
				// All other backends should get zero requests.
				require.Equal(t, 0, count, "non-receiving backends should get zero requests")
			}
		}
		require.NotEmpty(t, receivingNode, "one backend should have received all requests")
	})

	t.Run("Different senders can route to different backends", func(t *testing.T) {
		nodes, _, svr, shutdown := setupSenderHash(t)
		defer shutdown()

		body1 := makeSenderHashRawTx(senderHashTxHex1)
		req1, _ := http.NewRequest("POST", "https://1.1.1.1:8080", bytes.NewReader(body1))
		req1.Header.Set("X-Forwarded-For", "203.0.113.1")
		rr1 := httptest.NewRecorder()
		svr.HandleRPC(rr1, req1)

		resp1 := rr1.Result()
		defer resp1.Body.Close()
		require.Equal(t, 200, resp1.StatusCode)
		servedBy1 := resp1.Header.Get("X-Served-By")

		body2 := makeSenderHashRawTx(senderHashTxHex2)
		req2, _ := http.NewRequest("POST", "https://1.1.1.1:8080", bytes.NewReader(body2))
		req2.Header.Set("X-Forwarded-For", "203.0.113.2")
		rr2 := httptest.NewRecorder()
		svr.HandleRPC(rr2, req2)

		resp2 := rr2.Result()
		defer resp2.Body.Close()
		require.Equal(t, 200, resp2.StatusCode)
		servedBy2 := resp2.Header.Get("X-Served-By")

		require.NotEmpty(t, servedBy1)
		require.NotEmpty(t, servedBy2)
		require.NotEqual(t, servedBy1, servedBy2, "different senders should route to different backends")

		totalRequests := 0
		backendsWithRequests := 0
		for _, node := range nodes {
			count := node.GetRequestCount()
			totalRequests += count
			// Since we only sent 1 request per sender, each backend should have received exactly 1 request.
			if count > 0 {
				backendsWithRequests++
				require.Equal(t, 1, count, "each backend should receive exactly 1 request")
			}
		}
		require.Equal(t, 2, totalRequests, "total requests should be 2")
		require.Equal(t, 2, backendsWithRequests, "exactly 2 backends should have received requests")
	})

	t.Run("Non-sendRawTransaction uses fallback routing", func(t *testing.T) {
		nodes, _, svr, shutdown := setupSenderHash(t)
		defer shutdown()

		for _, node := range nodes {
			node.SetHandler(SingleResponseHandler(200, senderHashDummyRes))
		}

		body := makeEthCall()
		req, _ := http.NewRequest("POST", "https://1.1.1.1:8080", bytes.NewReader(body))
		req.Header.Set("X-Forwarded-For", "203.0.113.1")
		rr := httptest.NewRecorder()
		svr.HandleRPC(rr, req)

		resp := rr.Result()
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)

		rpcRes := &proxyd.RPCRes{}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(rpcRes))
		require.False(t, rpcRes.IsError())
	})

	t.Run("Returns success when transaction is accepted", func(t *testing.T) {
		nodes, _, svr, shutdown := setupSenderHash(t)
		defer shutdown()

		body := makeSenderHashRawTx(senderHashTxHex1)
		req, _ := http.NewRequest("POST", "https://1.1.1.1:8080", bytes.NewReader(body))
		req.Header.Set("X-Forwarded-For", "203.0.113.1")
		rr := httptest.NewRecorder()
		svr.HandleRPC(rr, req)

		resp := rr.Result()
		defer resp.Body.Close()

		require.Equal(t, 200, resp.StatusCode)

		rpcRes := &proxyd.RPCRes{}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(rpcRes))
		require.False(t, rpcRes.IsError())
		require.NotNil(t, rpcRes.Result)

		totalRequests := 0
		for _, node := range nodes {
			totalRequests += node.GetRequestCount()
		}
		require.Equal(t, 1, totalRequests)
	})
}

func TestSenderHashRoutingFailover(t *testing.T) {
	t.Run("Routes to next backend when primary returns error", func(t *testing.T) {
		nodes, bg, svr, shutdown := setupSenderHash(t)
		defer shutdown()

		body := makeSenderHashRawTx(senderHashTxHex1)
		req, _ := http.NewRequest("POST", "https://1.1.1.1:8080", bytes.NewReader(body))
		req.Header.Set("X-Forwarded-For", "203.0.113.1")
		rr := httptest.NewRecorder()
		svr.HandleRPC(rr, req)

		resp := rr.Result()
		primaryServedBy := resp.Header.Get("X-Served-By")
		resp.Body.Close()
		require.NotEmpty(t, primaryServedBy)

		for _, node := range nodes {
			node.ResetRequestCount()
		}

		for name, node := range nodes {
			if "node/"+name == primaryServedBy {
				node.SetHandler(SingleResponseHandler(500, `{"error": "internal error"}`))
			}
		}

		body2 := makeSenderHashRawTx(senderHashTxHex1)
		req2, _ := http.NewRequest("POST", "https://1.1.1.1:8080", bytes.NewReader(body2))
		req2.Header.Set("X-Forwarded-For", "203.0.113.1")
		rr2 := httptest.NewRecorder()
		svr.HandleRPC(rr2, req2)

		resp2 := rr2.Result()
		defer resp2.Body.Close()

		secondaryServedBy := resp2.Header.Get("X-Served-By")
		require.NotEmpty(t, secondaryServedBy)
		require.NotEqual(t, primaryServedBy, secondaryServedBy, "after primary returns error, request should route to a different backend")

		require.Equal(t, 200, resp2.StatusCode)
		require.Equal(t, 3, len(bg.Backends))
	})
}
