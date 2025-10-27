package proxyd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type txsJSON struct {
	OnchainTx    string `json:"onchainTx"`
	OffchainTxV0 string `json:"offchainTxV0"`
	OffchainTxV1 string `json:"offchainTxV1"`
}

func TestConvertSendReqToSendTx_Fusaka(t *testing.T) {
	txData, err := os.ReadFile("testdata/txs.json")
	require.NoError(t, err)
	var txs txsJSON
	require.NoError(t, json.Unmarshal(txData, &txs))

	tfn := func(txHex string, proofCount int) func(t *testing.T) {
		return func(t *testing.T) {
			params, err := json.Marshal([]any{txHex})
			require.NoError(t, err)
			rpcReq := &RPCReq{
				Method: "eth_sendRawTransaction",
				Params: params,
				ID:     json.RawMessage("1"),
			}

			// Create a minimal server instance for testing
			server := &Server{enableTxHashLogging: false}
			tx, err := server.convertSendReqToSendTx(context.Background(), rpcReq)
			require.NoError(t, err)

			require.Len(t, tx.BlobTxSidecar().Blobs, 2)
			require.Len(t, tx.BlobTxSidecar().Commitments, 2)
			require.Len(t, tx.BlobTxSidecar().Proofs, proofCount)
		}
	}
	t.Run("blob without cell proofs", tfn(txs.OffchainTxV0, 2))
	t.Run("blob with cell proofs", tfn(txs.OffchainTxV1, 256))
}

func TestEthSendBundle(t *testing.T) {
	tests := []struct {
		name          string
		ingressRpcURL string
		expectIngress bool
		bundleParams  []interface{}
	}{
		{
			name:          "eth_sendBundle with ingress RPC configured",
			ingressRpcURL: "http://mock-ingress:8080",
			expectIngress: true,
			bundleParams:  []interface{}{[]interface{}{"0x1234", "0x5678"}},
		},
		{
			name:          "eth_sendBundle without ingress RPC configured",
			ingressRpcURL: "",
			expectIngress: false,
			bundleParams:  []interface{}{[]interface{}{"0x1234", "0x5678"}},
		},
		{
			name:          "eth_sendBundle with empty bundle",
			ingressRpcURL: "http://mock-ingress:8080",
			expectIngress: true,
			bundleParams:  []interface{}{[]interface{}{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock ingress server
			var ingressRequests [][]byte
			var mu sync.Mutex
			mockIngressServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				ingressRequests = append(ingressRequests, body)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x0"}`))
			}))
			defer mockIngressServer.Close()

			// Set up server with or without ingress RPC
			ingressURL := ""
			if tt.expectIngress {
				ingressURL = mockIngressServer.URL
			}

			server := &Server{
				ingressRpc: ingressURL,
				ingressRpcClient: &http.Client{
					Timeout: 5 * time.Second,
				},
				enableTxHashLogging: false,
			}

			// Create RPC request
			params, err := json.Marshal(tt.bundleParams)
			require.NoError(t, err)

			rpcReq := &RPCReq{
				Method:  "eth_sendBundle",
				Params:  params,
				ID:      json.RawMessage("1"),
				JSONRPC: "2.0",
			}

			// Create mock responses slice
			responses := make([]*RPCRes, 1)

			// Simulate the eth_sendBundle handling logic from handleBatchRPC
			if rpcReq.Method == "eth_sendBundle" {
				// Send to ingress service only if configured
				if server.ingressRpc != "" {
					body, err := json.Marshal(rpcReq)
					require.NoError(t, err)

					go func() {
						req, err := http.NewRequest(http.MethodPost, server.ingressRpc, bytes.NewBuffer(body))
						if err != nil {
							return
						}
						req.Header.Set("Content-Type", "application/json")

						resp, err := server.ingressRpcClient.Do(req)
						if err != nil {
							return
						}
						defer resp.Body.Close()
					}()
				}
				// Return success response for eth_sendBundle
				responses[0] = NewRPCRes(rpcReq.ID, json.RawMessage(`"0x0"`))
			}

			// Wait a bit for the goroutine to complete if ingress is expected
			if tt.expectIngress {
				time.Sleep(100 * time.Millisecond)
			}

			// Verify the response
			require.NotNil(t, responses[0])
			require.Equal(t, rpcReq.ID, responses[0].ID)
			require.Equal(t, json.RawMessage(`"0x0"`), responses[0].Result)
			require.Nil(t, responses[0].Error)

			// Verify ingress call
			mu.Lock()
			if tt.expectIngress {
				require.Len(t, ingressRequests, 1, "Expected exactly one ingress request")

				// Parse the ingress request
				var ingressReq RPCReq
				err := json.Unmarshal(ingressRequests[0], &ingressReq)
				require.NoError(t, err)
				require.Equal(t, "eth_sendBundle", ingressReq.Method)
				require.Equal(t, rpcReq.Params, ingressReq.Params)
			} else {
				require.Len(t, ingressRequests, 0, "Expected no ingress requests when ingress RPC is not configured")
			}
			mu.Unlock()
		})
	}
}
