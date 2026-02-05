package proxyd

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func createMockBackend(name string, healthy bool) *Backend {
	b := NewBackend(name, "http://localhost:8545", "", semaphore.NewWeighted(10))
	b.probeSpec = &ProbeSpec{}
	b.healthyProbe.Store(healthy)
	return b
}

// TestSortScoredBackends tests the sortScoredBackends function.
func TestSortScoredBackends(t *testing.T) {
	t.Run("healthy backends sorted before unhealthy", func(t *testing.T) {
		scored := []scoredBackend{
			{backend: &Backend{Name: "unhealthy-1"}, score: 100, healthy: false},
			{backend: &Backend{Name: "healthy-1"}, score: 50, healthy: true},
			{backend: &Backend{Name: "unhealthy-2"}, score: 200, healthy: false},
			{backend: &Backend{Name: "healthy-2"}, score: 25, healthy: true},
		}

		sortScoredBackends(scored)

		require.True(t, scored[0].healthy)
		require.True(t, scored[1].healthy)
		require.False(t, scored[2].healthy)
		require.False(t, scored[3].healthy)
	})

	t.Run("within same health status sorted by score descending", func(t *testing.T) {
		scored := []scoredBackend{
			{backend: &Backend{Name: "low"}, score: 10, healthy: true},
			{backend: &Backend{Name: "high"}, score: 100, healthy: true},
			{backend: &Backend{Name: "mid"}, score: 50, healthy: true},
		}

		sortScoredBackends(scored)

		require.Equal(t, uint64(100), scored[0].score)
		require.Equal(t, uint64(50), scored[1].score)
		require.Equal(t, uint64(10), scored[2].score)
	})

	t.Run("unhealthy backends also sorted by score descending", func(t *testing.T) {
		scored := []scoredBackend{
			{backend: &Backend{Name: "low"}, score: 10, healthy: false},
			{backend: &Backend{Name: "high"}, score: 100, healthy: false},
			{backend: &Backend{Name: "mid"}, score: 50, healthy: false},
		}

		sortScoredBackends(scored)

		require.Equal(t, uint64(100), scored[0].score)
		require.Equal(t, uint64(50), scored[1].score)
		require.Equal(t, uint64(10), scored[2].score)
	})

	t.Run("empty slice", func(t *testing.T) {
		scored := []scoredBackend{}
		sortScoredBackends(scored)
		require.Empty(t, scored)
	})

	t.Run("single element", func(t *testing.T) {
		scored := []scoredBackend{
			{backend: &Backend{Name: "only"}, score: 42, healthy: true},
		}
		sortScoredBackends(scored)
		require.Len(t, scored, 1)
		require.Equal(t, "only", scored[0].backend.Name)
	})

	t.Run("mixed health and scores", func(t *testing.T) {
		scored := []scoredBackend{
			{backend: &Backend{Name: "u-low"}, score: 5, healthy: false},
			{backend: &Backend{Name: "h-high"}, score: 100, healthy: true},
			{backend: &Backend{Name: "u-high"}, score: 200, healthy: false},
			{backend: &Backend{Name: "h-low"}, score: 10, healthy: true},
		}

		sortScoredBackends(scored)

		require.Equal(t, "h-high", scored[0].backend.Name)
		require.Equal(t, "h-low", scored[1].backend.Name)
		require.Equal(t, "u-high", scored[2].backend.Name)
		require.Equal(t, "u-low", scored[3].backend.Name)
	})
}

// TestSenderHashRouter_DeterministicRouting verifies that the same sender always gets
// the same backend ordering when using the same salt.
func TestSenderHashRouter_DeterministicRouting(t *testing.T) {
	router := NewSenderHashRouter("test-salt")

	backends := []*Backend{
		createMockBackend("backend-1", true),
		createMockBackend("backend-2", true),
		createMockBackend("backend-3", true),
	}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")

	ordered1 := router.OrderedBackendsForSender(sender, backends)
	ordered2 := router.OrderedBackendsForSender(sender, backends)
	ordered3 := router.OrderedBackendsForSender(sender, backends)

	require.NotEmpty(t, ordered1)
	require.Equal(t, ordered1[0].Name, ordered2[0].Name)
	require.Equal(t, ordered2[0].Name, ordered3[0].Name)
}

// TestSenderHashRouter_SaltSensitivity verifies that different salts produce
// different backend orderings for the same sender.
func TestSenderHashRouter_SaltSensitivity(t *testing.T) {
	router1 := NewSenderHashRouter("salt-one")
	router2 := NewSenderHashRouter("salt-two")

	backends := []*Backend{
		createMockBackend("backend-1", true),
		createMockBackend("backend-2", true),
		createMockBackend("backend-3", true),
		createMockBackend("backend-4", true),
		createMockBackend("backend-5", true),
	}

	differentSelections := 0
	for i := 0; i < 100; i++ {
		sender := common.HexToAddress("0x" + string(rune('A'+i%26)) + "42d35Cc6634C0532925a3b844Bc9e7595f5e3E1")
		ordered1 := router1.OrderedBackendsForSender(sender, backends)
		ordered2 := router2.OrderedBackendsForSender(sender, backends)

		if ordered1[0].Name != ordered2[0].Name {
			differentSelections++
		}
	}

	require.Greater(t, differentSelections, 0)
}

// TestSenderHashRouter_HealthAwareRouting verifies that healthy backends are
// sorted before unhealthy backends.
func TestSenderHashRouter_HealthAwareRouting(t *testing.T) {
	router := NewSenderHashRouter("health-test-salt")

	healthyBackend := createMockBackend("healthy-backend", true)
	unhealthyBackend1 := createMockBackend("unhealthy-1", false)
	unhealthyBackend2 := createMockBackend("unhealthy-2", false)

	backends := []*Backend{unhealthyBackend1, healthyBackend, unhealthyBackend2}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")
	ordered := router.OrderedBackendsForSender(sender, backends)

	require.Len(t, ordered, 3)
	require.Equal(t, "healthy-backend", ordered[0].Name)
}

// TestSenderHashRouter_AllUnhealthy verifies that when all backends are unhealthy,
// they are still returned (sorted by score) as fallback options.
func TestSenderHashRouter_AllUnhealthy(t *testing.T) {
	router := NewSenderHashRouter("all-unhealthy-salt")

	backends := []*Backend{
		createMockBackend("backend-1", false),
		createMockBackend("backend-2", false),
		createMockBackend("backend-3", false),
	}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")
	ordered := router.OrderedBackendsForSender(sender, backends)

	require.Len(t, ordered, 3)
	for _, b := range ordered {
		require.False(t, b.IsHealthy())
	}
}

// TestSenderHashRouter_EmptyBackends verifies that nil is returned when there
// are no backends available.
func TestSenderHashRouter_EmptyBackends(t *testing.T) {
	router := NewSenderHashRouter("empty-test-salt")

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")
	ordered := router.OrderedBackendsForSender(sender, []*Backend{})

	require.Nil(t, ordered)
}

// TestSenderHashRouter_SingleBackend verifies that a single backend is returned
// as the only element in the ordered list.
func TestSenderHashRouter_SingleBackend(t *testing.T) {
	router := NewSenderHashRouter("single-backend-salt")

	backend := createMockBackend("only-backend", true)
	backends := []*Backend{backend}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")
	ordered := router.OrderedBackendsForSender(sender, backends)

	require.Len(t, ordered, 1)
	require.Equal(t, "only-backend", ordered[0].Name)
}

// TestSenderHashRouter_OrderedBackendsHealthyFirst verifies that healthy backends
// appear before unhealthy backends in the ordered list.
func TestSenderHashRouter_OrderedBackendsHealthyFirst(t *testing.T) {
	router := NewSenderHashRouter("healthy-first-salt")

	backends := []*Backend{
		createMockBackend("unhealthy-1", false),
		createMockBackend("healthy-1", true),
		createMockBackend("unhealthy-2", false),
		createMockBackend("healthy-2", true),
	}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")

	ordered := router.OrderedBackendsForSender(sender, backends)

	require.Len(t, ordered, 4)

	require.True(t, ordered[0].IsHealthy())
	require.True(t, ordered[1].IsHealthy())
	require.False(t, ordered[2].IsHealthy())
	require.False(t, ordered[3].IsHealthy())
}

// TestSenderHashRouter_FailoverBehavior verifies that when the primary backend
// becomes unhealthy, the secondary backend moves to the front of the list.
func TestSenderHashRouter_FailoverBehavior(t *testing.T) {
	router := NewSenderHashRouter("failover-salt")

	backend1 := createMockBackend("backend-1", true)
	backend2 := createMockBackend("backend-2", true)
	backend3 := createMockBackend("backend-3", true)

	backends := []*Backend{backend1, backend2, backend3}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")

	orderedBefore := router.OrderedBackendsForSender(sender, backends)
	primaryName := orderedBefore[0].Name
	secondaryName := orderedBefore[1].Name

	for _, b := range backends {
		if b.Name == primaryName {
			b.healthyProbe.Store(false)
		}
	}

	orderedAfter := router.OrderedBackendsForSender(sender, backends)
	require.Equal(t, secondaryName, orderedAfter[0].Name)
	require.Equal(t, primaryName, orderedAfter[len(orderedAfter)-1].Name)
}

// TestSenderHashRouter_ConcurrentAccess verifies that the router is safe for
// concurrent access from multiple goroutines.
func TestSenderHashRouter_ConcurrentAccess(t *testing.T) {
	router := NewSenderHashRouter("concurrent-salt")

	backends := []*Backend{
		createMockBackend("backend-1", true),
		createMockBackend("backend-2", true),
		createMockBackend("backend-3", true),
	}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")

	var successCount atomic.Int32
	numGoroutines := 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			ordered := router.OrderedBackendsForSender(sender, backends)
			if len(ordered) > 0 {
				successCount.Add(1)
			}
			done <- true
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	require.Equal(t, int32(numGoroutines), successCount.Load())
}

// TestIsSendRawTransactionMethod verifies that sendRawTransaction method variants
// are correctly identified.
func TestIsSendRawTransactionMethod(t *testing.T) {
	tests := []struct {
		method   string
		expected bool
	}{
		{"eth_sendRawTransaction", true},
		{"eth_sendRawTransactionConditional", true},
		{"eth_sendRawTransactionSync", true},
		{"eth_call", false},
		{"eth_getBalance", false},
		{"eth_getTransactionReceipt", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			result := IsSendRawTransactionMethod(tt.method)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractSenderFromTx verifies that the sender address is correctly extracted
// from a signed transaction.
func TestExtractSenderFromTx(t *testing.T) {
	tests := []struct {
		name           string
		txHex          string
		expectedSender string
	}{
		{
			name: "EIP-1559 tx on chain 420",
			txHex: "0x02f8b28201a406849502f931849502f931830147f9948f3ddd0fbf3e78ca1d6c" +
				"d17379ed88e261249b5280b84447e7ef2400000000000000000000000089c8b1" +
				"b2774201bac50f627403eac1b732459cf7000000000000000000000000000000" +
				"0000000000000000056bc75e2d63100000c080a0473c95566026c312c9664cd6" +
				"1145d2f3e759d49209fe96011ac012884ec5b017a0763b58f6fa6096e6ba28ee" +
				"08bfac58f58fb3b8bcef5af98578bdeaddf40bde42",
			expectedSender: "0x155c651ABd923B19f7b5440F23d3ba1a57784876",
		},
		{
			name: "simple transfer on chain 420",
			txHex: "0x02f8758201a48217fd84773594008504a817c80082520894be53e587975603" +
				"a13d0923d0aa6d37c5233dd750865af3107a400080c080a04aefbd5819c35729" +
				"138fe26b6ae1783ebf08d249b356c2f920345db97877f3f7a008d5ae92560a3c" +
				"65f723439887205713af7ce7d7f6b24fba198f2afa03435867",
			expectedSender: "0xbe53E587975603A13D0923D0AA6d37C5233DD750",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := hexutil.Decode(tt.txHex)
			require.NoError(t, err)

			tx := new(types.Transaction)
			err = tx.UnmarshalBinary(data)
			require.NoError(t, err)

			sender, err := ExtractSenderFromTx(context.Background(), tx)
			require.NoError(t, err)
			require.Equal(t, tt.expectedSender, sender.Hex())
		})
	}
}

// TestParseRawTx verifies that raw transaction hex is correctly parsed.
func TestParseRawTx(t *testing.T) {
	t.Run("invalid params", func(t *testing.T) {
		req := &RPCReq{
			Method: "eth_sendRawTransaction",
			Params: []byte(`"not an array"`),
		}

		_, err := ParseRawTx(req)
		require.Error(t, err)
	})

	t.Run("empty params", func(t *testing.T) {
		req := &RPCReq{
			Method: "eth_sendRawTransaction",
			Params: []byte(`[]`),
		}

		_, err := ParseRawTx(req)
		require.Error(t, err)
	})

	t.Run("invalid hex", func(t *testing.T) {
		req := &RPCReq{
			Method: "eth_sendRawTransaction",
			Params: []byte(`["not-valid-hex"]`),
		}

		_, err := ParseRawTx(req)
		require.Error(t, err)
	})

	t.Run("valid hex", func(t *testing.T) {
		validTxHex := "0x02f8b28201a406849502f931849502f931830147f9948f3ddd0fbf3e78ca1d6c" +
			"d17379ed88e261249b5280b84447e7ef2400000000000000000000000089c8b1" +
			"b2774201bac50f627403eac1b732459cf7000000000000000000000000000000" +
			"0000000000000000056bc75e2d63100000c080a0473c95566026c312c9664cd6" +
			"1145d2f3e759d49209fe96011ac012884ec5b017a0763b58f6fa6096e6ba28ee" +
			"08bfac58f58fb3b8bcef5af98578bdeaddf40bde42"
		req := &RPCReq{
			Method: "eth_sendRawTransaction",
			Params: []byte(`["` + validTxHex + `"]`),
		}

		tx, err := ParseRawTx(req)
		require.NoError(t, err)
		require.NotNil(t, tx)
	})
}

// TestSenderHashRouter_DifferentSendersDistribute verifies that different senders
// are distributed across multiple backends (not all routed to the same one).
func TestSenderHashRouter_DifferentSendersDistribute(t *testing.T) {
	router := NewSenderHashRouter("distribution-salt")

	backends := []*Backend{
		createMockBackend("backend-1", true),
		createMockBackend("backend-2", true),
		createMockBackend("backend-3", true),
	}

	senders := []common.Address{
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
		common.HexToAddress("0x2222222222222222222222222222222222222222"),
		common.HexToAddress("0x3333333333333333333333333333333333333333"),
		common.HexToAddress("0x4444444444444444444444444444444444444444"),
		common.HexToAddress("0x5555555555555555555555555555555555555555"),
		common.HexToAddress("0x6666666666666666666666666666666666666666"),
	}

	selections := make(map[string]bool)
	for _, sender := range senders {
		ordered := router.OrderedBackendsForSender(sender, backends)
		selections[ordered[0].Name] = true
	}

	require.GreaterOrEqual(t, len(selections), 2)
}
