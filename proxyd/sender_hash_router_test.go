package proxyd

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func createMockBackend(name string, healthy bool) *Backend {
	b := NewBackend(name, "http://localhost:8545", "", semaphore.NewWeighted(10))
	b.probeSpec = &ProbeSpec{}
	b.healthyProbe.Store(healthy)
	return b
}

func TestSenderHashRouter_DeterministicRouting(t *testing.T) {
	router := NewSenderHashRouter("test-salt")

	backends := []*Backend{
		createMockBackend("backend-1", true),
		createMockBackend("backend-2", true),
		createMockBackend("backend-3", true),
	}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")

	selected1 := router.SelectBackend(sender, backends)
	selected2 := router.SelectBackend(sender, backends)
	selected3 := router.SelectBackend(sender, backends)

	require.NotNil(t, selected1)
	require.Equal(t, selected1.Name, selected2.Name)
	require.Equal(t, selected2.Name, selected3.Name)
}

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
		selected1 := router1.SelectBackend(sender, backends)
		selected2 := router2.SelectBackend(sender, backends)

		if selected1.Name != selected2.Name {
			differentSelections++
		}
	}

	require.Greater(t, differentSelections, 0)
}

func TestSenderHashRouter_DistributionQuality(t *testing.T) {
	router := NewSenderHashRouter("distribution-test-salt")

	backends := []*Backend{
		createMockBackend("backend-1", true),
		createMockBackend("backend-2", true),
		createMockBackend("backend-3", true),
	}

	counts := make(map[string]int)
	numSamples := 3000

	for i := 0; i < numSamples; i++ {
		sender := common.BigToAddress(common.Big1.Add(common.Big1, common.Big1.SetInt64(int64(i*12345))))
		selected := router.SelectBackend(sender, backends)
		counts[selected.Name]++
	}

	expectedPerBackend := float64(numSamples) / float64(len(backends))
	tolerance := expectedPerBackend * 0.25

	for name, count := range counts {
		deviation := math.Abs(float64(count) - expectedPerBackend)
		require.LessOrEqual(t, deviation, tolerance,
			"Backend %s has count %d, expected ~%.0f (deviation %.0f > tolerance %.0f)",
			name, count, expectedPerBackend, deviation, tolerance)
	}
}

func TestSenderHashRouter_HealthAwareRouting(t *testing.T) {
	router := NewSenderHashRouter("health-test-salt")

	healthyBackend := createMockBackend("healthy-backend", true)
	unhealthyBackend1 := createMockBackend("unhealthy-1", false)
	unhealthyBackend2 := createMockBackend("unhealthy-2", false)

	backends := []*Backend{unhealthyBackend1, healthyBackend, unhealthyBackend2}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")
	selected := router.SelectBackend(sender, backends)

	require.NotNil(t, selected)
	require.Equal(t, "healthy-backend", selected.Name)
}

func TestSenderHashRouter_AllUnhealthy(t *testing.T) {
	router := NewSenderHashRouter("all-unhealthy-salt")

	backends := []*Backend{
		createMockBackend("backend-1", false),
		createMockBackend("backend-2", false),
		createMockBackend("backend-3", false),
	}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")
	selected := router.SelectBackend(sender, backends)

	require.Nil(t, selected)
}

func TestSenderHashRouter_EmptyBackends(t *testing.T) {
	router := NewSenderHashRouter("empty-test-salt")

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")
	selected := router.SelectBackend(sender, []*Backend{})

	require.Nil(t, selected)
}

func TestSenderHashRouter_SingleBackend(t *testing.T) {
	router := NewSenderHashRouter("single-backend-salt")

	backend := createMockBackend("only-backend", true)
	backends := []*Backend{backend}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")
	selected := router.SelectBackend(sender, backends)

	require.NotNil(t, selected)
	require.Equal(t, "only-backend", selected.Name)
}

func TestSenderHashRouter_OrderedBackendsForSender(t *testing.T) {
	router := NewSenderHashRouter("ordered-test-salt")

	backends := []*Backend{
		createMockBackend("backend-1", true),
		createMockBackend("backend-2", true),
		createMockBackend("backend-3", true),
	}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")

	ordered1 := router.OrderedBackendsForSender(sender, backends)
	ordered2 := router.OrderedBackendsForSender(sender, backends)

	require.Len(t, ordered1, 3)
	require.Len(t, ordered2, 3)

	for i := range ordered1 {
		require.Equal(t, ordered1[i].Name, ordered2[i].Name)
	}
}

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
}

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
			selected := router.SelectBackend(sender, backends)
			if selected != nil {
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

func TestFilterHealthyBackends(t *testing.T) {
	backends := []*Backend{
		createMockBackend("healthy-1", true),
		createMockBackend("unhealthy-1", false),
		createMockBackend("healthy-2", true),
		createMockBackend("unhealthy-2", false),
	}

	healthy := filterHealthyBackends(backends)

	require.Len(t, healthy, 2)
	for _, b := range healthy {
		require.True(t, b.IsHealthy())
	}
}

func TestExtractSenderFromTx(t *testing.T) {
	t.Skip("Skipping sender extraction test - requires signed transaction fixtures")
}

func TestParseRawTx(t *testing.T) {
	t.Run("invalid params", func(t *testing.T) {
		req := &RPCReq{
			Method: "eth_sendRawTransaction",
			Params: []byte(`"not an array"`),
		}

		_, err := parseRawTx(req)
		require.Error(t, err)
	})

	t.Run("empty params", func(t *testing.T) {
		req := &RPCReq{
			Method: "eth_sendRawTransaction",
			Params: []byte(`[]`),
		}

		_, err := parseRawTx(req)
		require.Error(t, err)
	})

	t.Run("invalid hex", func(t *testing.T) {
		req := &RPCReq{
			Method: "eth_sendRawTransaction",
			Params: []byte(`["not-valid-hex"]`),
		}

		_, err := parseRawTx(req)
		require.Error(t, err)
	})
}

func TestNewSenderHashRouter(t *testing.T) {
	router := NewSenderHashRouter("my-secret-salt")
	require.NotNil(t, router)
	require.Equal(t, []byte("my-secret-salt"), router.salt)
}

func TestSenderHashRouter_EmptySalt(t *testing.T) {
	router := NewSenderHashRouter("")

	backends := []*Backend{
		createMockBackend("backend-1", true),
		createMockBackend("backend-2", true),
	}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")
	selected := router.SelectBackend(sender, backends)

	require.NotNil(t, selected)

	selected2 := router.SelectBackend(sender, backends)
	require.Equal(t, selected.Name, selected2.Name)
}

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
		selected := router.SelectBackend(sender, backends)
		selections[selected.Name] = true
	}

	require.GreaterOrEqual(t, len(selections), 2)
}

func BenchmarkSenderHashRouter_SelectBackend(b *testing.B) {
	router := NewSenderHashRouter("benchmark-salt")

	backends := []*Backend{
		createMockBackend("backend-1", true),
		createMockBackend("backend-2", true),
		createMockBackend("backend-3", true),
		createMockBackend("backend-4", true),
		createMockBackend("backend-5", true),
	}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		router.SelectBackend(sender, backends)
	}
}

func BenchmarkSenderHashRouter_OrderedBackendsForSender(b *testing.B) {
	router := NewSenderHashRouter("benchmark-salt")

	backends := []*Backend{
		createMockBackend("backend-1", true),
		createMockBackend("backend-2", true),
		createMockBackend("backend-3", true),
		createMockBackend("backend-4", true),
		createMockBackend("backend-5", true),
	}

	sender := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f5e3E1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		router.OrderedBackendsForSender(sender, backends)
	}
}
