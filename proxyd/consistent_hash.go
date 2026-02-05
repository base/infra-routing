package proxyd

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
	"github.com/ethereum/go-ethereum/common"
)

type ConsistentHashRouter struct {
	ring             *consistent.Consistent
	backends         map[string]*Backend
	salt             []byte
	numCandidates    int
	candidateTimeout time.Duration
	mu               sync.RWMutex
}

type hashMember string

func (h hashMember) String() string {
	return string(h)
}

type xxHasher struct{}

func (x xxHasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func NewConsistentHashRouter(
	backends []*Backend,
	salt string,
	numCandidates int,
	timeoutMs int,
) *ConsistentHashRouter {
	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            xxHasher{},
	}

	ring := consistent.New(nil, cfg)
	backendMap := make(map[string]*Backend)

	for _, backend := range backends {
		ring.Add(hashMember(backend.Name))
		backendMap[backend.Name] = backend
	}

	return &ConsistentHashRouter{
		ring:             ring,
		backends:         backendMap,
		salt:             []byte(salt),
		numCandidates:    numCandidates,
		candidateTimeout: time.Duration(timeoutMs) * time.Millisecond,
	}
}

func (r *ConsistentHashRouter) RouteToBackends(senderAddr common.Address) ([]*Backend, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	h := sha256.New()
	h.Write(senderAddr.Bytes())
	h.Write(r.salt)
	key := h.Sum(nil)

	numToFetch := r.numCandidates
	if numToFetch > len(r.backends) {
		numToFetch = len(r.backends)
	}

	members, err := r.ring.GetClosestN(key, numToFetch)
	if err != nil {
		return nil, fmt.Errorf("failed to get closest members: %w", err)
	}

	candidates := make([]*Backend, 0, len(members))
	for _, member := range members {
		backend := r.backends[member.String()]
		if backend != nil {
			candidates = append(candidates, backend)
		}
	}

	if len(candidates) == 0 {
		return nil, ErrNoBackends
	}

	return candidates, nil
}

func (r *ConsistentHashRouter) GetCandidateTimeout() time.Duration {
	return r.candidateTimeout
}

func (r *ConsistentHashRouter) UpdateSalt(newSalt string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.salt = []byte(newSalt)
}

func (r *ConsistentHashRouter) AddBackend(backend *Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ring.Add(hashMember(backend.Name))
	r.backends[backend.Name] = backend
}

func (r *ConsistentHashRouter) RemoveBackend(backendName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ring.Remove(backendName)
	delete(r.backends, backendName)
}

func (r *ConsistentHashRouter) NumBackends() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.backends)
}
