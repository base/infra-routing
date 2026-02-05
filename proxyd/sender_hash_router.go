package proxyd

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// SenderHashRouter is a router that selects a backend for a sender address based on the sender hash.
type SenderHashRouter struct {
	salt []byte
}

// scoredBackend is a backend with a score and a healthy flag.
type scoredBackend struct {
	backend *Backend
	score   uint64
	healthy bool
}

// NewSenderHashRouter creates a new sender hash router with the given salt.
func NewSenderHashRouter(salt string) *SenderHashRouter {
	return &SenderHashRouter{salt: []byte(salt)}
}

// computeScore implements Highest Random Weighted hashing by computing
// a score from hash(senderAddress + salt + backendName).
//
// It returns the first 8 bytes of the hash as a uint64.
func (r *SenderHashRouter) computeScore(senderAddress common.Address, backendName string) uint64 {
	h := sha256.New()
	h.Write(senderAddress.Bytes())
	h.Write(r.salt)
	h.Write([]byte(backendName))
	hash := h.Sum(nil)
	return binary.BigEndian.Uint64(hash[:8])
}

// OrderedBackendsForSender returns the ordered backends for the given sender address.
func (r *SenderHashRouter) OrderedBackendsForSender(senderAddress common.Address, backends []*Backend) []*Backend {
	if len(backends) == 0 {
		return nil
	}

	scored := make([]*scoredBackend, len(backends))
	for i, b := range backends {
		scored[i] = &scoredBackend{
			backend: b,
			score:   r.computeScore(senderAddress, b.Name),
			healthy: b.IsHealthy(),
		}
	}
	sortScoredBackends(scored)

	result := make([]*Backend, len(scored))
	for i, sb := range scored {
		result[i] = sb.backend
	}

	return result
}

// sortScoredBackends sorts backends by health (healthy first) then by score (descending).
func sortScoredBackends(scored []*scoredBackend) {
	slices.SortFunc(scored, func(a, b *scoredBackend) int {
		if a.healthy != b.healthy {
			if a.healthy {
				return -1
			}
			return 1
		}
		return cmp.Compare(b.score, a.score)
	})
}

// IsSendRawTransactionMethod checks if the method is a sendRawTransaction equivalent method.
func IsSendRawTransactionMethod(method string) bool {
	return method == "eth_sendRawTransaction" ||
		method == "eth_sendRawTransactionConditional" ||
		method == "eth_sendRawTransactionSync"
}

// SplitBatchBySender splits a batch of RPC requests into groups based on sender address.
// Returns a map of sender address to requests, plus requests that should use default routing.
func (r *SenderHashRouter) SplitBatchBySender(ctx context.Context, reqs []*RPCReq) (map[common.Address][]*RPCReq, []*RPCReq) {
	senderReqs := make(map[common.Address][]*RPCReq)
	var defaultReqs []*RPCReq

	// Find out which requests are sendRawTransaction requests and which are not.
	for _, req := range reqs {
		if !IsSendRawTransactionMethod(req.Method) {
			defaultReqs = append(defaultReqs, req)
			continue
		}

		// Parse the transaction parameters
		var params []string
		if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
			log.Debug("failed to parse tx params, using default routing",
				"req_id", GetReqID(ctx),
				"err", err,
			)
			defaultReqs = append(defaultReqs, req)
			continue
		}
		tx, err := decodeSignedTx(ctx, params[0])
		if err != nil {
			log.Debug("failed to decode tx, using default routing",
				"req_id", GetReqID(ctx),
				"err", err,
			)
			defaultReqs = append(defaultReqs, req)
			continue
		}

		// Extract the sender address from the transaction
		sender, err := getSender(tx)
		if err != nil {
			log.Debug("failed to extract sender, using default routing",
				"req_id", GetReqID(ctx),
				"err", err,
			)
			defaultReqs = append(defaultReqs, req)
			continue
		}

		senderReqs[sender] = append(senderReqs[sender], req)
	}

	return senderReqs, defaultReqs
}
