package proxyd

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
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

	// Get the health and score for each backend and sort it
	//
	// Note: `b.IsHealthy()` calls on the backend's probeSpec to check if the backend is healthy.
	scored := make([]scoredBackend, len(backends))
	for i, b := range backends {
		scored[i] = scoredBackend{
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
// `slices.SortFunc` is O(n log n).
func sortScoredBackends(scored []scoredBackend) {
	slices.SortFunc(scored, func(a, b scoredBackend) int {
		if a.healthy != b.healthy {
			if a.healthy {
				return -1
			}
			return 1
		}
		return cmp.Compare(b.score, a.score)
	})
}

// ExtractSenderFromTx extracts the sender from the transaction.
func ExtractSenderFromTx(ctx context.Context, tx *types.Transaction) (common.Address, error) {
	var signer types.Signer
	if tx.ChainId().Sign() == 0 {
		signer = new(types.HomesteadSigner)
	} else {
		signer = types.LatestSignerForChainID(tx.ChainId())
	}

	from, err := types.Sender(signer, tx)
	if err != nil {
		log.Debug("failed to extract sender from transaction", "err", err, "req_id", GetReqID(ctx))
		return common.Address{}, err
	}

	return from, nil
}

// IsSendRawTransactionMethod checks if the method is a sendRawTransaction equivalent method.
func IsSendRawTransactionMethod(method string) bool {
	return method == "eth_sendRawTransaction" ||
		method == "eth_sendRawTransactionConditional" ||
		method == "eth_sendRawTransactionSync"
}

// parseRawTx parses the raw transaction from the request parameters.
func parseRawTx(req *RPCReq) (*types.Transaction, error) {
	var params []string
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return nil, err
	}

	if len(params) == 0 {
		return nil, ErrInvalidParams("missing transaction data")
	}

	var data hexutil.Bytes
	if err := data.UnmarshalText([]byte(params[0])); err != nil {
		return nil, err
	}

	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(data); err != nil {
		return nil, err
	}

	return tx, nil
}
