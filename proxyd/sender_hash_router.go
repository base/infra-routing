package proxyd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

type SenderHashRouter struct {
	salt []byte
}

func NewSenderHashRouter(salt string) *SenderHashRouter {
	return &SenderHashRouter{salt: []byte(salt)}
}

func (r *SenderHashRouter) SelectBackend(senderAddress common.Address, backends []*Backend) *Backend {
	healthyBackends := filterHealthyBackends(backends)
	if len(healthyBackends) == 0 {
		return nil
	}

	if len(healthyBackends) == 1 {
		return healthyBackends[0]
	}

	var selectedBackend *Backend
	var maxScore uint64

	for _, backend := range healthyBackends {
		score := r.computeScore(senderAddress, backend.Name)
		if score > maxScore {
			maxScore = score
			selectedBackend = backend
		}
	}

	return selectedBackend
}

func (r *SenderHashRouter) computeScore(senderAddress common.Address, backendName string) uint64 {
	h := sha256.New()
	h.Write(senderAddress.Bytes())
	h.Write(r.salt)
	h.Write([]byte(backendName))
	hash := h.Sum(nil)
	return binary.BigEndian.Uint64(hash[:8])
}

func (r *SenderHashRouter) OrderedBackendsForSender(senderAddress common.Address, backends []*Backend) []*Backend {
	if len(backends) == 0 {
		return nil
	}

	type scoredBackend struct {
		backend *Backend
		score   uint64
		healthy bool
	}

	scored := make([]scoredBackend, len(backends))
	for i, b := range backends {
		scored[i] = scoredBackend{
			backend: b,
			score:   r.computeScore(senderAddress, b.Name),
			healthy: b.IsHealthy(),
		}
	}

	for i := 0; i < len(scored)-1; i++ {
		for j := i + 1; j < len(scored); j++ {
			swapNeeded := false
			if scored[i].healthy && scored[j].healthy {
				swapNeeded = scored[j].score > scored[i].score
			} else if !scored[i].healthy && scored[j].healthy {
				swapNeeded = true
			} else if scored[i].healthy && !scored[j].healthy {
				swapNeeded = false
			} else {
				swapNeeded = scored[j].score > scored[i].score
			}

			if swapNeeded {
				scored[i], scored[j] = scored[j], scored[i]
			}
		}
	}

	result := make([]*Backend, len(scored))
	for i, sb := range scored {
		result[i] = sb.backend
	}

	return result
}

func filterHealthyBackends(backends []*Backend) []*Backend {
	healthy := make([]*Backend, 0, len(backends))
	for _, b := range backends {
		if b.IsHealthy() {
			healthy = append(healthy, b)
		}
	}
	return healthy
}

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

func IsSendRawTransactionMethod(method string) bool {
	return method == "eth_sendRawTransaction" ||
		method == "eth_sendRawTransactionConditional" ||
		method == "eth_sendRawTransactionSync"
}

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
