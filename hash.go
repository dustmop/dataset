package dataset

import (
	"crypto/sha256"
	"github.com/jbenet/go-base58"
	// "encoding/hex"
	"encoding/json"
	"github.com/multiformats/go-multihash"
)

// JSONHash calculates the hash of a json.Marshaler
func JSONHash(m json.Marshaler) (hash string, err error) {
	// marshal to cannoncical JSON representation
	data, err := m.MarshalJSON()
	if err != nil {
		return
	}
	return HashBytes(data)
}

// TODO - this will have to place nice with IPFS block hashing strategies
func HashBytes(data []byte) (hash string, err error) {
	h := sha256.New()

	if _, err = h.Write(data); err != nil {
		return
	}

	mhBuf, err := multihash.Encode(h.Sum(nil), multihash.SHA2_256)
	if err != nil {
		return
	}

	hash = base58.Encode(mhBuf)
	return
}