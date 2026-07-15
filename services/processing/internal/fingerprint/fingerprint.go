package fingerprint

import (
	"pulseguard/services/pkg/contracts"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
)

type Fingerprinter struct {
	pool *sync.Pool
}

func (f Fingerprinter) GenerateFingerprint(ee contracts.ErrorEvent) uint64 {
	h := f.pool.Get().(*xxhash.Digest)
	defer f.pool.Put(h)
	h.Reset()
	var pIdBytes [20]byte
	var lineBytes [20]byte

	h.Write(strconv.AppendInt(pIdBytes[:0], int64(ee.ProjectId), 10))
	h.Write([]byte{'|'})
	h.WriteString(ee.Type)
	h.Write([]byte{'|'})
	h.WriteString(ee.Level)
	h.Write([]byte{'|'})

	if len(ee.StackTrace) == 0 {
		h.Write([]byte{'u', 'n', 'k', 'n', 'o', 'w', 'n'})
	} else {
		h.WriteString(ee.StackTrace[0].File)
		h.Write([]byte{'|'})
		h.WriteString(ee.StackTrace[0].Method)
		h.Write([]byte{'|'})
		h.Write(strconv.AppendInt(lineBytes[:0], int64(ee.StackTrace[0].Line), 10))
	}

	return h.Sum64()
}
