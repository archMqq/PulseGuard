package buffers

import (
	"strconv"
	"unsafe"
)

type NumBuffer struct {
	buf [20]byte
}

func (nb *NumBuffer) Convert(val uint64) string {
	sl := nb.buf[:0]

	res := strconv.AppendUint(sl, val, 10)

	return unsafe.String(unsafe.SliceData(res), len(res))
}
