package broton

import (
	"encoding/binary"
	"math"
	"unsafe"
)

func Float64ToBytes(n float64) []byte {
	b := make([]byte, 8)
	bits := math.Float64bits(n)
	binary.BigEndian.PutUint64(b, bits)
	return b
}

func BytesToFloat64(data []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(data))
}

func Uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

func BytesToUint64(data []byte) uint64 {
	return uint64(binary.BigEndian.Uint64(data))
}

func Int64ToBytes(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

func BytesToInt64(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data))
}

func StrToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}
