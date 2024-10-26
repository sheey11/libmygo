package bitmap

import (
	"encoding/binary"
	"unsafe"
)

var nativeEndian binary.ByteOrder

func init() {
	// https://stackoverflow.com/questions/51332658/any-better-way-to-check-endianness-in-go
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		nativeEndian = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		nativeEndian = binary.BigEndian
	default:
		panic("Could not determine native endianness.")
	}
}

func bitMask32(i int) uint32 {
	if nativeEndian == binary.BigEndian {
		return uint32(1 << byte(i%32))
	} else {
		bitOff := i % 8
		bytOff := (i % 32) / 8
		return uint32(1 << byte(bytOff*8+bitOff))
	}
}
