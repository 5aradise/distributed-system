package main

import (
	"crypto/sha256"
	"encoding/binary"
)

func hash(s string) uint64 {
	data := sha256.Sum256([]byte(s))
	i := len(s) % len(data)
	var generated [8]byte
	n := copy(generated[:], data[i:])
	copy(generated[n:], data[:])
	return binary.LittleEndian.Uint64(generated[:])
}
