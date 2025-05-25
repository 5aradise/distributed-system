package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func deterministic(t *testing.T) {
	assert := assert.New(t)

	for _, input := range []string{
		"Mazur", "is", "the", "best", "teacher", "!",
	} {
		expected := hash(input)
		for range 100 {
			actual := hash(input)
			assert.Equal(expected, actual)
		}
	}
}

func uniqueness(t *testing.T) {
	assert := assert.New(t)

	generated := make(map[uint64]struct{})
	var collisions int
	inputs := []string{
		"Mazur", "is", "the", "best", "teacher", "!",
		"", "8e3c1a2f9dcbf7a3b6d4c12e", "7b4f89a", "ac", "d1a4c3e8f79b",
		"2fc7a81e6d3b9c0d7f1a5e3bb6d4f2e3c8a1", "9c0abf", "a",
		"60e3d7a2b49f", "94bd1c7e3f", "17c2d3e8b9fa", "c8f37d90be", "ba70f9c13",
		"31c7e2b4f23dwwsed9ad", "c4e1aer5yf79d3a8b", "e7c4b1erh4q590d3fa", "6d9fb2c8e7312gq5", "7ea3c2bd9",
		"3b9f17e4", "d431c8f2a", "2ed9a3f70b", "bf7d12c4e9a", "f1c83a97d04b",
		"eaf90c3b71d", "14df9e07ca", "72cb90f4d", "dc1f432", "3ea174c",
		"09bf1e4d3c2a87ce4a90bd136b3f20e9a74dcb1e9d3a740fb92cf4d3a7e1",
		"e31d9bc4a70f", "a0d3cbf2917e", "cd4a92b7f130", "2b7d41afc9e0", "f8d0a93c4be1",
		"ac7e4f1309db", "d1f3b89e207c", "b4e9a73cd012", "90f27c1e8d3b", "cf3a091be4d7",
		"7e2fdc1a30b9", "e39dc4b1f0a7", "14b9d2f03ace", "f9e0c72d1b4a", "2d0a3bf8e19c",
	}
	for _, input := range inputs {
		res := hash(input)
		_, ok := generated[res]
		if ok {
			collisions++
		}
		generated[res] = struct{}{}
	}

	assert.LessOrEqual(collisions, int(math.Log(float64(len(inputs)))))
}

func TestHash(t *testing.T) {
	t.Run("Deterministic", deterministic)

	t.Run("Uniqueness", uniqueness)
}
