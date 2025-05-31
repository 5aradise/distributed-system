package datastore

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestDb(t *testing.T) {
	tmp := t.TempDir()
	SegmentSizeLimit = 1024
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][2]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k2", "v2.1"},
		{"k4", "v4"},
		{"k5", "v5"},
		{"k6", "v6"},
		{"k5", "v5.1"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs[:4] {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("file growth", func(t *testing.T) {
		sizeBefore, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs[4:] {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		sizeAfter, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		if sizeAfter <= sizeBefore {
			t.Errorf("Size does not grow after put (before %d, after %d)", sizeBefore, sizeAfter)
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(tmp)
		if err != nil {
			t.Fatal(err)
		}

		uniquePairs := make(map[string]string)
		for _, pair := range pairs {
			uniquePairs[pair[0]] = pair[1]
		}

		for key, expectedValue := range uniquePairs {
			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if value != expectedValue {
				t.Errorf("Get(%q) = %q, wanted %q", key, value, expectedValue)
			}
		}
	})
}

func TestMergeSegments(t *testing.T) {
	tmp := t.TempDir()
	SegmentSizeLimit = 12
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := range 10 {
		key := fmt.Sprintf("key%d", i%3)
		value := fmt.Sprintf("value%d", i)
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	expected := map[string]string{
		"key0": "value9",
		"key1": "value7",
		"key2": "value8",
	}

	for k, v := range expected {
		got, err := db.Get(k)
		if err != nil {
			t.Errorf("Get(%q) failed: %v", k, err)
			continue
		}
		if got != v {
			t.Errorf("Get(%q) = %q; want %q", k, got, v)
		}
	}

	files, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}

	segmentCount := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), segmentPrefix) {
			segmentCount++
		}
	}

	if segmentCount < 2 {
		t.Errorf("Expected multiple segments, got %d", segmentCount)
	}
}
