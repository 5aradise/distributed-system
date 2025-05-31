package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	segmentPrefix = "segment-"
)

var (
	SegmentSizeLimit = int64(10 * 1024 * 1024)
)

var ErrNotFound = errors.New("record does not exist")

type hashIndex map[string]recordLocation

type recordLocation struct {
	segment *segment
	offset  int64
}

type Db struct {
	dir           string
	activeSegment activeSegment
	mu            sync.RWMutex
	segments      []*segment
	index         hashIndex
}

func Open(dir string) (*Db, error) {
	db := &Db{
		dir:      dir,
		segments: []*segment{},
		index:    make(hashIndex),
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		name := file.Name()
		if !strings.HasPrefix(name, segmentPrefix) {
			continue
		}

		path := filepath.Join(db.dir, name)
		err := db.recoverSegment(path)
		if err != nil {
			return nil, fmt.Errorf("failed to recover %s: %w", name, err)
		}
	}

	if len(db.segments) > 0 {
		last := db.segments[len(db.segments)-1]
		active, err := last.activate()
		if err != nil {
			return nil, err
		}
		db.activeSegment = active
	} else {
		if err := db.initNextSegment(); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (db *Db) Close() error {
	db.mu.Lock()
	return db.activeSegment.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var loc recordLocation

	offset, ok := db.activeSegment.index[key]
	if ok {
		loc = recordLocation{
			segment: db.activeSegment.segment,
			offset:  offset,
		}
	} else {
		loc, ok = db.index[key]
		if !ok {
			return "", ErrNotFound
		}
	}

	f, err := os.Open(loc.segment.path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	_, err = f.Seek(loc.offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(f)
	var e entry
	_, err = e.DecodeFromReader(reader)
	if err != nil {
		return "", err
	}

	return e.value, nil
}

func (db *Db) Put(key, value string) error {
	db.mu.Lock()

	e := entry{
		key:   key,
		value: value,
	}
	data := e.Encode()

	if db.activeSegment.size+int64(len(data)) > SegmentSizeLimit {
		db.activeSegment.Close()
		if err := db.initNextSegment(); err != nil {
			db.mu.Unlock()
			return err
		}
	}

	n, err := db.activeSegment.Write(data)
	if err != nil {
		db.mu.Unlock()
		return err
	}

	db.activeSegment.index[key] = db.activeSegment.size
	db.activeSegment.size += int64(n)

	if len(db.segments) >= 3 {
		go db.lockMergeSegments()
	} else {
		db.mu.Unlock()
	}

	return nil
}

func (db *Db) Size() (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var total int64
	for _, seg := range db.segments {
		info, err := os.Stat(seg.path)
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}
