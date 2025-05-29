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
	segmentPrefix    = "segment-"
	segmentSizeLimit = 10 * 1024 * 1024
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type segment struct {
	path   string
	offset int64
	index  hashIndex
}

type recordLocation struct {
	segment *segment
	offset  int64
}

type Db struct {
	dir              string
	segments         []*segment
	activeSegment    *segment
	out              *os.File
	index            map[string]recordLocation
	segmentSizeLimit int64

	mu sync.RWMutex
}

func Open(dir string, segmentSizeLimit int64) (*Db, error) {
	db := &Db{
		dir:              dir,
		segments:         []*segment{},
		index:            make(map[string]recordLocation),
		segmentSizeLimit: segmentSizeLimit,
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
		seg, err := db.recoverSegment(path)
		if err != nil {
			return nil, fmt.Errorf("failed to recover %s: %w", name, err)
		}

		db.segments = append(db.segments, seg)
	}

	if len(db.segments) > 0 {
		last := db.segments[len(db.segments)-1]
		f, err := os.OpenFile(last.path, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		db.out = f
		db.activeSegment = last
	} else {
		if err := db.createNewSegment(); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (db *Db) recoverSegment(path string) (*segment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	seg := &segment{
		path:  path,
		index: make(hashIndex),
	}

	reader := bufio.NewReader(f)
	var offset int64

	for {
		pos := offset
		var e entry
		n, err := e.DecodeFromReader(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if n != 0 {
					return nil, fmt.Errorf("corrupted file")
				}
				break
			}
			return nil, err
		}
		seg.index[e.key] = pos
		db.index[e.key] = recordLocation{segment: seg, offset: pos}
		offset += int64(n)
	}

	seg.offset = offset
	return seg, nil
}

func (db *Db) createNewSegment() error {
	id := len(db.segments)
	path := filepath.Join(db.dir, fmt.Sprintf("%s%d", segmentPrefix, id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	seg := &segment{
		path:  path,
		index: make(hashIndex),
	}
	db.segments = append(db.segments, seg)
	db.activeSegment = seg
	db.out = f
	return nil
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	loc, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
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
	defer db.mu.Unlock()

	e := entry{
		key:   key,
		value: value,
	}
	data := e.Encode()

	if db.activeSegment.offset+int64(len(data)) > db.segmentSizeLimit {
		_ = db.out.Close()
		if err := db.createNewSegment(); err != nil {
			return err
		}
	}

	offset := db.activeSegment.offset

	n, err := db.out.Write(data)
	if err != nil {
		return err
	}

	db.activeSegment.offset += int64(n)
	db.activeSegment.index[key] = offset

	db.index[key] = recordLocation{
		segment: db.activeSegment,
		offset:  offset,
	}

	if len(db.segments) >= 3 {
		go db.MergeSegments()
	}

	return nil
}

func (db *Db) Size() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

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

func (db *Db) MergeSegments() {
	db.mu.Lock()
	defer db.mu.Unlock()

	oldSegments := db.segments[:len(db.segments)-1]
	latest := make(map[string]entry)

	for _, seg := range oldSegments {
		if err := readSegmentEntries(seg, latest); err != nil {
			fmt.Printf("merge: failed to read segment %s: %v\n", seg.path, err)
			return
		}
	}

	mergeDir := filepath.Join(db.dir, "merge-tmp")
	if err := os.MkdirAll(mergeDir, 0755); err != nil {
		fmt.Printf("merge: failed to create merge directory: %v\n", err)
		return
	}

	newSegments, err := db.writeMergedSegments(mergeDir, latest)
	if err != nil {
		fmt.Printf("merge: failed to write merged segments: %v\n", err)
		return
	}

	if err := db.replaceOldSegments(oldSegments, newSegments); err != nil {
		fmt.Printf("merge: failed to replace old segments: %v\n", err)
		return
	}

	_ = os.RemoveAll(mergeDir)
}

func readSegmentEntries(seg *segment, latest map[string]entry) error {
	f, err := os.Open(seg.path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var offset int64

	for {
		pos := offset
		var e entry
		n, err := e.DecodeFromReader(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		latest[e.key] = e
		offset = pos + int64(n)
	}

	return nil
}

func (db *Db) writeMergedSegments(dir string, entries map[string]entry) ([]*segment, error) {
	var (
		newSegments []*segment
		offset      int64
		segIndex    int
		currentFile *os.File
		currentSeg  *segment
		err         error
	)

	createSegment := func(index int) (*os.File, *segment, error) {
		path := filepath.Join(dir, fmt.Sprintf("%s%d", segmentPrefix, index))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, nil, err
		}
		return f, &segment{path: path, index: make(hashIndex)}, nil
	}

	currentFile, currentSeg, err = createSegment(segIndex)
	if err != nil {
		return nil, err
	}

	for key, e := range entries {
		data := e.Encode()
		if offset+int64(len(data)) > db.segmentSizeLimit {
			currentFile.Close()
			currentSeg.offset = offset
			newSegments = append(newSegments, currentSeg)

			segIndex++
			currentFile, currentSeg, err = createSegment(segIndex)
			if err != nil {
				return nil, err
			}
			offset = 0
		}

		n, err := currentFile.Write(data)
		if err != nil {
			return nil, err
		}
		currentSeg.index[key] = offset
		offset += int64(n)
	}

	if offset > 0 {
		currentFile.Close()
		currentSeg.offset = offset
		newSegments = append(newSegments, currentSeg)
	}

	return newSegments, nil
}

func (db *Db) replaceOldSegments(oldSegments, newSegments []*segment) error {
	for _, seg := range oldSegments {
		if err := os.Remove(seg.path); err != nil {
			return err
		}
	}

	for _, seg := range newSegments {
		filename := filepath.Base(seg.path)
		newPath := filepath.Join(db.dir, filename)
		if err := os.Rename(seg.path, newPath); err != nil {
			return err
		}
		seg.path = newPath
	}

	newIndex := make(map[string]recordLocation)

	for _, seg := range newSegments {
		for key, offset := range seg.index {
			newIndex[key] = recordLocation{segment: seg, offset: offset}
		}
	}
	for key, offset := range db.activeSegment.index {
		newIndex[key] = recordLocation{segment: db.activeSegment, offset: offset}
	}

	db.segments = append(newSegments, db.activeSegment)
	db.index = newIndex
	return nil
}
