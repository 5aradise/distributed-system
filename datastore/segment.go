package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type (
	segment struct {
		path string
	}

	activeSegment struct {
		*segment
		io.WriteCloser
		index map[string]int64
		size  int64
	}
)

func (seg *segment) rename(name string) error {
	dir := filepath.Dir(seg.path)
	newPath := filepath.Join(dir, segmentPrefix+name)

	err := os.Rename(seg.path, newPath)
	if err != nil {
		return err
	}

	seg.path = newPath
	return nil
}

func (seg *segment) activate() (activeSegment, error) {
	f, err := os.OpenFile(seg.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return activeSegment{}, err
	}

	return activeSegment{
		segment:     seg,
		WriteCloser: f,
		index:       make(map[string]int64),
		size:        0,
	}, nil
}

func (db *Db) newSegment(name string) (activeSegment, error) {
	path := filepath.Join(db.dir, segmentPrefix+name)

	seg := &segment{
		path: path,
	}

	return seg.activate()
}

func (db *Db) recoverSegment(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	seg := &segment{
		path: path,
	}
	db.segments = append(db.segments, seg)

	reader := bufio.NewReader(f)
	var offset int64
	for {
		pos := offset
		var e entry
		n, err := e.DecodeFromReader(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if n != 0 {
					return fmt.Errorf("corrupted file")
				}
				break
			}
			return err
		}
		db.index[e.key] = recordLocation{segment: seg, offset: pos}
		offset += int64(n)
	}
	db.activeSegment.size = offset

	return nil
}

func (db *Db) initNextSegment() error {
	active, err := db.newSegment(strconv.FormatInt(time.Now().UnixNano(), 10))
	if err != nil {
		return err
	}

	for key, offset := range db.activeSegment.index {
		db.index[key] = recordLocation{
			segment: db.activeSegment.segment,
			offset:  offset,
		}
	}

	db.segments = append(db.segments, active.segment)
	db.activeSegment = active

	return nil
}

func (db *Db) MergeSegments() {
	db.mu.Lock()
	db.lockMergeSegments()
}

func (db *Db) lockMergeSegments() {
	defer db.mu.Unlock()

	oldSegments := db.segments[:len(db.segments)-1]

	mergedSeg, err := db.newSegment(strconv.FormatInt(time.Now().UnixNano(), 10))
	if err != nil {
		fmt.Printf("MergeSegments: failed to create merged segment: %v\n", err)
		return
	}
	defer func() {
		if err := mergedSeg.Close(); err != nil {
			fmt.Printf("MergeSegments: failed to close merged segment: %v\n", err)
		}
	}()

	newIndex := make(map[string]recordLocation, len(db.index))

	for _, seg := range oldSegments {
		err := db.copyActualData(&mergedSeg, seg, newIndex)
		if err != nil {
			fmt.Printf("MergeSegments: failed to copy actual data from segment %s: %v\n", seg.path, err)
			if err := os.Remove(mergedSeg.path); err != nil {
				fmt.Printf("MergeSegments: failed to remove merged segment: %v\n", err)
			}
			return
		}
	}

	for _, seg := range oldSegments {
		if err := os.Remove(seg.path); err != nil {
			fmt.Printf("MergeSegments: failed to delete old segments: %v\n", err)
		}
	}

	err = mergedSeg.rename("0")
	if err != nil {
		fmt.Printf("MergeSegments: failed to rename merged segments: %v\n", err)
	}

	copy(db.segments, []*segment{mergedSeg.segment, db.segments[len(db.segments)-1]})
	db.segments = db.segments[:2]
	db.index = newIndex
}

func (db *Db) copyActualData(dst *activeSegment, src *segment, newIndex map[string]recordLocation) error {
	f, err := os.Open(src.path)
	if err != nil {
		return fmt.Errorf("failed to open old segment: %w", err)
	}
	defer f.Close()

	var readOffset int64
	reader := bufio.NewReader(f)

	for {
		var e entry
		readed, err := e.DecodeFromReader(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read from old segment: %w", err)
		}

		oldLoc := recordLocation{
			segment: src,
			offset:  readOffset,
		}
		readOffset += int64(readed)

		_, inActive := db.activeSegment.index[e.key]
		if inActive || db.index[e.key] != oldLoc {
			continue
		}
		data := e.Encode()

		writed, err := dst.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write to merged segment: %w", err)
		}

		newIndex[e.key] = recordLocation{
			segment: dst.segment,
			offset:  dst.size,
		}
		dst.size += int64(writed)
	}

	return nil
}
