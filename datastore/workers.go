package datastore

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type readCall struct {
	offset   int64
	returnCh chan<- readReturn
}

type readReturn struct {
	value string
	err   error
}

type readWorkers struct {
	chans map[*segment]chan<- readCall
}

func newReadWorkers() readWorkers {
	return readWorkers{make(map[*segment]chan<- readCall)}
}

func (rw readWorkers) get(loc recordLocation) (string, error) {
	ch, ok := rw.chans[loc.segment]
	if !ok {
		return "", fmt.Errorf("worker for this segment does not exist: %s", loc.segment.path)
	}

	returnCh := make(chan readReturn)

	ch <- readCall{
		offset:   loc.offset,
		returnCh: returnCh,
	}

	res := <-returnCh
	return res.value, res.err
}

func (rw readWorkers) addWorker(seg *segment) error {
	f, err := os.Open(seg.path)
	if err != nil {
		return err
	}

	ch := make(chan readCall)
	go runFileReader(f, ch)
	rw.chans[seg] = ch
	return nil
}

func runFileReader(f *os.File, ch <-chan readCall) {
	for call := range ch {
		returnCh := call.returnCh

		_, err := f.Seek(call.offset, io.SeekStart)
		if err != nil {
			returnCh <- readReturn{
				err: err,
			}
			continue
		}

		reader := bufio.NewReader(f)
		var e entry
		_, err = e.DecodeFromReader(reader)
		if err != nil {
			returnCh <- readReturn{
				err: err,
			}
			continue
		}

		returnCh <- readReturn{
			value: e.value,
		}
	}

	f.Close()
}

func (rw readWorkers) deleteWorker(seg *segment) {
	ch, ok := rw.chans[seg]
	if ok {
		delete(rw.chans, seg)
		close(ch)
	}
}

func (rw readWorkers) clear() {
	for _, ch := range rw.chans {
		close(ch)
	}
}
