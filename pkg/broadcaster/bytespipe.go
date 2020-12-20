package broadcaster

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/docker/docker/pkg/ioutils"
)

// Unbuffered accumulates multiple io.WriteCloser by stream.
type BytesPipe struct {
	mu      sync.Mutex
	writers writers
}

// Add adds new io.WriteCloser.
func (w *BytesPipe) Add(writer *ioutils.BytesPipe) {
	w.mu.Lock()
	w.writers.add(writer)
	w.mu.Unlock()
}

// Write writes bytes to all writers. Failed writers will be evicted during
// this call.
func (w *BytesPipe) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	writers := w.writers.get()

	if writers == nil {
		return
	}

	var evict []int
	for i, sw := range *writers {
		if n, err := sw.Write(p); err != nil || n != len(p) {
			// On error, evict the writer
			evict = append(evict, i)
		}
	}

	w.writers.mu.Lock()
	for n, i := range evict {
		*writers = append((*writers)[:i-n], (*writers)[i-n+1:]...)
	}
	w.writers.mu.Unlock()

	return len(p), nil
}

// CleanContext closes and removes all writers.
// CleanContext supports timeouts via the context to unblock and forcefully
// close the io streams. This function should only be used if all writers
// added to Unbuffered support concurrent calls to Close and Write: it will
// call Close while Write may be in progress in order to forcefully close
// writers
func (w *BytesPipe) CleanContext(ctx context.Context) error {
	var cleaningUnderway int32 = 0

	regularCleanDone := make(chan struct{}, 1)

	go func() {
		defer close(regularCleanDone)
		w.mu.Lock()
		defer w.mu.Unlock()
		if !atomic.CompareAndSwapInt32(&cleaningUnderway, 0, 1) {
			return
		}
		w.writers.clean()
	}()
	select {
	case <-regularCleanDone:
		return nil
	case <-ctx.Done():
	}

	if !atomic.CompareAndSwapInt32(&cleaningUnderway, 0, 1) {
		return nil
	}

	w.writers.clean()
	return nil
}

type writers struct {
	mu      sync.Mutex
	writers *[]*ioutils.BytesPipe
}

func (w *writers) add(writer *ioutils.BytesPipe) {
	w.mu.Lock()
	if w.writers == nil {
		w.writers = new([]*ioutils.BytesPipe)
	}
	*w.writers = append(*w.writers, writer)
	w.mu.Unlock()
}

func (w *writers) get() *[]*ioutils.BytesPipe {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writers
}

func (w *writers) clean() {
	w.mu.Lock()
	if w.writers == nil {
		return
	}
	for _, sw := range *w.writers {
		sw.Close()
	}
	w.writers = nil
	w.mu.Unlock()
}
