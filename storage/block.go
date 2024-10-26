package storage

import (
	"errors"
	"fmt"
	"os"
	"sheey/libmygo/bitmap"
	"sync"
	"sync/atomic"
	"time"
)

// Key of the MyGO storage system
type MyGOKey uint32

// Value of the MyGO storage system, up to 1024 bytes
type MyGOValue []byte

var BlockSize = 2 ^ 20
var cacheSize = 128
var bufferSize = 32

// Block stores 2^20 key-values, occupying
// 2^20 * 1KB + 2^20 / 8B = 1GB + 128KB on the disk. up to
// 2^12 of this block is needed to store entire 2^32 key
// space.
type Block struct {
	// bitmap for this block, size = 128KB
	bitmap bitmap.BitMap

	// vacancy in this block.
	vacancy uint32

	// cache of this block, LRU or FIFO? use map for now. only
	// read cache will be stored here.
	cache map[MyGOKey]MyGOValue

	// start position of this block in the bigfile in bytes.
	offset uint32

	// pending changes to be write into filesystem, a map with
	// in-block index as key and actual data as value.
	pendingChanges map[uint32]MyGOValue

	// mutex for pendingChanges buffer
	mutex sync.Mutex

	// signal for flushing, it is a channel of channle pointer
	// that closes if the flush is done.
	flushSignal chan *chan struct{}

	// the pointer(descriptor) to the storage file, here since
	// golang is not safe to fork it self(multiple goroutines
	// running), so the file will be opened multiple times, each
	// block holds different file pointer.
	file *os.File
}

func NewBlock(file *os.File, offset uint32) *Block {
	b := &Block{
		bitmap:         bitmap.New(1 << 20),
		vacancy:        1 << 20,
		cache:          make(map[MyGOKey]MyGOValue, cacheSize),
		offset:         offset,
		pendingChanges: make(map[uint32]MyGOValue, bufferSize),
		flushSignal:    make(chan *chan struct{}, 4),
		mutex:          sync.Mutex{},
		file:           file,
	}

	go b.flushWorker()
	return b
}

// goroutine to wirte pendingChanges to filesystem if flush signal
// arrives or timeout. A flush signal will also be sent via
// channel when the pendingChanges is full.
func (b *Block) flushWorker() {
	flush := func() {
		if len(b.pendingChanges) == 0 {
			return
		}

		b.mutex.Lock()
		for blockIdx, v := range b.pendingChanges {
			_, _ = b.file.WriteAt(v, int64(b.offset+blockIdx*1024))
			delete(b.pendingChanges, blockIdx)
		}
		b.mutex.Unlock()
	}

	// wait 100ms or flush signal arrives
	for {
		select {
		case done := <-b.flushSignal:
			flush()
			if done != nil {
				close(*done)
			}
		case <-time.After(100):
			flush()
		}
	}
}

// Flushes to the filesystem asychronosly.
func (b *Block) Flush() {
	b.flushSignal <- nil
}

func (b *Block) FlushSync() {
	waiter := make(chan struct{})
	b.flushSignal <- &waiter

	// wait until the channel closes
	<-waiter
}

// Is the block still have room for storaging.
func (b *Block) Vacant() bool {
	return b.vacancy != 0
}

// adjust vacancy by `diff` parameter atomically.
func (b *Block) adjustVacancy(diff int) bool {
	for {
		old := b.vacancy
		nxt := int64(old) + int64(diff)
		if nxt < 0 { // prevent underflow
			return false
		}
		nxtU32 := uint32(nxt)

		if atomic.CompareAndSwapUint32(&b.vacancy, old, nxtU32) {
			return true
		}
	}
}

// Put data to this block, returns success and the offset in
// this block if there is still a room.
func (b *Block) Put(data MyGOValue) (error, uint32) {
	// flush buffer before write if buffer is full.
	if len(b.pendingChanges) == bufferSize {
		b.FlushSync()
	}

	if b.vacancy == 0 {
		return errors.New("No vacancy remaining"), 0
	}

	if !b.adjustVacancy(-1) {
		return errors.New("No vacancy remaining"), 0
	}
	offset := uint32(b.bitmap.FindVacantAndSet())

	b.mutex.Lock()
	b.pendingChanges[offset] = data
	b.mutex.Unlock()

	return nil, offset
}

// Retrive MyGOValue from block.
func (b *Block) Get(inBlockIndex uint32) MyGOValue {
	b.mutex.Lock()
	value, exist := b.pendingChanges[inBlockIndex]
	b.mutex.Unlock()

	if exist {
		return value
	}

	buffer := make([]byte, 1024)
	n, err := b.file.ReadAt(buffer, int64(b.offset+inBlockIndex*1024))

	if n != 1024 {
		panic(fmt.Sprintf(
			"seems the reader do not read a full MyGOValue = 1024 bytes, block offset: %x, in-block index: %d, reads: %d bytes, error: %v",
			b.offset,
			inBlockIndex,
			n,
			err,
		))
	}

	return buffer
}

// Delete
func (b *Block) Delete(inBlockIndex uint32) {
	b.mutex.Lock()
	// delete the element is necessary, because the iteration
	// on map is un-ordered, if there's a Put() that be put on
	// the same index, it is posible that flushWorker overwrites
	// the nower data with this not deleted data.
	// delete() do nothing if there's no such key in map.
	delete(b.pendingChanges, inBlockIndex)
	b.mutex.Unlock()

	b.bitmap.Set(int(inBlockIndex), false)
	b.adjustVacancy(1)
}

func (b *Block) Shutdown() {
	b.FlushSync()
	close(b.flushSignal)

	b.file.Close()
}
