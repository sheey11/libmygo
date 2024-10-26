package bitmap

import (
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

func init() {
	if _, set := os.LookupEnv("MYGO_DEBUG"); set {
		log.SetLevel(log.DebugLevel)
	}
}

type BitMap struct {
	store []byte

	// last zero bit for fast seeking
	lastVacant int

	// changing lastVacant needs lock
	vacantMutex sync.Mutex
}

func New(size int) BitMap {
	bytes := size / 8
	if size%8 != 0 {
		bytes += 1
	}

	return BitMap{
		store:      make([]byte, bytes, bytes+3),
		lastVacant: 0,
	}
}

func (m *BitMap) Set(i int, v bool) {
	addr := (*uint32)(unsafe.Pointer(&m.store[int(i/32)*4]))
	mask := bitMask32(i)

	var n uint32
	for {
		old := atomic.LoadUint32(addr)

		if v {
			n = old | mask
		} else {
			n = old & ^mask
		}

		if old == n {
			// already set or unset
			return
		}

		if atomic.CompareAndSwapUint32(addr, old, n) {
			if !v {
				m.vacantMutex.Lock()
				defer m.vacantMutex.Unlock()

				if i < m.lastVacant {
					m.lastVacant = i
				}
			}
			return
		}
	}
}

func (m *BitMap) Get(i int) bool {
	mask := bitMask32(i)
	addr := (*uint32)(unsafe.Pointer(&m.store[int(i/32)*4]))
	v32 := atomic.LoadUint32(addr)
	return v32&mask != 0
}

func (m *BitMap) FindVacantAndSet() int {
	m.vacantMutex.Lock()
	defer m.vacantMutex.Unlock()

	nextVacant := -1
	for i := m.lastVacant / 8; i < len(m.store); i++ {
		if m.store[i] != 0xFF {
			byt := ^m.store[i]
			complement := -byt
			bitwiseIndex := fastLog2(byt & complement)

			nextVacant = i*8 + int(bitwiseIndex)
			break
		}
	}

	m.lastVacant = nextVacant

	// set bit before releasing mutex
	if nextVacant != -1 {
		old := m.store[nextVacant/8]
		n := old | (1 << (nextVacant % 8))
		m.store[nextVacant/8] = n
	}

	return nextVacant
}

func fastLog2(b byte) byte {
	var i byte = 0
	for ; i < 8; i++ {
		if b == 1 {
			return i
		}
		b = b >> 1
	}
	return 255
}
