// Implementation of mygo storage server
package mygo

import "sheey/libmygo/bitmap"

type MyGO struct {
	// number of blocks allocated (occupied + vacancy)
	blocks uint32
	// number of blocks vacancy
	vacancyBlocks uint32

	// an array of bitmaps, each bitmap stores 2^24 bits, and
	// comsumes 2M memory, to store total 2^32 keys, up to 256
	// bitmaps needed, though I guess there's no need to allocate
	// that much at once since the use of all the 2^32 keys
	// needs total of 4 Terabytes storage.
	bitmaps []bitmap.BitMap

	
}

