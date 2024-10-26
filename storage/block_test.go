package storage

import (
	"os"
	"sync"
	"testing"
)

var tempFilePath = "/tmp/libmygo-block-test-file"

func generateData() MyGOValue {
	buffer := make([]byte, 1024)
	file, _ := os.Open("/dev/random")

	file.Read(buffer)
	return buffer
}

func TestBlock(t *testing.T) {
	file, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_RDWR, 0600)

	// pre allocate file
	file.Truncate(1<<30 + 1<<17)

	if err != nil {
		t.Errorf("failed to create file at %s, error: %v", tempFilePath, err)
		return
	}

	block := NewBlock(file, 0)
	defer block.Shutdown()
	data := generateData()

	err, idx := block.Put(data)
	if err != nil {
		t.Errorf("failed to write data: %v", err)
		return
	}

	block.FlushSync()

	readData := block.Get(idx)
	if len(readData) != 1024 {
		t.Errorf("expected read data to be 1024 long, but got %d", len(readData))
		return
	}

	for i := 0; i < 1024; i++ {
		if data[i] != readData[i] {
			t.Errorf("read data is not same with generated data at byte position %d, read data: %x, generated data: %x.", i, readData[i], data[i])
			return
		}
	}

	wg := sync.WaitGroup{}
	testReadWrite := func(i int) {
		defer wg.Done()

		data := generateData()
		err, idx := block.Put(data)
		if err != nil {
			t.Errorf("failed to write %d-th data, reason: %v", i, err)
			return
		}
		if i%2 == 0 {
			block.FlushSync()
		}

		readData := block.Get(idx)

		for i := 0; i < 1024; i++ {
			if data[i] != readData[i] {
				t.Errorf("read data is not same with generated data at byte position %d, read data: %x, generated data: %x, stored index: %d.", i, readData[i], data[i], idx)
				return
			}
		}
	}

	nTest := 100
	for i := 0; i < nTest; i++ {
		wg.Add(1)
		go testReadWrite(i)
	}

	wg.Wait()
}
