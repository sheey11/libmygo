package bitmap

import (
	"testing"
)

func TestBitMapGetSet(t *testing.T) {
	bitmap := New(511)

	for i := 0; i < 512; i++ {
		if bitmap.Get(i) {
			t.Errorf("bit %d should not be set initially", i)
		}
	}

	for i := 0; i < 128; i++ {
		// set 0, 2, 4, ..., 254 bits
		bitmap.Set(i*2, true)
	}

	for i := 0; i < 512; i++ {
		v := bitmap.Get(i)
		if i < 256 && i%2 == 0 && !v {
			t.Errorf("bit %d should be set but it isn't", i)
		} else if i < 256 && i%2 == 1 && v {
			t.Errorf("bit %d should not be set but it is set", i)
		} else if i >= 256 && v {
			t.Errorf("bit %d should not be set but it is set", i)
		}
	}
}

func TestFastLog2(t *testing.T) {
	testCases := [][]byte{
		{byte(1) << 0, 0},
		{byte(1) << 1, 1},
		{byte(1) << 2, 2},
		{byte(1) << 3, 3},
		{byte(1) << 4, 4},
		{byte(1) << 5, 5},
		{byte(1) << 6, 6},
		{byte(1) << 7, 7},
		{0, 255},
	}

	for _, testCase := range testCases {
		byt, idx := testCase[0], testCase[1]
		result := fastLog2(byt)
		if idx != result {
			t.Errorf("fastLog2(%d) expected to be %d, but got %d", byt, idx, result)
			return
		}
	}
}

func TestFindVacant(t *testing.T) {
	m := New(32)

	m.Set(1, true)
	m.Set(2, true)
	m.Set(7, true)
	m.Set(31, true)

	vacant := m.FindVacantAndSet()
	expect := 0
	if vacant != expect {
		t.Errorf("expect vacant index to be %d, but got %d", expect, vacant)
	}
	if m.Get(vacant) == false {
		t.Errorf("expected %d-th bit to be set, but not set", expect)
	}

	vacant = m.FindVacantAndSet()
	expect = 3
	if vacant != expect {
		t.Errorf("expect vacant index to be %d, but got %d", expect, vacant)
	}
	if m.Get(vacant) == false {
		t.Errorf("expected %d-th bit to be set, but not set", expect)
	}

	// set 4-6, i.e. first byte
	for i := 4; i < 7; i++ {
		m.Set(i, true)
	}
	m.Set(9, true)

	vacant = m.FindVacantAndSet()
	expect = 8
	if vacant != expect {
		t.Errorf("expect vacant index to be %d, but got %d", expect, vacant)
	}
	if m.Get(vacant) == false {
		t.Errorf("expected %d-th bit to be set, but not set", expect)
	}

	for i := 10; i < 31; i++ {
		m.Set(i, true)
	}

	// test if all bits are set
	vacant = m.FindVacantAndSet()
	expect = -1
	if vacant != expect {
		t.Errorf("expect vacant index to be %d, but got %d", expect, vacant)
	}
}
