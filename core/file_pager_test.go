package core

import (
	"bufio"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilePagerRead(t *testing.T) {
	removeTestFile()
	fileName := getTestFileName()
	f, _ := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	w := bufio.NewWriter(f)
	expectedPage := createPageFromSlice([]byte{1, 2, 3, 4, 5})
	bs := append(make([]byte, 4096), expectedPage[:]...)
	w.Write(bs)
	w.Flush()
	f.Close()
	pager, _ := NewFilePager(fileName)

	newPage := emptyPage()
	err := pager.Read(int64(4096), &newPage)

	assert.Nil(t, err)
	assert.Equal(t, expectedPage, newPage)
}

func TestFilePagerWrite(t *testing.T) {
	removeTestFile()
	fileName := getTestFileName()
	pager, _ := NewFilePager(fileName)
	offset := int64(0)
	expectedPage := createPageFromSlice([]byte{1, 2, 3, 4, 5})

	err := pager.Write(offset, &expectedPage)

	assert.Nil(t, err)
	pager.Close()
	f, _ := os.Open(fileName)
	f.Seek(offset, 0)
	bs := make([]byte, PAGE_SIZE)
	f.Read(bs)
	assert.Equal(t, expectedPage[:], bs)
	removeTestFile()
}

func TestFilePagerIncrementPageID(t *testing.T) {
	removeTestFile()
	fileName := getTestFileName()
	f, _ := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	w := bufio.NewWriter(f)
	expectedPage := createPageFromSlice([]byte{1, 2, 3, 4, 5})
	bs := append(make([]byte, 4096), expectedPage[:]...)
	w.Write(bs)
	w.Flush()
	f.Close()
	pager, _ := NewFilePager(fileName)

	pageID := pager.IncrementPageID()

	assert.Equal(t, uint32(3), pageID)
}
