package core

import (
	"bufio"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func getTestFileName() string {
// 	dir, _ := os.Getwd()
// 	return dir + "/test.db"
// }

// func removeTestFile() {
// 	fileName := getTestFileName()
// 	_, err := os.Stat(fileName)
// 	if err == nil {
// 		os.Remove(fileName)
// 	}
// }

// func TestMain(m *testing.M) {
// 	removeTestFile()
// 	retCode := m.Run()
// 	removeTestFile()
// 	os.Exit(retCode)
// }

func TestReadPage2(t *testing.T) {
	removeTestFile()
	fileName := getTestFileName()
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	w := bufio.NewWriter(f)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(9527))
	bs = append(make([]byte, 4096), bs...)
	_, err = w.Write(bs)
	w.Flush()
	f.Close()
	pager, _ := OpenPager2(fileName)

	bytes, err := pager.ReadPage(1)

	assert.Nil(t, err)
	assert.Equal(t, uint32(9527), binary.LittleEndian.Uint32(bytes[:4]))
	removeTestFile()
}
