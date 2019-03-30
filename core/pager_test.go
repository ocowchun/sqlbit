package core

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getTestFileName() string {
	dir, _ := os.Getwd()
	return dir + "/test.db"
}

func removeTestFile() {
	fileName := getTestFileName()
	_, err := os.Stat(fileName)
	if err == nil {
		os.Remove(fileName)
	}
}

func TestMain(m *testing.M) {
	removeTestFile()
	retCode := m.Run()
	removeTestFile()
	os.Exit(retCode)
}

func fakeRow() *Row {
	id := uint32(2147483647)
	username := "harry"
	email := "harry@hogwarts.edu"
	return NewRow(id, username, email)
}

func TestReadPage(t *testing.T) {
	fileName := getTestFileName()
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	w := bufio.NewWriter(f)
	row := fakeRow()
	b := row.Bytes()
	_, err = w.Write(b)
	w.Flush()
	f.Close()
	pager, _ := OpenPager(fileName)

	page, err := pager.ReadPage(0)

	assert.Nil(t, err)
	fmt.Println(page.rows[0])
	assert.Equal(t, row.Id(), page.Rows()[0].Id())
	assert.Equal(t, row.Username(), page.Rows()[0].Username())
	assert.Equal(t, row.Email(), page.Rows()[0].Email())
}

func TestFlushPage(t *testing.T) {
	fileName := getTestFileName()
	pager, _ := OpenPager(fileName)
	page, err := pager.ReadPage(0)
	row := fakeRow()
	page.InsertRow(0, row)

	err = pager.FlushPage(0)

	assert.Nil(t, err)
	dat, err := ioutil.ReadFile(fileName)
	assert.Equal(t, dat, row.Bytes())
}
