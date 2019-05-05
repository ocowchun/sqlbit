package core

import (
	"bufio"
	"os"
	"sync/atomic"
)

type FilePager struct {
	file     *os.File
	numPages int64
}

func NewFilePager(fileName string) (*FilePager, error) {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	numPages := fi.Size() / PAGE_SIZE

	pager := &FilePager{
		file:     f,
		numPages: numPages,
	}
	return pager, nil
}

func (p *FilePager) Read(offset int64, bs *PageBody) error {
	_, err := p.file.Seek(offset, 0)
	if err != nil {
		return err
	}

	_, err = p.file.Read(bs[:])
	return err
}

func (p *FilePager) Write(offset int64, bs *PageBody) error {
	_, err := p.file.Seek(offset, 0)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(p.file)
	_, err = w.Write(bs[:])
	if err != nil {
		return err
	}
	return w.Flush()
}

func (p *FilePager) IncrementPageID() uint32 {
	id := atomic.AddInt64(&p.numPages, 1)
	// TODO: avoid id > max uint32
	return uint32(id)
}

func (p *FilePager) Close() error {
	return p.file.Close()
}
