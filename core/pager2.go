package core

import (
	"io"
	"os"
)

type Page2 struct {
	bytes []byte
}

// The Pager2 access the page cache and file.
type Pager2 struct {
	file     *os.File
	numPages int64
}

// OpenPager will create a Pager instance to access file given fileName
func OpenPager2(fileName string) (*Pager2, error) {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	numPages := fi.Size() / PAGE_SIZE

	pager := &Pager2{
		file:     f,
		numPages: numPages,
	}
	return pager, nil
}

func (p *Pager2) ReadPage(pageNum uint32) ([]byte, error) {
	_, err := p.file.Seek(int64(pageNum*PAGE_SIZE), 0)
	if err != nil {
		return nil, err
	}

	bytes := make([]byte, PAGE_SIZE)
	_, err = p.file.Read(bytes)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return bytes, nil
}

func (p *Pager2) IncrementPageNum() int64 {
	p.numPages = p.numPages + 1
	return p.numPages
}
