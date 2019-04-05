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
	file  *os.File
	pages []*Page2
	// numPages int64
}

// OpenPager will create a Pager instance to access file given fileName
func OpenPager2(fileName string) (*Pager2, error) {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	pager := &Pager2{
		file:  f,
		pages: []*Page2{},
	}
	return pager, nil
}

func (p *Pager2) ReadPage(pageNum uint32) ([]byte, error) {
	_, err := p.file.Seek(int64(pageNum*PAGE_SIZE), 0)
	if err != nil {
		return nil, err
	}

	if len(p.pages) > int(pageNum) && p.pages[pageNum] != nil {
		return p.pages[pageNum].bytes, nil
	}

	bytes := make([]byte, PAGE_SIZE)
	_, err = p.file.Read(bytes)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return bytes, nil
}
