package core

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync/atomic"
)

type FilePager struct {
	file     *os.File
	numPages int64
}

func NewFilePager(fileName string) (*FilePager, error) {
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		err = createDBFile(fileName)
		if err != nil {
			return nil, err
		}
	}

	f, err := os.OpenFile(fileName, os.O_RDWR, 0644)

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

func createDBFile(fileName string) error {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	//Prepare Table Header
	w := bufio.NewWriter(f)
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(PAGE_TYPE_TABLE_HEADER))
	rootPageNum := uint32(1)
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, rootPageNum)
	bs = append(bs, b...)
	bs = append(bs, make([]byte, 4096-len(bs))...)

	//Prepare Leaf Node
	b = make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(PAGE_TYPE_LEAF_NODE))
	bs = append(bs, b...)
	numTuples := uint32(0)
	b = make([]byte, 4)
	binary.LittleEndian.PutUint32(b, numTuples)
	bs = append(bs, b...)

	b = make([]byte, LEAF_NODE_HEADER_SIZE-(4+2))
	bs = append(bs, b...)

	_, err = w.Write(bs)
	if err != nil {
		return err
	}

	err = w.Flush()
	if err != nil {
		return err
	}

	return f.Close()
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

func (p *FilePager) IncrementPageID() PageID {
	id := atomic.AddInt64(&p.numPages, 1)
	// TODO: avoid id > max uint32
	return PageID(id)
}

func (p *FilePager) Close() error {
	return p.file.Close()
}
