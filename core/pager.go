package core

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
)

// The Pager access the page cache and file.
type Pager struct {
	file  *os.File
	pages [TABLE_MAX_PAGES]*Page
}

// OpenPager will create a Pager instance to access file given fileName
func OpenPager(fileName string) (*Pager, error) {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	return &Pager{file: f}, nil
}

func (p *Pager) FileSize() (int64, error) {
	fi, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (p *Pager) ReadPage(pageNum int) (*Page, error) {
	if pageNum > TABLE_MAX_PAGES {
		return nil, errors.New("tried to fetch page number out of bounds")
	}

	if p.pages[pageNum] != nil {
		return p.pages[pageNum], nil
	}

	// Read page from file
	_, err := p.file.Seek(int64(pageNum*PAGE_SIZE), 0)
	if err != nil {
		return nil, err
	}
	bytes := make([]byte, PAGE_SIZE)
	_, err = p.file.Read(bytes)
	if err != nil && err != io.EOF {
		return nil, err
	}

	page, err := deserializePage(bytes), nil
	if err != nil && err != io.EOF {
		return nil, err
	}
	p.pages[pageNum] = page
	return page, nil

}

func deserializePage(bytes []byte) *Page {
	page := &Page{}
	for i := 0; i < ROW_PER_PAGE; i++ {
		from := i * ROW_SIZE
		bs := bytes[from : from+ROW_SIZE]
		if len(bs) == 0 {
			break
		}
		id := binary.LittleEndian.Uint32(bs)
		replacer := strings.NewReplacer("\x00", "")
		usrename := replacer.Replace(string(bs[4:COLUMN_USERNAME_LENGTH]))
		email := replacer.Replace(string(bs[36:COLUMN_EMAIL_LENGTH]))

		page.rows[i] = NewRow(id, usrename, email)
	}
	return page
}

func (p *Pager) FlushPage(pageNum int) error {
	if p.pages[pageNum] == nil {
		return errors.New("Tried to flush null page")
	}

	_, err := p.file.Seek(int64(pageNum*PAGE_SIZE), 0)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(p.file)
	page := p.pages[pageNum]
	bs := page.Bytes()
	_, err = w.Write(bs)
	if err != nil {
		return err
	}
	w.Flush()

	return nil
}
