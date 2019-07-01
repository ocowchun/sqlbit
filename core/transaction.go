package core

type transactionPage struct {
	page     *PageBody
	snapshot *Page
}

func (tp *transactionPage) isDirty() bool {
	return tp.snapshot.isDirty
}

type Transaction struct {
	id         int32
	pageTable  map[PageID]*transactionPage
	bufferPool *BufferPool
}

func NewTransaction(id int32, bufferPool *BufferPool) *Transaction {
	return &Transaction{
		id:         id,
		pageTable:  make(map[PageID]*transactionPage),
		bufferPool: bufferPool,
	}
}

// ID returns the transaction id.
func (t *Transaction) ID() int32 {
	return t.id
}

func EmptySnapshotPage() *Page {
	body := emptyPageBody()
	return &Page{
		body:    &body,
		isDirty: false,
	}
}

func (t *Transaction) ReadPage(pageID PageID) (*Page, error) {
	if t.pageTable[pageID] == nil {
		page, err := t.bufferPool.FetchPage(pageID)
		if err != nil {
			return nil, err
		}

		body := emptyPageBody()
		copy(body[:], page[:])
		snapshot := &Page{
			id:      pageID,
			body:    &body,
			isDirty: false,
		}

		t.pageTable[pageID] = &transactionPage{
			page:     page,
			snapshot: snapshot,
		}
	}
	return t.pageTable[pageID].snapshot, nil
}

func (t *Transaction) NewPage() (*Page, error) {
	page, err := t.bufferPool.NewPage()
	pageID := page.id
	body := page.body
	if err != nil {
		return nil, err
	}

	snapshotBody := emptyPageBody()
	copy(snapshotBody[:], body[:])
	snapshot := &Page{
		id:      pageID,
		body:    &snapshotBody,
		isDirty: true,
	}

	t.pageTable[pageID] = &transactionPage{
		page:     body,
		snapshot: snapshot,
	}
	return t.pageTable[pageID].snapshot, nil
}

func (t *Transaction) Commit() {
	for pageID, tp := range t.pageTable {
		if tp.snapshot.isDirty {
			copy(t.pageTable[pageID].page[:], t.pageTable[pageID].snapshot.body[:])
		}
		t.bufferPool.UnpinPage(pageID, tp.isDirty())
	}
}

func (t *Transaction) Rollback() {
	for pageID, _ := range t.pageTable {
		t.bufferPool.UnpinPage(pageID, false)
	}
}
