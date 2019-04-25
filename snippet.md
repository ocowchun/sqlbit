## TODO
- [ ] buffer pool implementation
- [x] load btree from file
- [x] write tuple to btree
- [ ] refactor code, put fileNoder, pager2 their function in right place
- [ ] add sibling pointer to leaf node


insert 1 cstack foo@bar.com
insert 2147483647 ocowchun ocowchun@bar.com

https://gobyexample.com/reading-files
https://gobyexample.com/writing-files

<!-- return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)])) -->

### Buffer Pool
https://15445.courses.cs.cmu.edu/fall2018/project1/
page table will record 
pageId, pageIdxInBufferPool, dirtyFlag, pin

sorted map might be a good choice for Replacer

```go
func (m *BufferPoolManager) evict(pageID uint32){
    pageIdxInBufferPool := m.pageTable.map[pageID]
    // use mutex to ensure no thread can access the page in pageTable
    m.pageTable.free(pageID)
    m.bufferPool.free(pageIdxInBufferPool)
}

// maybe we can let high level component handle the concurrency stuff? i.e. concurrency read write
func (p *PageTable) Read(pageID uint32)  *page {
    // use a readMutex to ensure no thread can modify the page in pageTable, but other thred can read
    pageIdxInBufferPool := p.map[pageID]
    p.bf.Pin(pageIdxInBufferPool)
    page := p.bf.get(pageIdxInBufferPool)
    if page.ID == pageID {
        return page
    } else {

    }
}
```


### Cursor
https://cstack.github.io/db_tutorial/parts/part6.html

Why do we need Cursor?
Cursor will help statements (i.e., insert) to interact with the table entirely through the cursor without assuming anything about how the table is stored.

### B Tree
https://www.youtube.com/watch?v=Z1Qrsm7EfRw&t=2108s
https://www.youtube.com/watch?v=VHSDhMO63ww
https://15445.courses.cs.cmu.edu/fall2018/slides/07-trees1.pdf

#### demo
https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html

According to this video(https://youtu.be/VHSDhMO63ww?t=849), you should not record a pointer to parent node, because it's error prone when concurrency update. 

insert
delete
search

leaf node, internal node
leaf node and internal node is interface
#### common node interface
NodeType() => root, internal, leaf
Keys() => return keys of node
Put()


maybe I can implement a dummy version (no interface), and then extract the common part.
[1, 5, 7, 11, 17]
1,5,7
11,17


L 分割成 Ｌ, L2, 把 middle key 丟到 parent node


cursor

pager

btree

capacity of internal node and leaf node are quite different!

define internal node and leaf node format


table meta data
rootNode pageNum

### page format
#### Table Header
PAGE_TYPE(2 bytes), ROOT_PAGE_NUM(4 bytes)

#### Internal Node
PAGE_TYPE(2 bytes), NUM_KEYS(4 bytes), Child1(4 bytes), Key1(4 bytes), Child2(4 bytes),...

#### Leaf Node
PAGE_TYPE(2 bytes), Child1(291 bytes), Child2(291 bytes)...

### load btree from file
first page is table header, which will record the pageNum to rootPage

### write tuple to btree
use Cursor to encapsulated the btree details
Table <=> Cursor <=> Btree

Add a new method `Write(row *Row)` to Cursor

#### flush Btree node to file
when? db close?


how to create init db file?
