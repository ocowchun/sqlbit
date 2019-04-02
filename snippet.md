insert 1 cstack foo@bar.com
insert 2147483647 ocowchun ocowchun@bar.com

https://gobyexample.com/reading-files
https://gobyexample.com/writing-files

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
