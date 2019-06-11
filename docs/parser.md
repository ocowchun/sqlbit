> the parser will parse a SQL query to a query plan.

## Query Plan
root node is a projection
it can have a select operator.

For step 1, it'd easier to only implement an primary key select operator (i.e., only allow something like `where id = 1`)


https://www.youtube.com/watch?v=vyVGm_2iFwU&list=PLSE8ODhjZXja3hgmuwhf89qboV1kOxMx7
https://www.cockroachlabs.com/blog/join-ordering-pt1/
https://15445.courses.cs.cmu.edu/fall2018/slides/01-introduction.pdf
https://15445.courses.cs.cmu.edu/fall2018/slides/10-queryprocessing.pdf