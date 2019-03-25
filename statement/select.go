package statement

import (
	"fmt"

	"github.com/ocowchun/sqlbit/core"
)

func ExecuteSelect(s Statement, table *core.Table) ExecuteResult {
	numRows := table.NumRows()
	for i := 0; i < numRows; i++ {
		pageIdx := i / core.ROW_PER_PAGE
		rowIdx := i - pageIdx*core.ROW_PER_PAGE
		row := table.Pages()[pageIdx].Rows()[rowIdx]
		fmt.Println(row)
	}
	return ExecuteResult_Success
}
