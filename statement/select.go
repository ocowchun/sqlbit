package statement

import (
	"fmt"

	"github.com/ocowchun/sqlbit/core"
)

func ExecuteSelect(s Statement, table *core.Table) ExecuteResult {
	rows, err := table.Select()
	if err != nil {
		return ExecuteResult_Failure
	}
	for _, row := range rows {
		fmt.Println(row)
	}
	return ExecuteResult_Success
}
