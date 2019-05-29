package statement

import (
	"fmt"

	"github.com/ocowchun/sqlbit/core"
	"github.com/ocowchun/sqlbit/parser"
)

func PrepareSelect(text string) (Statement, error) {
	plan, err := parser.Parse(text)
	if err != nil {
		return Statement{}, err
	}
	return Statement{
		Type:      StatementType_Select,
		QueryPlan: plan,
	}, nil
}

func ExecuteSelect(s Statement, table *core.Table) ExecuteResult {
	var rows []*core.Row
	var err error
	// filter tuples
	whereExpression := s.QueryPlan.From.Where
	if whereExpression == nil {
		rows, err = table.SeqScan(nil)
		if err != nil {
			return ExecuteResult_Failure
		}
	} else {
		filter, err := core.NewFilter(whereExpression, table.Schema())
		if err != nil {
			return ExecuteResult_Failure
		}

		rows, err = table.SeqScan(filter)
		if err != nil {
			return ExecuteResult_Failure
		}
	}

	for _, row := range rows {
		fmt.Println(row)
	}

	return ExecuteResult_Success
	// select necessary attributes
}

// func ExecuteSelect(s Statement, table *core.Table) ExecuteResult {
// 	rows, err := table.Select()
// 	if err != nil {
// 		return ExecuteResult_Failure
// 	}
// 	for _, row := range rows {
// 		fmt.Println(row)
// 	}
// 	return ExecuteResult_Success
// }
