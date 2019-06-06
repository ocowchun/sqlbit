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

type ScanMethodType int

const (
	ScanMethodType_SeqScan ScanMethodType = iota
	ScanMethodType_IndexScan
)

type QueryPlan struct {
	ScanMethod     ScanMethodType
	Filter         core.Filter
	IndexCondition *core.IndexCondition
}

func OptimizeQueryPlan(s Statement, table *core.Table) (*QueryPlan, error) {
	whereExpression := s.QueryPlan.From.Where
	if whereExpression == nil {
		return &QueryPlan{
			ScanMethod: ScanMethodType_SeqScan,
		}, nil
	} else {
		filter, err := core.NewFilter(whereExpression, table.Schema())
		if err != nil {
			return nil, err
		}

		operator := whereExpression.Condition.Compare.Operator
		if whereExpression.Condition.LHS == "id" && (operator != "!=" && operator != "<>") {
			indexCondition := &core.IndexCondition{
				ColumnName: whereExpression.Condition.LHS,
				Target:     uint32(*whereExpression.Condition.Value.Number),
				Operator:   operator,
			}
			return &QueryPlan{
				ScanMethod:     ScanMethodType_IndexScan,
				IndexCondition: indexCondition,
			}, nil
		} else {
			return &QueryPlan{
				ScanMethod: ScanMethodType_SeqScan,
				Filter:     filter,
			}, nil
		}
	}
}

func ExecuteSelect(s Statement, table *core.Table) ExecuteResult {
	var rows []*core.Row
	var err error

	queryPlan, err := OptimizeQueryPlan(s, table)
	if err != nil {
		return ExecuteResult_Failure
	}

	if queryPlan.ScanMethod == ScanMethodType_IndexScan {
		rows, err = table.IndexScan(queryPlan.IndexCondition, queryPlan.Filter)
		if err != nil {
			return ExecuteResult_Failure
		}
	} else {
		rows, err = table.SeqScan(queryPlan.Filter)
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
