package statement

import (
	"github.com/ocowchun/sqlbit/core"
	"github.com/ocowchun/sqlbit/parser"
)

type StatementType int

const (
	StatementType_Insert StatementType = iota
	StatementType_Select
	StatementType_Delete
)

type Statement struct {
	Type        StatementType
	RowToInsert *core.Row
	QueryPlan   *parser.Select
}

type ExecuteResult int

const (
	ExecuteResult_Success ExecuteResult = iota
	ExecuteResult_Failure
	ExecuteResult_TableFull
)
