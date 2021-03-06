package core

import (
	"errors"
	"fmt"

	"github.com/ocowchun/sqlbit/parser"
)

type Filter interface {
	Test(row *Row) (bool, error)
}

func GetValueFromRow(row *Row, columnName string) (interface{}, error) {
	switch columnName {
	case "id":
		return row.Id(), nil
	case "username":
		return row.Username(), nil
	case "email":
		return row.Email(), nil
	default:
		message := fmt.Sprintf("column \"%s\" does not exist", columnName)
		return nil, errors.New(message)
	}
}

type Uint32Filter struct {
	columnName string
	target     uint32
	operator   string
}

func NewUint32Filter(columnName string, target uint32, operator string) (*Uint32Filter, error) {
	supportedOperators := []string{"<>", "<=", ">=", "=", "<", ">", "!="}
	isSupportedOperator := false
	for _, supportedOperator := range supportedOperators {
		if operator == supportedOperator {
			isSupportedOperator = true
		}
	}

	if isSupportedOperator {
		return &Uint32Filter{
			columnName: columnName,
			target:     target,
			operator:   operator,
		}, nil
	} else {
		message := fmt.Sprintf("invalid input syntax for uint32: %s", operator)
		return nil, errors.New(message)
	}

}

func (f *Uint32Filter) Test(row *Row) (bool, error) {
	value, err := GetValueFromRow(row, f.columnName)
	if err != nil {
		return false, err
	}

	val := value.(uint32)
	switch f.operator {
	case "<>":
		return val != f.target, nil
	case "<=":
		return val <= f.target, nil
	case ">=":
		return val >= f.target, nil
	case "=":
		return val == f.target, nil
	case "<":
		return val < f.target, nil
	case ">":
		return val > f.target, nil
	case "!=":
		return val != f.target, nil
	default:
		return false, errors.New("invalid operator")
	}
}

type StringFilter struct {
	columnName string
	target     string
	operator   string
}

func NewStringFilter(columnName string, target string, operator string) (*StringFilter, error) {

	supportedOperators := []string{"<>", "=", "!="}
	isSupportedOperator := false
	for _, supportedOperator := range supportedOperators {
		if operator == supportedOperator {
			isSupportedOperator = true
		}
	}

	if isSupportedOperator {
		return &StringFilter{
			columnName: columnName,
			target:     target,
			operator:   operator,
		}, nil
	} else {
		message := fmt.Sprintf("invalid input syntax for string: %s", operator)
		return nil, errors.New(message)
	}

}

func (f *StringFilter) Test(row *Row) (bool, error) {
	value, err := GetValueFromRow(row, f.columnName)
	if err != nil {
		return false, err
	}

	val := value.(string)
	switch f.operator {
	case "<>":
		return val != f.target, nil
	case "=":
		return val == f.target, nil
	case "!=":
		return val != f.target, nil
	default:
		return false, errors.New("invalid operator")
	}
}

func NewFilter(whereExpression *parser.Expression, schema map[string]string) (Filter, error) {
	// check schema and type, ensure LHS and Value are valid
	columnName := whereExpression.Condition.LHS
	if schema[columnName] == "" {
		message := fmt.Sprintf("column \"%s\" does not exist", columnName)
		return nil, errors.New(message)
	}
	columnType := schema[columnName]
	var condVal interface{}
	value := whereExpression.Condition.Value
	if value.Str == nil {
		condVal = value.Number
	} else {
		condVal = value.Str
	}
	condValType := value.Type()

	supportedTypeMap := make(map[string][]string)
	supportedTypeMap["string"] = []string{"string"}
	supportedTypeMap["float64"] = []string{"uint32"}

	isSupportedType := false
	for _, supportedType := range supportedTypeMap[condValType] {
		if columnType == supportedType {
			isSupportedType = true
		}
	}

	if isSupportedType == false {
		message := fmt.Sprintf("invalid input syntax for %s: %s", columnName, value)
		return nil, errors.New(message)

	}

	operator := whereExpression.Condition.Compare.Operator
	if columnType == "uint32" {
		target := condVal.(*float64)
		return NewUint32Filter(columnName, uint32(*target), operator)
	} else {
		target := condVal.(*string)
		return NewStringFilter(columnName, *target, operator)
	}
}
