package core

import "errors"

type IndexCondition struct {
	ColumnName string
	Target     uint32
	Operator   string
}

func (indexCond *IndexCondition) ShouldEnd(val uint32) (bool, error) {
	switch indexCond.Operator {
	case "<=":
		return val > indexCond.Target, nil
	case ">=":
		return val < indexCond.Target, nil
	case "=":
		return val != indexCond.Target, nil
	case "<":
		return val >= indexCond.Target, nil
	case ">":
		return val <= indexCond.Target, nil
	default:
		return false, errors.New("invalid operator")
	}
}
