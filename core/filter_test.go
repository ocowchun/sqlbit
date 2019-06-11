package core

import (
	"fmt"
	"testing"

	"github.com/ocowchun/sqlbit/parser"
	"github.com/stretchr/testify/assert"
)

func prepareFakeWhereExpression() *parser.Expression {
	number := float64(123)
	condition := &parser.Condition{
		LHS:     "id",
		Compare: &parser.Compare{Operator: ">"},
		Value: &parser.Value{
			Number: &number,
		},
	}
	return &parser.Expression{
		Condition: condition,
	}
}

func prepareFakeSchema() map[string]string {
	return map[string]string{
		"id":       "uint32",
		"username": "string",
		"email":    "string",
	}
}

func TestNewFilter(t *testing.T) {
	whereExpression := prepareFakeWhereExpression()
	schema := prepareFakeSchema()

	filter, err := NewFilter(whereExpression, schema)

	assert.Nil(t, err)
	row := NewRow(uint32(1), "username", "email")
	result, _ := filter.Test(row)
	assert.Equal(t, false, result)
}

func TestNewFilterWithInvalidColumnName(t *testing.T) {
	whereExpression := prepareFakeWhereExpression()
	whereExpression.Condition.LHS = "bad-column"
	schema := prepareFakeSchema()

	filter, err := NewFilter(whereExpression, schema)

	assert.Equal(t, "column \"bad-column\" does not exist", err.Error())
	assert.Nil(t, filter)
}

func TestNewFilterWithInvalidConditionValue(t *testing.T) {
	whereExpression := prepareFakeWhereExpression()
	whereExpression.Condition.LHS = "username"
	schema := prepareFakeSchema()

	filter, err := NewFilter(whereExpression, schema)

	number := *whereExpression.Condition.Value.Number
	expectedMessage := fmt.Sprintf("invalid input syntax for %s: %s", "username", fmt.Sprintf("%f", number))
	assert.Equal(t, expectedMessage, err.Error())
	assert.Nil(t, filter)
}
