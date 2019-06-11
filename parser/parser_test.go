package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectQuery(t *testing.T) {
	query, err := Parse("select id, name from users")

	assert.Nil(t, err)
	assert.Equal(t, false, query.Expression.All)
	actualAttributes := []string{}
	for _, attr := range query.Expression.Expressions {
		actualAttributes = append(actualAttributes, attr.Name)
	}
	assert.Equal(t, []string{"id", "name"}, actualAttributes)
	assert.Equal(t, "users", query.From.Name)
	assert.Nil(t, query.From.Where)
}

func TestSelectAll(t *testing.T) {
	query, err := Parse("select * from users")

	assert.Nil(t, err)
	assert.Equal(t, true, query.Expression.All)
	assert.Equal(t, "users", query.From.Name)
}

func TestSelectWhere(t *testing.T) {
	query, err := Parse("select * from users where id = 1")

	assert.Nil(t, err)
	cond := query.From.Where.Condition
	assert.Equal(t, "id", cond.LHS)
	assert.Equal(t, "=", cond.Compare.Operator)
	assert.Equal(t, 1, int(*cond.Value.Number))
}
