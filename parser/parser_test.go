package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectQuery(t *testing.T) {
	plan, err := Parse("select id, name from users")

	assert.Nil(t, err)
	assert.Equal(t, false, plan.Expression.All)
	actualAttributes := []string{}
	for _, attr := range plan.Expression.Expressions {
		actualAttributes = append(actualAttributes, attr.Name)
	}
	assert.Equal(t, []string{"id", "name"}, actualAttributes)
	assert.Equal(t, "users", plan.From.Name)
}

func TestSelectAll(t *testing.T) {
	plan, err := Parse("select * from users")

	assert.Nil(t, err)
	assert.Equal(t, true, plan.Expression.All)
	assert.Equal(t, "users", plan.From.Name)
}
