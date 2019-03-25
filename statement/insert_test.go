package statement

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ocowchun/sqlbit/core"
)

func TestPrepareInsert(t *testing.T) {
	text := "insert 1 cstack foo@bar.com"

	s, err := PrepareInsert(text)

	assert.Nil(t, err)
	assert.Equal(t, StatementType_Insert, s.Type)
	assert.Equal(t, 1, s.RowToInsert.ID)
	var expectedUsername [core.COLUMN_USERNAME_LENGTH]rune
	copy(expectedUsername[:], []rune("cstack"))
	assert.Equal(t, expectedUsername, s.RowToInsert.Username)
	var expectedEmail [core.COLUMN_EMAIL_LENGTH]rune
	copy(expectedEmail[:], []rune("foo@bar.com"))
	assert.Equal(t, expectedEmail, s.RowToInsert.Email)
}

func TestPrepareInsert_withSyntaxError(t *testing.T) {
	text := "insert 1 cstack"

	_, err := PrepareInsert(text)

	assert.Equal(t, "PREPARE_SYNTAX_ERROR", err.Error())
}

func TestPrepareInsert_withNegativeId(t *testing.T) {
	text := "insert -1 cstack foo@bar.com"

	_, err := PrepareInsert(text)

	assert.Equal(t, "id must be positive", err.Error())
}
func TestPrepareInsert_withInvalidDataType(t *testing.T) {
	text := "insert s cstack foo@bar.com"

	_, err := PrepareInsert(text)

	assert.Equal(t, "id must be integer", err.Error())
}

func TestPrepareInsert_withInvalidLength(t *testing.T) {
	username := ""
	for i := 0; i < core.COLUMN_USERNAME_LENGTH+1; i++ {
		username = username + "a"
	}
	text := fmt.Sprintf("insert 1 %s foo@bar.com", username)

	_, err := PrepareInsert(text)

	assert.Equal(t, "username too long", err.Error())
}

func TestExecuteInsert(t *testing.T) {
	var username [core.COLUMN_USERNAME_LENGTH]rune
	copy(username[:], []rune("harry"))
	var email [core.COLUMN_EMAIL_LENGTH]rune
	copy(email[:], []rune("harry@hogwarts.edu"))
	id := 1
	rowToInsert := &core.Row{ID: id, Username: username, Email: email}
	s := Statement{StatementType_Insert, rowToInsert}
	table := &core.Table{}

	result := ExecuteInsert(s, table)

	assert.Equal(t, ExecuteResult_Success, result)
	row := table.Pages()[0].Rows()[0]
	assert.Equal(t, id, row.ID)
	assert.Equal(t, username, row.Username)
	assert.Equal(t, email, row.Email)
}
