package statement

import (
	"errors"
	"strconv"
	"strings"

	"github.com/ocowchun/sqlbit/core"
)

func extractUserFromTokens(tokens []string) (Statement, error) {
	idIdx := 1
	usernameIdx := 2
	emailIdx := 3

	userID, err := strconv.ParseUint(tokens[idIdx], 10, 32)
	if err != nil {
		return Statement{}, errors.New("id must be integer")
	}
	if userID < 1 {
		return Statement{}, errors.New("id must be positive")
	}

	username := tokens[usernameIdx]
	if len(username) > core.COLUMN_USERNAME_LENGTH {
		return Statement{}, errors.New("username too long")
	}

	email := tokens[emailIdx]
	if len(email) > core.COLUMN_EMAIL_LENGTH {
		return Statement{}, errors.New("email too long")
	}

	row := core.NewRow(uint32(userID), username, email)
	return Statement{
		Type:        StatementType_Insert,
		RowToInsert: row,
	}, nil
}

func PrepareInsert(text string) (Statement, error) {
	tokens := strings.Split(text, " ")
	if len(tokens) != 4 {
		return Statement{}, errors.New("PREPARE_SYNTAX_ERROR")
	}
	return extractUserFromTokens(tokens)
}

func ExecuteInsert(s Statement, table *core.Table) ExecuteResult {
	if table.NumRows() >= core.TABLE_MAX_ROWS {
		return ExecuteResult_TableFull
	}

	table.InsertRow(s.RowToInsert)
	return ExecuteResult_Success
}
