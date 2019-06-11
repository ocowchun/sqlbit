package parser

import (
	"fmt"

	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
)

type Select struct {
	Expression *SelectExpression `"SELECT" @@`
	From       *From             `"FROM" @@`
}

type SelectExpression struct {
	All         bool         `@"*"`
	Expressions []*Attribute `| @@ ("," @@)*`
}

type Attribute struct {
	Name string `@Ident`
}

func (a *Attribute) String() string {
	return a.Name
}

type From struct {
	Name  string      `@Ident`
	Where *Expression `( "WHERE" @@ )?`
}

type Expression struct {
	Condition *Condition `@@`
}

type Condition struct {
	LHS     string   `@Ident`
	Compare *Compare `@@`
	Value   *Value   `@@`
}

type Value struct {
	Str    *string  `  @String`
	Number *float64 `| @Number`
}

func (v *Value) Type() string {
	if v.Str != nil {
		return "string"
	} else {
		return "float64"
	}
}

func (v *Value) String() string {
	if v.Str != nil {
		return *v.Str
	} else {
		return fmt.Sprintf("%f", *v.Number)
	}
}

type Compare struct {
	Operator string `@( "<>" | "<=" | ">=" | "=" | "<" | ">" | "!=" )`
}

func Parse(query string) (*Select, error) {
	sqlLexer := lexer.Must(lexer.Regexp(`(\s+)` +
		`|(?P<Keyword>(?i)SELECT|FROM|WHERE|AND|OR)` +
		`|(?P<Ident>[a-zA-Z_][a-zA-Z0-9_]*)` +
		`|(?P<Number>[-+]?\d*\.?\d+([eE][-+]?\d+)?)` +
		`|(?P<String>'[^']*'|"[^"]*")` +
		`|(?P<Operators><>|!=|<=|>=|[-+*/%,.()=<>])`,
	))
	sqlParser := participle.MustBuild(
		&Select{},
		participle.Lexer(sqlLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Keyword"),
	)
	sql := &Select{}
	err := sqlParser.ParseString(query, sql)
	return sql, err
}
