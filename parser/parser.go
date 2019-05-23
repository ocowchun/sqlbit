package parser

import (
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
	And *AndCondition `@@ ("OR" @@)*`
}

type AndCondition struct {
	Conditions []*Condition `@@ ( "AND" @@)*`
}

type Condition struct {
	LHS     string   `@Ident`
	Compare *Compare `@@`
	Value   *Value   `@@`
}

type Value struct {
	String *string  `  @String`
	Number *float64 `| @Number`
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
