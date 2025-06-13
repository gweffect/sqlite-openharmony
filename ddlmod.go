package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"gorm.io/gorm/migrator"
)

// Token 表示词法单元
type Token struct {
	Type  TokenType
	Value string
}

// TokenType 表示词法单元类型
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenIdentifier
	TokenKeyword
	TokenSymbol
	TokenString
	TokenNumber
	TokenWhitespace
)

// Lexer 词法分析器
type Lexer struct {
	input string
	pos   int
}

// Parser SQL解析器
type Parser struct {
	lexer *Lexer
}

type ddl struct {
	head    string
	fields  []string
	columns []migrator.ColumnType
}

// 创建新的词法分析器
func newLexer(input string) *Lexer {
	return &Lexer{
		input: input,
		pos:   0,
	}
}

// 创建新的解析器
func newParser(input string) *Parser {
	return &Parser{
		lexer: newLexer(input),
	}
}

// 获取下一个字符
func (l *Lexer) next() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	r := rune(l.input[l.pos])
	l.pos++
	return r
}

// 查看下一个字符但不移动位置
func (l *Lexer) peek() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	return rune(l.input[l.pos])
}

// 解析标识符
func (l *Lexer) parseIdentifier() string {
	var result strings.Builder
	for {
		r := l.peek()
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			break
		}
		result.WriteRune(l.next())
	}
	return result.String()
}

// 解析字符串
func (l *Lexer) parseString() (string, error) {
	quote := l.next()
	var result strings.Builder

	for {
		r := l.next()
		if r == 0 {
			return "", errors.New("unterminated string")
		}
		if r == quote {
			break
		}
		result.WriteRune(r)
	}
	return result.String(), nil
}

// 解析DDL语句
func parseDDL(strs ...string) (*ddl, error) {
	var result ddl

	for _, str := range strs {
		parser := newParser(str)

		// 解析CREATE TABLE语句
		if strings.HasPrefix(strings.ToUpper(str), "CREATE TABLE") {
			tableName, err := parser.parseTableName()
			if err != nil {
				return nil, err
			}

			result.head = fmt.Sprintf("CREATE TABLE `%s`", tableName)

			// 解析表结构
			fields, err := parser.parseTableFields()
			if err != nil {
				return nil, err
			}

			result.fields = fields

			// 解析列信息
			columns, err := parser.parseColumns(fields)
			if err != nil {
				return nil, err
			}

			result.columns = columns
		} else if strings.HasPrefix(strings.ToUpper(str), "CREATE INDEX") {
			// 处理索引创建语句
			continue
		} else {
			return nil, errors.New("invalid DDL")
		}
	}

	return &result, nil
}

// 解析表名
func (p *Parser) parseTableName() (string, error) {
	// 跳过CREATE TABLE关键字
	for p.lexer.peek() != '`' && p.lexer.peek() != '"' && p.lexer.peek() != '\'' {
		p.lexer.next()
	}

	// 解析表名
	tableName, err := p.lexer.parseString()
	if err != nil {
		return "", err
	}

	return tableName, nil
}

// 解析表字段
func (p *Parser) parseTableFields() ([]string, error) {
	var fields []string
	var currentField strings.Builder

	// 跳过左括号
	for p.lexer.peek() != '(' {
		p.lexer.next()
	}
	p.lexer.next() // 跳过左括号

	bracketLevel := 1

	for bracketLevel > 0 {
		r := p.lexer.next()
		if r == 0 {
			return nil, errors.New("unterminated table definition")
		}

		if r == '(' {
			bracketLevel++
		} else if r == ')' {
			bracketLevel--
			if bracketLevel == 0 {
				if currentField.Len() > 0 {
					fields = append(fields, strings.TrimSpace(currentField.String()))
				}
				break
			}
		} else if r == ',' && bracketLevel == 1 {
			fields = append(fields, strings.TrimSpace(currentField.String()))
			currentField.Reset()
			continue
		}

		currentField.WriteRune(r)
	}

	return fields, nil
}

// 解析列信息
func (p *Parser) parseColumns(fields []string) ([]migrator.ColumnType, error) {
	var columns []migrator.ColumnType

	for _, field := range fields {
		field = strings.TrimSpace(field)
		if strings.HasPrefix(strings.ToUpper(field), "PRIMARY KEY") ||
			strings.HasPrefix(strings.ToUpper(field), "CHECK") ||
			strings.HasPrefix(strings.ToUpper(field), "CONSTRAINT") {
			continue
		}

		parts := strings.Fields(field)
		if len(parts) < 2 {
			continue
		}

		column := migrator.ColumnType{
			NameValue:         sql.NullString{String: strings.Trim(parts[0], "`'\""), Valid: true},
			DataTypeValue:     sql.NullString{String: parts[1], Valid: true},
			ColumnTypeValue:   sql.NullString{String: parts[1], Valid: true},
			PrimaryKeyValue:   sql.NullBool{Valid: true},
			UniqueValue:       sql.NullBool{Valid: true},
			NullableValue:     sql.NullBool{Bool: true, Valid: true},
			DefaultValueValue: sql.NullString{Valid: false},
		}

		// 解析列属性
		rest := strings.Join(parts[2:], " ")
		if strings.Contains(strings.ToUpper(rest), "NOT NULL") {
			column.NullableValue = sql.NullBool{Bool: false, Valid: true}
		}
		if strings.Contains(strings.ToUpper(rest), "UNIQUE") {
			column.UniqueValue = sql.NullBool{Bool: true, Valid: true}
		}
		if strings.Contains(strings.ToUpper(rest), "PRIMARY KEY") {
			column.PrimaryKeyValue = sql.NullBool{Bool: true, Valid: true}
		}

		// 解析默认值
		if strings.Contains(strings.ToUpper(rest), "DEFAULT") {
			defaultParts := strings.SplitN(rest, "DEFAULT", 2)
			if len(defaultParts) > 1 {
				defaultValue := strings.TrimSpace(defaultParts[1])
				if !strings.EqualFold(defaultValue, "NULL") {
					column.DefaultValueValue = sql.NullString{String: strings.Trim(defaultValue, "()"), Valid: true}
				}
			}
		}

		// 解析数据类型长度
		if strings.Contains(column.DataTypeValue.String, "(") {
			parts := strings.Split(column.DataTypeValue.String, "(")
			if len(parts) > 1 {
				lengthStr := strings.TrimRight(parts[1], ")")
				if length, err := strconv.Atoi(lengthStr); err == nil {
					column.LengthValue = sql.NullInt64{Valid: true, Int64: int64(length)}
					column.DataTypeValue.String = parts[0]
				}
			}
		}

		columns = append(columns, column)
	}

	return columns, nil
}

func (d *ddl) clone() *ddl {
	copied := new(ddl)
	*copied = *d

	copied.fields = make([]string, len(d.fields))
	copy(copied.fields, d.fields)
	copied.columns = make([]migrator.ColumnType, len(d.columns))
	copy(copied.columns, d.columns)

	return copied
}

func (d *ddl) compile() string {
	if len(d.fields) == 0 {
		return d.head
	}

	return fmt.Sprintf("%s (%s)", d.head, strings.Join(d.fields, ","))
}

func (d *ddl) renameTable(dst, src string) error {
	// 使用字符串替换替代正则表达式
	oldHead := d.head
	d.head = strings.Replace(d.head, fmt.Sprintf("`%s`", src), fmt.Sprintf("`%s`", dst), 1)

	if oldHead == d.head {
		return fmt.Errorf("failed to look up tablename `%s` from DDL head '%s'", src, d.head)
	}

	return nil
}

func (d *ddl) addConstraint(name string, sql string) {
	for i, field := range d.fields {
		if strings.HasPrefix(strings.ToUpper(field), "CONSTRAINT") &&
			strings.Contains(field, name) {
			d.fields[i] = sql
			return
		}
	}
	d.fields = append(d.fields, sql)
}

func (d *ddl) removeConstraint(name string) bool {
	for i, field := range d.fields {
		if strings.HasPrefix(strings.ToUpper(field), "CONSTRAINT") &&
			strings.Contains(field, name) {
			d.fields = append(d.fields[:i], d.fields[i+1:]...)
			return true
		}
	}
	return false
}

func (d *ddl) hasConstraint(name string) bool {
	for _, field := range d.fields {
		if strings.HasPrefix(strings.ToUpper(field), "CONSTRAINT") &&
			strings.Contains(field, name) {
			return true
		}
	}
	return false
}

func (d *ddl) getColumns() []string {
	var res []string

	for _, field := range d.fields {
		field = strings.TrimSpace(field)
		if strings.HasPrefix(strings.ToUpper(field), "PRIMARY KEY") ||
			strings.HasPrefix(strings.ToUpper(field), "CHECK") ||
			strings.HasPrefix(strings.ToUpper(field), "CONSTRAINT") ||
			strings.Contains(strings.ToUpper(field), "GENERATED ALWAYS AS") {
			continue
		}

		parts := strings.Fields(field)
		if len(parts) > 0 {
			columnName := strings.Trim(parts[0], "`'\"")
			res = append(res, "`"+columnName+"`")
		}
	}
	return res
}

func (d *ddl) removeColumn(name string) bool {
	for i, field := range d.fields {
		parts := strings.Fields(field)
		if len(parts) > 0 {
			columnName := strings.Trim(parts[0], "`'\"")
			if columnName == name {
				d.fields = append(d.fields[:i], d.fields[i+1:]...)
				return true
			}
		}
	}
	return false
}
