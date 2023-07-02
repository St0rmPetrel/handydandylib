package sqlbuilder

import (
	"fmt"
	"strings"
)

type Builder struct {
	wth []with
	s   []string
	f   from
	whr []withArgs
	g   []string
	h   []withArgs
	ord []string
	l   withArgs
}

func NewSelectBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) From(table string) *Builder {
	b.f = from{
		table: table,
	}
	return b
}

func (b *Builder) FromSQ(query string, args []any) *Builder {
	b.f = from{
		subQuery: subQuery{
			isSq:  true,
			query: query,
			args:  args,
		},
	}
	return b
}

func (b *Builder) With(exp, iden string) *Builder {
	b.wth = append(b.wth, with{
		exp:  exp,
		iden: iden,
	})
	return b
}

func (b *Builder) WithSQ(iden, query string, args []any) *Builder {
	b.wth = append(b.wth, with{
		subQuery: subQuery{
			isSq:  true,
			query: query,
			args:  args,
		},
	})
	return b
}

func (b *Builder) Where(cond string, args ...any) *Builder {
	b.whr = append(b.whr, withArgs{
		s:    cond,
		args: args,
	})
	return b
}

func (b *Builder) GroupBy(columns string) *Builder {
	b.g = append(b.g, columns)
	return b
}

func (b *Builder) Having(cond string, args ...any) *Builder {
	b.h = append(b.h, withArgs{
		s:    cond,
		args: args,
	})
	return b
}

func (b *Builder) OrderBy(columns string) *Builder {
	b.ord = append(b.ord, columns)
	return b
}

func (b *Builder) Limit(lim string, args ...any) *Builder {
	b.l = withArgs{
		s:    lim,
		args: args,
	}
	return b
}

func (b *Builder) Select(columns string) *Builder {
	b.s = append(b.s, columns)
	return b
}

func (b *Builder) ToQuery() (string, []any) {
	var (
		args  []any
		query string
	)

	// WITH
	if len(b.wth) > 0 {
		raw := make([]string, 0, len(b.wth))
		for _, wth := range b.wth {
			if wth.isSubQuery() {
				raw = append(raw, fmt.Sprintf("%s AS (%s)", wth.iden, wth.query))
				args = append(args, wth.args...)
			} else {
				raw = append(raw, fmt.Sprintf("%s AS %s", wth.exp, wth.iden))
			}
		}
		query = fmt.Sprintf("WITH %s", strings.Join(raw, ", "))
	}
	// SELECT
	{
		rawSelect := fmt.Sprintf("SELECT %s", strings.Join(b.s, ", "))
		if query != "" {
			query = strings.Join([]string{query, rawSelect}, " ")
		} else {
			query = rawSelect
		}
	}
	// FROM
	{
		rawFrom := ""
		if b.f.isSubQuery() {
			rawFrom = fmt.Sprintf("FROM (%s)", b.f.query)
			args = append(args, b.f.args...)
		} else {
			rawFrom = fmt.Sprintf("FROM %s", b.f.table)
		}
		query = strings.Join([]string{query, rawFrom}, " ")
	}
	// WHERE
	if len(b.whr) > 0 {
		raw := mergeCondArgs(b.whr)
		args = append(args, raw.args...)
		rawWhr := fmt.Sprintf("WHERE %s", raw.s)
		query = strings.Join([]string{query, rawWhr}, " ")
	}
	// GROUP BY
	if len(b.g) > 0 {
		rawGB := fmt.Sprintf("GROUP BY %s", strings.Join(b.g, ", "))
		query = strings.Join([]string{query, rawGB}, " ")
	}
	// HAVING
	if len(b.h) > 0 {
		raw := mergeCondArgs(b.h)
		args = append(args, raw.args...)
		rawHv := fmt.Sprintf("WHERE %s", raw.s)
		query = strings.Join([]string{query, rawHv}, " ")
	}
	// ORDER BY
	if len(b.ord) > 0 {
		rawOrd := fmt.Sprintf("ORDER BY %s", strings.Join(b.ord, ", "))
		query = strings.Join([]string{query, rawOrd}, " ")
	}
	// LIMIT
	if b.l.s != "" {
		rawLim := fmt.Sprintf("LIMIT %s", b.l.s)
		args = append(args, b.l.args...)
		query = strings.Join([]string{query, rawLim}, " ")
	}

	if len(args) == 0 {
		args = nil
	}

	return query, args
}

func (b *Builder) Clone() *Builder {
	ret := new(Builder)
	*ret = *b
	return ret
}

type withArgs struct {
	s    string
	args []any
}

func mergeCondArgs(s []withArgs) withArgs {
	var (
		args []any
		str  []string
	)

	for _, e := range s {
		args = append(args, e.args...)
		str = append(str, fmt.Sprintf("(%s)", e.s))
	}
	return withArgs{
		s:    strings.Join(str, " AND "),
		args: args,
	}
}

func mergeArgs(s []withArgs) withArgs {
	var (
		args []any
		str  []string
	)

	for _, e := range s {
		args = append(args, e.args...)
		str = append(str, e.s)
	}
	return withArgs{
		s:    strings.Join(str, ", "),
		args: args,
	}
}

type subQuery struct {
	isSq  bool
	query string
	args  []any
}

func (sq subQuery) isSubQuery() bool {
	return sq.isSq
}

type from struct {
	subQuery
	table string
}

type with struct {
	subQuery
	exp  string
	iden string
}
