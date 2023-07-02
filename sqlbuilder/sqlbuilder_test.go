package sqlbuilder

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPositive(t *testing.T) {
	tests := map[string]struct {
		b        *Builder
		wantSQL  string
		wantArgs []any
	}{
		"simple": {
			b:        NewSelectBuilder().From("parts").Where("active").Select("sum(bytes)"),
			wantSQL:  "SELECT sum(bytes) FROM parts WHERE (active)",
			wantArgs: nil,
		},
		"simple_args": {
			b:        NewSelectBuilder().From("hits").Where("EventDate <= ?", "2023-01-01").Select("*"),
			wantSQL:  "SELECT * FROM hits WHERE (EventDate <= ?)",
			wantArgs: []any{"2023-01-01"},
		},
		"WITH_simple": {
			b: NewSelectBuilder().From("system.parts").With("sum(bytes)", "s").
				Select("formatReadableSize(s)").Select("table").
				GroupBy("table").OrderBy("s").Limit("0, ?", 5),
			wantSQL:  "WITH sum(bytes) AS s SELECT formatReadableSize(s), table FROM system.parts GROUP BY table ORDER BY s LIMIT 0, ?",
			wantArgs: []any{5},
		},
		"WITH_scalar_sub_query": {
			b: NewSelectBuilder().From("system.parts").GroupBy("table").
				OrderBy("table_disk_usage DESC").Limit("?", 10).Select("table").
				With("(SELECT sum(bytes) FROM system.parts WHERE active)", "total_disk_usage"),
			wantSQL:  "WITH (SELECT sum(bytes) FROM system.parts WHERE active) AS total_disk_usage SELECT table FROM system.parts GROUP BY table ORDER BY table_disk_usage DESC LIMIT ?",
			wantArgs: []any{10},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			query, args := test.b.ToQuery()
			wantSQL := strings.Replace(test.wantSQL, "\n", " ", 0)
			require.Equal(t, query, wantSQL)
			require.Equal(t, reflect.DeepEqual(args, test.wantArgs), true)
		})
	}
}
