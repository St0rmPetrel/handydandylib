package sqlbuilder

import (
	"reflect"
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
		"simple_with_args": {
			b:        NewSelectBuilder().From("hits").Where("EventDate <= ?", "2023-01-01").Select("*"),
			wantSQL:  "SELECT * FROM hits WHERE (EventDate <= ?)",
			wantArgs: []any{"2023-01-01"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			query, args := test.b.ToQuery()
			require.Equal(t, query, test.wantSQL)
			require.Equal(t, reflect.DeepEqual(args, test.wantArgs), true)
		})
	}
}
