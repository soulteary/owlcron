package api

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/soulteary/owlcron/types"
)

func TestConfigurationPaginationBoundsAndOrder(t *testing.T) {
	records := make([]*types.Category, maxConfigurationPageSize+2)
	for i := range records {
		records[i] = &types.Category{ID: fmt.Sprintf("%05d", len(records)-i-1)}
	}
	for _, tc := range []struct {
		query        string
		first, count int
	}{
		{"", 0, maxConfigurationPageSize},
		{"?offset=2&limit=3", 2, 3},
		{"?offset=-1&limit=1", 0, 1},
		{"?limit=0", 0, maxConfigurationPageSize},
		{"?limit=-1", 0, maxConfigurationPageSize},
		{"?limit=99999999", 0, maxConfigurationPageSize},
		{"?offset=99999999", len(records), 0},
		{"?offset=10001&limit=99999999", 10001, 1},
	} {
		t.Run(tc.query, func(t *testing.T) {
			page := recordPage(httptest.NewRequest("GET", "/"+tc.query, nil), append([]*types.Category(nil), records...), func(c *types.Category) string { return c.ID })
			rows := page["rows"].([]*types.Category)
			if len(rows) != tc.count || page["list"].(map[string]int)["length"] != len(records) {
				t.Fatalf("bad bounds: rows=%d metadata=%v", len(rows), page["list"])
			}
			for i, row := range rows {
				if row.ID != fmt.Sprintf("%05d", tc.first+i) {
					t.Fatalf("unexpected ID %q at %d", row.ID, i)
				}
			}
		})
	}
	page := recordPage(httptest.NewRequest("GET", "/", nil), []*types.Category{}, func(c *types.Category) string { return c.ID })
	if len(page["rows"].([]*types.Category)) != 0 {
		t.Fatal("empty list returned rows")
	}
}

func BenchmarkConfigurationRecordSort(b *testing.B) {
	records := make([]*types.Category, 1000)
	for i := range records {
		records[i] = &types.Category{ID: fmt.Sprintf("%05d", len(records)-i), Title: "Category with configuration", MaxChildren: 10}
	}
	b.Run("JSONComparator", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rows := append([]*types.Category(nil), records...)
			sort.Slice(rows, func(i, j int) bool {
				a, _ := json.Marshal(rows[i])
				z, _ := json.Marshal(rows[j])
				var x, y struct {
					ID string `json:"id"`
				}
				_ = json.Unmarshal(a, &x)
				_ = json.Unmarshal(z, &y)
				return x.ID < y.ID
			})
		}
	})
	b.Run("TypedComparator", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			recordPage(httptest.NewRequest("GET", "/", nil), append([]*types.Category(nil), records...), func(c *types.Category) string { return c.ID })
		}
	})
}
