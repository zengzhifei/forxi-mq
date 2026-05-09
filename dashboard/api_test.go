package dashboard

import "testing"

func TestCompareStreamID(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"1000-0", "1000-0", 0},
		{"1000-0", "1000-1", -1},
		{"1000-1", "1000-0", 1},
		{"999-0", "1000-0", -1},
		{"1000-0", "999-0", 1},
		{"9999-1", "10000-0", -1},  // key case: string "9999" > "10000" but numeric is less
		{"10000-0", "9999-1", 1},
		{"0-0", "0-1", -1},
		{"1678900000000-0", "1678900000001-0", -1},
		{"1678900000000-5", "1678900000000-3", 1},
	}

	for _, tt := range tests {
		got := compareStreamID(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("compareStreamID(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestExtractTopic(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"fxmq:orders", "orders"},
		{"fxmq:dead:orders", "orders"},
		{"fxmq:delay:orders", "orders"},
		{"fxmq:delay:data:orders", "orders"},
		{"fxmq:retry:orders:12345", ""},
	}

	for _, tt := range tests {
		got := extractTopic(tt.key)
		if got != tt.want {
			t.Errorf("extractTopic(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}
