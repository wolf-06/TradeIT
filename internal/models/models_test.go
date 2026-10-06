package models

import "testing"

func TestPriceToInt(t *testing.T) {
	cases := []struct {
		in   float64
		want int64
	}{
		{0, 0},
		{0.01, 1},
		{1.25, 125},
		{99.99, 9999},
		{185.40, 18540},
		{2812.75, 281275},
	}
	for _, c := range cases {
		if got := PriceToInt(c.in); got != c.want {
			t.Errorf("PriceToInt(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestPriceToFloat(t *testing.T) {
	if got := PriceToFloat(18540); got != 185.40 {
		t.Errorf("PriceToFloat(18540) = %v, want 185.40", got)
	}
	if got := PriceToFloat(0); got != 0 {
		t.Errorf("PriceToFloat(0) = %v, want 0", got)
	}
}
