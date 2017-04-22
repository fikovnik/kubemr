package jsonpatch

import "testing"

func TestRegionEffecter(t *testing.T) {
	p := New()
	if len(p) != 0 {
		t.Errorf("Got length %s should be 0", p)
	}
	p = p.Add("foo", "/bar", "baz")
	if len(p) != 1 {
		t.Errorf("Got length %s should be 1", p)
	}
}
