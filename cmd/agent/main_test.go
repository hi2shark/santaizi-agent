package main

import (
	"context"
	"net"
	"reflect"
	"testing"
)

func Test(t *testing.T) {
	cases := []struct {
		start, size int
		want        []int
	}{
		{0, 2, []int{0, 1}},
		{1, 2, []int{1, 0}},
		{0, 3, []int{0, 1, 2}},
		{1, 3, []int{1, 2, 0}},
		{2, 3, []int{2, 0, 1}},
	}

	for _, c := range cases {
		if !reflect.DeepEqual(c.want, generateQueue(c.start, c.size)) {
			t.Errorf("generateQueue(%d, %d) == %d, want %d", c.start, c.size, generateQueue(c.start, c.size), c.want)
		}
	}
}

func TestLookupIP(t *testing.T) {
	original := lookupIPAddr
	lookupIPAddr = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("192.0.2.10")}}, nil
	}
	t.Cleanup(func() { lookupIPAddr = original })

	ip, err := lookupIP("telemetry.test")
	if err != nil || ip != "192.0.2.10" {
		t.Fatalf("lookupIP() = %q, %v; want 192.0.2.10", ip, err)
	}
	ip, err = lookupIP("2001:db8::10")
	if err != nil || ip != "2001:db8::10" {
		t.Fatalf("lookupIP(literal) = %q, %v", ip, err)
	}
}
