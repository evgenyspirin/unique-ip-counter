// ipv4_bitset_test.go
package ipv4_bitset

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func u32(a, b, c, d uint32) uint32 { return (a<<24 | b<<16 | c<<8 | d) }

func TestIPv4ByteToUint32_Valid(t *testing.T) {
	t.Parallel()
	b := New()

	cases := []struct {
		in   string
		want uint32
	}{
		{"1.1.1.1", u32(1, 1, 1, 1)},
		{"0.0.0.0", u32(0, 0, 0, 0)},
		{"255.255.255.255", u32(255, 255, 255, 255)},
		{"10.0.0.42", u32(10, 0, 0, 42)},
		{"192.168.0.1", u32(192, 168, 0, 1)},
		{"8.8.8.8", u32(8, 8, 8, 8)},
		{"001.002.003.004", u32(1, 2, 3, 4)},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			got, ok := b.IPv4ByteToUint32([]byte(tt.in))
			if !ok {
				t.Fatalf("IPv4ByteToUint32(%q) => ok=false; want true", tt.in)
			}
			if got != tt.want {
				t.Fatalf("IPv4ByteToUint32(%q) => %d; want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestIPv4ByteToUint32_Invalid(t *testing.T) {
	t.Parallel()
	b := New()

	cases := []string{
		"", "1", "1.1", "1.1.1", "1.1.1.1.1", "1.1..1", "1..1.1", "1.1.1.", ".1.1.1",
		"256.1.1.1", "1.256.1.1", "1.1.256.1", "1.1.1.256",
		"999.1.1.1", "a.b.c.d", "1.a.1.1", "1.1.1.a",
		" 1.1.1.1", "1.1.1.1 ", "1 .1.1.1",
	}
	for _, in := range cases {
		in := in
		t.Run(fmt.Sprintf("bad_%q", in), func(t *testing.T) {
			t.Parallel()
			if _, ok := b.IPv4ByteToUint32([]byte(in)); ok {
				t.Fatalf("IPv4ByteToUint32(%q) => ok=true; want false", in)
			}
		})
	}
}

func TestSetIfNew_Idempotent(t *testing.T) {
	t.Parallel()
	bs := New()

	addr := u32(10, 0, 0, 1)
	if !bs.SetIfNew(addr) {
		t.Fatalf("first SetIfNew should return true")
	}
	if bs.SetIfNew(addr) {
		t.Fatalf("second SetIfNew should return false (already set)")
	}
}

func TestSetIfNew_CrossShards_Independent(t *testing.T) {
	t.Parallel()
	bs := New()

	a1 := (uint32(0x1234) << 16) | 0x0001
	a2 := (uint32(0x5678) << 16) | 0x0001

	if !bs.SetIfNew(a1) {
		t.Fatalf("a1 first set must be true")
	}
	if !bs.SetIfNew(a2) {
		t.Fatalf("a2 first set must be true (independent shard)")
	}
	if bs.SetIfNew(a1) {
		t.Fatalf("a1 second set must be false")
	}
	if bs.SetIfNew(a2) {
		t.Fatalf("a2 second set must be false")
	}
}

func TestSetIfNew_ConcurrentSameIP(t *testing.T) {
	t.Parallel()
	bs := New()
	const goroutines = 64
	addr := u32(100, 64, 32, 16)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	var trues int64
	for i := 0; i < goroutines; i++ {
		go func() {
			if bs.SetIfNew(addr) {
				atomic.AddInt64(&trues, 1)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	if trues != 1 {
		t.Fatalf("exactly one goroutine should get true; got %d", trues)
	}
}

func TestSetIfNew_ManyIPs_ManyShards(t *testing.T) {
	t.Parallel()
	bs := New()

	N := 4096
	gotTrue := 0
	for i := 0; i < N; i++ {
		hi := uint32(i % 1024)
		lo := uint32((i * 131) & 0xFFFF)
		u := (hi << 16) | lo
		if bs.SetIfNew(u) {
			gotTrue++
		}
		if bs.SetIfNew(u) {
			t.Fatalf("SetIfNew returned true on a repeated address (u=%08x)", u)
		}
	}
	if gotTrue != N {
		t.Fatalf("unique first sets %d; want %d", gotTrue, N)
	}
}

func TestGetOrCreate_IsCASdedAndReusable(t *testing.T) {
	t.Parallel()
	bs := New()

	const hi = uint16(0xBEEF)
	const workers = 64

	var wg sync.WaitGroup
	wg.Add(workers)

	ptrs := make([]*shard16, workers)

	for i := 0; i < workers; i++ {
		i := i
		go func() {
			ptrs[i] = bs.getOrCreate(hi)
			wg.Done()
		}()
	}
	wg.Wait()

	base := ptrs[0]
	if base == nil {
		t.Fatalf("getOrCreate returned nil shard")
	}
	if len(base.bits) != 1024 {
		t.Fatalf("bits length = %d; want 1024", len(base.bits))
	}
	for i := 1; i < workers; i++ {
		if ptrs[i] != base {
			t.Fatalf("expected all pointers to be the same instance; idx %d differs", i)
		}
	}

	if got := bs.shards[hi].Load(); got != base {
		t.Fatalf("atomic pointer mismatch with created shard")
	}
}

func TestUniqueCounter_SerialAndConcurrent(t *testing.T) {
	t.Parallel()
	bs := New()

	// serial
	for i := 0; i < 10; i++ {
		bs.AddUnique(1)
	}
	// no-op with zero
	bs.AddUnique(0)
	if c := bs.GetUniqueCount(); c != 10 {
		t.Fatalf("GetUniqueCount=%d; want 10", c)
	}

	// concurrent
	var wg sync.WaitGroup
	adders := runtime.GOMAXPROCS(0) * 4
	per := 1000
	wg.Add(adders)
	for i := 0; i < adders; i++ {
		go func() {
			for j := 0; j < per; j++ {
				bs.AddUnique(1)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	want := uint64(10 + adders*per)
	if c := bs.GetUniqueCount(); c != want {
		t.Fatalf("GetUniqueCount=%d; want %d", c, want)
	}
}

func TestSetIfNew_BitPositionsAll64Slots(t *testing.T) {
	t.Parallel()
	bs := New()

	hi := uint32(0x1234)
	for pos := uint32(0); pos < 64; pos++ {
		lo := (uint32(7) << 6) | pos // idx=7, varying bit
		u := (hi << 16) | lo
		if !bs.SetIfNew(u) {
			t.Fatalf("expected first set true at bit pos %d", pos)
		}
		if bs.SetIfNew(u) {
			t.Fatalf("expected second set false at bit pos %d", pos)
		}
	}
}

func TestIPv4ByteToUint32_EdgeLengths(t *testing.T) {
	t.Parallel()
	b := New()
	if _, ok := b.IPv4ByteToUint32([]byte("0.0.0.0")); !ok {
		t.Fatalf("expected min-length IPv4 to be valid")
	}
	if _, ok := b.IPv4ByteToUint32([]byte("255.255.255.255")); !ok {
		t.Fatalf("expected max-length IPv4 to be valid")
	}
}

func TestIPv4ByteToUint32_RandomValid(t *testing.T) {
	t.Parallel()
	b := New()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 1000; i++ {
		a := uint32(r.Intn(256))
		c := uint32(r.Intn(256))
		d := uint32(r.Intn(256))
		e := uint32(r.Intn(256))
		s := fmt.Sprintf("%d.%d.%d.%d", a, c, d, e)
		got, ok := b.IPv4ByteToUint32([]byte(s))
		if !ok {
			t.Fatalf("random valid %q parsed as invalid", s)
		}
		want := u32(a, c, d, e)
		if got != want {
			t.Fatalf("parse %q => %d; want %d", s, got, want)
		}
	}
}

func TestIPv4ByteToUint32_NoAllocOnHappyPath(t *testing.T) {
	b := New()
	ip := []byte("123.45.67.89")

	allocs := testing.AllocsPerRun(1000, func() {
		if _, ok := b.IPv4ByteToUint32(ip); !ok {
			t.Fatal("unexpected parse failure")
		}
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs; got %.2f", allocs)
	}
}

func TestIPv4ByteToUint32_RejectsInternalSpaces(t *testing.T) {
	t.Parallel()
	b := New()
	if _, ok := b.IPv4ByteToUint32([]byte("1. 1.1.1")); ok {
		t.Fatalf("internal spaces must be invalid")
	}
}

func TestSetIfNew_WithParsedInput(t *testing.T) {
	t.Parallel()
	bs := New()

	ip := []byte("203.0.113.7")
	u, ok := bs.IPv4ByteToUint32(ip)
	if !ok {
		t.Fatalf("parse failed for %q", ip)
	}
	if !bs.SetIfNew(u) {
		t.Fatalf("first SetIfNew should be true")
	}
	if bs.SetIfNew(u) {
		t.Fatalf("second SetIfNew should be false")
	}
}

func TestIPv4ByteToUint32_BufferReuse(t *testing.T) {
	t.Parallel()
	b := New()

	buf := bytes.NewBufferString("10.0.0.1")
	got1, ok1 := b.IPv4ByteToUint32(buf.Bytes())
	if !ok1 {
		t.Fatal("first parse failed")
	}
	buf.Reset()
	buf.WriteString("10.0.0.2")
	got2, ok2 := b.IPv4ByteToUint32(buf.Bytes())
	if !ok2 {
		t.Fatal("second parse failed")
	}
	if got1 == got2 {
		t.Fatalf("different inputs must give different outputs")
	}
}

func ExampleBitset_IPv4ByteToUint32() {
	b := New()
	u, ok := b.IPv4ByteToUint32([]byte("192.168.1.10"))
	if ok {
		fmt.Printf("%08x\n", u)
	}
}
