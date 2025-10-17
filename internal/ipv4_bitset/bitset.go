package ipv4_bitset

import (
	"sync/atomic"
)

type (
	Bitset struct {
		// 65 536 lazy shards
		// atomic.Pointer - thread safe
		shards [1 << 16]atomic.Pointer[shard16]
		unique atomic.Uint64
	}
	shard16 struct {
		bits []uint64 // 65536 bit => 1024 uint64 (8 KB)
	}
)

func New() *Bitset { return &Bitset{} }

func (b *Bitset) getOrCreate(hi uint16) *shard16 {
	if p := b.shards[hi].Load(); p != nil {
		return p
	}
	n := &shard16{bits: make([]uint64, 1024)}
	if b.shards[hi].CompareAndSwap(nil, n) {
		return n
	}

	return b.shards[hi].Load()
}

// SetIfNew set bit; true — new addr
func (b *Bitset) SetIfNew(u32 uint32) bool {
	hi := uint16(u32 >> 16)
	lo := u32 & 0xFFFF
	sh := b.getOrCreate(hi)

	idx := lo >> 6
	mask := uint64(1) << (lo & 63)
	for {
		old := atomic.LoadUint64(&sh.bits[idx])
		if old&mask != 0 {
			return false
		}
		if atomic.CompareAndSwapUint64(&sh.bits[idx], old, old|mask) {
			return true
		}
	}
}

func (b *Bitset) AddUnique(n uint64) {
	if n != 0 {
		b.unique.Add(n)
	}

}

func (b *Bitset) GetUniqueCount() uint64 { return b.unique.Load() }

// IPv4ByteToUint32 Parse IPV4 to uint32 with no allocations.
// input format: A.B.C.D (0-255 each)
func (b *Bitset) IPv4ByteToUint32(sb []byte) (uint32, bool) {
	// min="1.1.1.1"), max="255.255.255.255"
	if n := len(sb); n < 7 || n > 15 {
		return 0, false
	}
	var acc, part, dots uint32
	for i := 0; i < len(sb); i++ {
		c := sb[i]
		d := c - '0'
		if d <= 9 {
			part = part*10 + uint32(d)
			if part > 255 {
				return 0, false
			}
			continue
		}
		if c == '.' {
			if dots >= 3 {
				return 0, false
			}

			acc = (acc << 8) | part
			part = 0
			dots++

			continue
		}
		return 0, false
	}
	if dots != 3 {
		return 0, false
	}

	acc = (acc << 8) | part

	return acc, true
}
