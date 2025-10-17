package file_processor

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"

	"unique-ip-counter/internal/ipv4_bitset"
)

func mustTempFile(t *testing.T, name string, data []byte) *os.File {
	t.Helper()
	dir := t.TempDir()
	fp := filepath.Join(dir, name)
	if err := os.WriteFile(fp, data, 0o600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	f, err := os.Open(fp)
	if err != nil {
		t.Fatalf("open temp file: %v", err)
	}
	return f
}

func fileSize(t *testing.T, f *os.File) int64 {
	t.Helper()
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	return fi.Size()
}

func Test_trimCRLF(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in, want []byte
	}{
		{[]byte("1.2.3.4\n"), []byte("1.2.3.4")},
		{[]byte("1.2.3.4\r\n"), []byte("1.2.3.4")},
		{[]byte("1.2.3.4\r"), []byte("1.2.3.4")},
		{[]byte("1.2.3.4"), []byte("1.2.3.4")},
		{[]byte("\r\n"), []byte("")},
		{[]byte("\n"), []byte("")},
		{[]byte(""), []byte("")},
	}
	for i, tt := range cases {
		i, tt := i, tt
		t.Run(funcName("case", i), func(t *testing.T) {
			t.Parallel()
			got := trimCRLF(append([]byte(nil), tt.in...))
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("trimCRLF(%q) = %q; want %q", tt.in, got, tt.want)
			}
		})
	}
}

func funcName(prefix string, n int) string { return prefix + "_" + string(rune('a'+n)) }

func Test_splitToShards_Alignment(t *testing.T) {
	logger := zap.NewNop()
	data := []byte("AAA\nBBBBB\nCC\nDDD\nEEEEEEEE\nF\n")
	f := mustTempFile(t, "align.txt", data)
	defer f.Close()

	fp := New(logger, f, ipv4_bitset.New(), 3)
	size := fileSize(t, f)

	shs, err := fp.splitToShards(size, 3)
	if err != nil {
		t.Fatalf("splitToShards error: %v", err)
	}
	if len(shs) != 3 {
		t.Fatalf("len(shards)=%d; want 3", len(shs))
	}

	for i, s := range shs {
		if s.Start < 0 || s.End < s.Start || s.End > size {
			t.Fatalf("bad shard bounds: %+v", s)
		}
		if i < len(shs)-1 {
			b := make([]byte, 1)
			if _, err := f.ReadAt(b, s.End-1); err != nil {
				t.Fatalf("read end-1: %v", err)
			}
			if b[0] != '\n' {
				t.Fatalf("shard %d does not end at newline; got 0x%02x", i, b[0])
			}
			next := shs[i+1]
			if next.Start < s.End {
				t.Fatalf("next shard starts before current ends: %d < %d", next.Start, s.End)
			}
			if next.Start > 0 {
				if _, err := f.ReadAt(b, next.Start-1); err != nil {
					t.Fatalf("read start-1: %v", err)
				}
				if b[0] != '\n' {
					t.Fatalf("next shard %d not aligned to newline; prev byte is 0x%02x", i+1, b[0])
				}
			}
		}
	}
}

func Test_splitToShards_SmallFiles(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop()
	data := []byte("X\n")
	f := mustTempFile(t, "small.txt", data)
	defer f.Close()

	fp := New(logger, f, ipv4_bitset.New(), 100)
	size := fileSize(t, f)

	shs, err := fp.splitToShards(size, 100)
	if err != nil {
		t.Fatalf("splitToShards error: %v", err)
	}
	if len(shs) != 1 {
		t.Fatalf("len(shards)=%d; want 1", len(shs))
	}
	if shs[0].Start != 0 || shs[0].End != size {
		t.Fatalf("unexpected single shard: %+v (size=%d)", shs[0], size)
	}
}

func Test_moveStartToNewline(t *testing.T) {
	logger := zap.NewNop()
	data := []byte("AAA\nBBBBB\nCCC")
	f := mustTempFile(t, "move.txt", data)
	defer f.Close()

	fp := New(logger, f, ipv4_bitset.New(), 2)

	start := int64(bytes.Index(data, []byte("BBBBB"))) + 2
	s := shard{Start: start, End: int64(len(data))}
	got, err := fp.moveStartToNewline(s)
	if err != nil {
		t.Fatalf("moveStartToNewline error: %v", err)
	}

	nl2 := int64(bytes.LastIndex(data, []byte{'\n'}))
	if got.Start != nl2+1 {
		t.Fatalf("start moved to %d; want %d", got.Start, nl2+1)
	}
	if got.End != int64(len(data)) {
		t.Fatalf("end kept %d; want %d", got.End, len(data))
	}
}

func Test_ProcessFile_EmptyFile(t *testing.T) {
	logger := zap.NewNop()
	f := mustTempFile(t, "empty.txt", nil)
	defer f.Close()

	fp := New(logger, f, ipv4_bitset.New(), 4)
	fi, _ := f.Stat()
	if err := fp.ProcessFile(context.Background(), fi); err != nil {
		t.Fatalf("ProcessFile(empty) error: %v", err)
	}
	if got := fp.UniqueCount(); got != 0 {
		t.Fatalf("UniqueCount=%d; want 0", got)
	}
}

func Test_ProcessFile_CountUniques(t *testing.T) {
	logger := zap.NewNop()

	// 3 unique IP: 1.1.1.1, 2.2.2.2, 255.255.255.255
	data := []byte(
		"1.1.1.1\n" +
			"2.2.2.2\r\n" +
			"garbage\n" +
			"1.1.1.1\n" +
			"255.255.255.255\n",
	)
	f := mustTempFile(t, "uniq.txt", data)
	defer f.Close()

	fp := New(logger, f, ipv4_bitset.New(), 4)
	fi, _ := f.Stat()
	if err := fp.ProcessFile(context.Background(), fi); err != nil {
		t.Fatalf("ProcessFile error: %v", err)
	}
	if got := fp.UniqueCount(); got != 3 {
		t.Fatalf("UniqueCount=%d; want 3", got)
	}
	if fp.GetFile() != f {
		t.Fatalf("GetFile() mismatch")
	}
}

func Test_processShard_SingleShard(t *testing.T) {
	logger := zap.NewNop()
	data := []byte("10.0.0.1\r\n10.0.0.2\n10.0.0.1\nbad\n")
	f := mustTempFile(t, "one_shard.txt", data)
	defer f.Close()

	bit := ipv4_bitset.New()
	fp := New(logger, f, bit, 1)

	s := shard{Start: 0, End: int64(len(data))}
	if err := fp.processShard(context.Background(), f, s); err != nil {
		t.Fatalf("processShard error: %v", err)
	}
	if got := fp.UniqueCount(); got != 2 {
		t.Fatalf("UniqueCount=%d; want 2", got)
	}
}

func Test_processShard_ContextCancel(t *testing.T) {
	logger := zap.NewNop()
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	for i := 0; i < 5000; i++ {
		w.WriteString("123.45.67.89\n")
	}
	w.Flush()

	f := mustTempFile(t, "cancel.txt", buf.Bytes())
	defer f.Close()

	bit := ipv4_bitset.New()
	fp := New(logger, f, bit, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменяем сразу, до начала чтения

	s := shard{Start: 0, End: int64(len(buf.Bytes()))}
	if err := fp.processShard(ctx, f, s); err == nil {
		t.Fatalf("expected context cancellation error, got nil")
	}
}

func Test_ProcessFile_ContextCancel(t *testing.T) {
	logger := zap.NewNop()

	var buf bytes.Buffer
	for i := 0; i < 10000; i++ {
		buf.WriteString("8.8.8.8\n")
	}
	f := mustTempFile(t, "cancel_file.txt", buf.Bytes())
	defer f.Close()

	fp := New(logger, f, ipv4_bitset.New(), 8)
	fi, _ := f.Stat()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := fp.ProcessFile(ctx, fi)
	if err == nil {
		t.Fatalf("expected cancellation error, got nil")
	}
}

// splitToShards: size <= 0
func Test_splitToShards_ZeroSize(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop()
	f := mustTempFile(t, "zero.txt", nil)
	defer f.Close()

	fp := New(logger, f, ipv4_bitset.New(), 4)
	shs, err := fp.splitToShards(0, 4)
	if err != nil {
		t.Fatalf("splitToShards err: %v", err)
	}
	if shs != nil {
		t.Fatalf("want nil shards for zero size, got: %#v", shs)
	}
}

func Test_moveStartToNewline_OffBeyondEnd(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop()
	data := []byte("abc\n")
	f := mustTempFile(t, "beyond.txt", data)
	defer f.Close()

	fp := New(logger, f, ipv4_bitset.New(), 1)
	end := int64(len(data))
	got, err := fp.moveStartToNewline(shard{Start: end, End: end})
	if err != nil {
		t.Fatalf("moveStartToNewline err: %v", err)
	}
	if got.Start != end || got.End != end {
		t.Fatalf("expected Start==End==%d; got %+v", end, got)
	}
}

func Test_processShard_CRWithoutLFAtEOF(t *testing.T) {
	logger := zap.NewNop()
	data := []byte("1.2.3.4\r")
	f := mustTempFile(t, "cr_only.txt", data)
	defer f.Close()

	bit := ipv4_bitset.New()
	fp := New(logger, f, bit, 1)

	s := shard{Start: 0, End: int64(len(data))}
	err := fp.processShard(context.Background(), f, s)
	if err != nil && err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}
	if fp.UniqueCount() != 0 {
		t.Fatalf("UniqueCount=%d; want 0", fp.UniqueCount())
	}
}
