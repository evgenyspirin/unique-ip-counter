package file_processor

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"unique-ip-counter/internal/ipv4_bitset"
)

type (
	FileProcessor struct {
		logger   *zap.Logger
		file     *os.File
		bitset   *ipv4_bitset.Bitset
		th       int
		progress *Progress
	}
	shard struct {
		Start, End int64
	}
	shards []shard
)

func New(
	logger *zap.Logger,
	file *os.File,
	bitset *ipv4_bitset.Bitset,
	th int,
) *FileProcessor {
	return &FileProcessor{
		logger:   logger,
		file:     file,
		bitset:   bitset,
		th:       th,
		progress: NewProgress(logger),
	}
}

func (fp *FileProcessor) ProcessFile(ctx context.Context, fi os.FileInfo) error {
	if fi.Size() <= 0 {
		return nil
	}
	defer fp.progress.Run(fi.Size())()

	shs, err := fp.splitToShards(fi.Size(), fp.th)
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(ctx)
	for _, s := range shs {
		g.Go(func() error {
			return fp.processShard(ctx, fp.file, s)
		})
	}
	if err = g.Wait(); err != nil {
		return err
	}

	return nil
}

func (fp *FileProcessor) splitToShards(size int64, n int) (shards, error) {
	if size <= 0 {
		return nil, nil
	}
	if int64(n) > size {
		n = 1
	}

	part := size / int64(n)
	if part == 0 {
		part, n = size, 1
	}

	shs := make(shards, n)
	start := int64(0)
	for i := 0; i < n; i++ {
		end := start + part
		if i == n-1 || end > size {
			end = size
		}

		cur := shard{Start: start, End: end}
		if i > 0 {
			aligned, err := fp.moveStartToNewline(cur)
			if err != nil {
				return nil, err
			}
			shs[i-1].End = aligned.Start
			cur = aligned
		}
		shs[i] = cur
		start = end
	}
	return shs, nil
}

// moveStartToNewline Moves the start of the shard forward to the first line break character "\n"
// to avoid possible start from the middle of the line.
func (fp *FileProcessor) moveStartToNewline(s shard) (shard, error) {
	if s.Start == 0 {
		return s, nil
	}

	buf := make([]byte, 64<<10) // 64KB scan size
	off := s.Start
	for {
		if off >= s.End {
			return shard{Start: s.End, End: s.End}, nil
		}
		n, err := fp.file.ReadAt(buf, off)
		if n == 0 && err != nil {
			return s, err
		}
		idx := bytes.IndexByte(buf[:n], '\n')
		if idx >= 0 {
			return shard{Start: off + int64(idx) + 1, End: s.End}, nil
		}
		off += int64(n)
	}
}

func (fp *FileProcessor) processShard(ctx context.Context, f *os.File, s shard) error {
	r := bufio.NewReaderSize(io.NewSectionReader(f, s.Start, s.End-s.Start), 2<<20) // 2MB

	// progress
	var (
		local     int64
		localUniq uint64
	)
	const flushEvery = int64(256 << 10) // 256 KB
	flushProgress := func() {
		if fp.progress != nil && local != 0 {
			fp.progress.Add(local)
			local = 0
		}
	}
	defer func() {
		if localUniq > 0 {
			fp.bitset.AddUnique(localUniq)
		}
		flushProgress()
	}()

	for {
		// gracefully stop if parent send cancel signal
		if err := ctx.Err(); err != nil {
			return err
		}

		line, err := r.ReadSlice('\n')
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if len(line) > 0 {
			// progress
			local += int64(len(line))
			if local >= flushEvery {
				flushProgress()
			}

			if ipUint32, ok := fp.bitset.IPv4ByteToUint32(trimCRLF(line)); ok {
				if fp.bitset.SetIfNew(ipUint32) {
					localUniq++
				}
			}
		}
	}
}

func (fp *FileProcessor) GetFile() *os.File   { return fp.file }
func (fp *FileProcessor) UniqueCount() uint64 { return fp.bitset.GetUniqueCount() }

func trimCRLF(b []byte) []byte {
	for n := len(b); n > 0; n-- {
		c := b[n-1]
		if c == '\n' || c == '\r' {
			b = b[:n-1]
			continue
		}
		break
	}

	return b
}
