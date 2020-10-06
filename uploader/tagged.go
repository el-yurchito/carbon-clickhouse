package uploader

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
)

type Tagged struct {
	*cached
	ignoredMetrics map[string]bool
}

var (
	_ Uploader          = &Tagged{}
	_ UploaderWithReset = &Tagged{}
)

func NewTagged(base *Base) *Tagged {
	u := &Tagged{}
	u.cached = newCached(base)
	u.cached.parser = u.parseFile
	u.query = fmt.Sprintf("%s (Date, Tag1, Path, Tags, Version)", u.config.TableName)

	u.ignoredMetrics = make(map[string]bool, len(u.config.IgnoredTaggedMetrics))
	for _, metric := range u.config.IgnoredTaggedMetrics {
		u.ignoredMetrics[metric] = true
	}

	return u
}

func urlParse(rawurl string) (*url.URL, error) {
	p := strings.IndexByte(rawurl, '?')
	if p < 0 {
		return url.Parse(rawurl)
	}
	m, err := url.Parse(rawurl[p:])
	if m != nil {
		m.Path, err = url.PathUnescape(rawurl[:p])
		if err != nil {
			return nil, err
		}
	}
	return m, err
}

func (u *Tagged) parseFile(filename string, out io.Writer) (map[string]bool, error) {
	var reader *RowBinary.Reader
	var err error

	reader, err = RowBinary.NewReader(filename, false)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	version := uint32(time.Now().Unix())

	newTagged := make(map[string]bool)

	wb := RowBinary.GetWriteBuffer()
	tagsBuf := RowBinary.GetWriteBuffer()
	defer wb.Release()
	defer tagsBuf.Release()

	tag1 := make([]string, 0)

LineLoop:
	for {
		name, err := reader.ReadRecord()
		if err != nil { // io.EOF or corrupted file
			break
		}

		// skip not tagged
		if bytes.IndexByte(name, '?') < 0 {
			continue
		}

		key := fmt.Sprintf("%d:%s", reader.Days(), unsafeString(name))

		if u.existsCache.Exists(key) {
			continue LineLoop
		}

		if newTagged[key] {
			continue LineLoop
		}

		// AD-12973: temporary hacks
		// skip all metrics that are too long
		malformedLogger := zapwriter.Logger("upload-malformed")
		nameLen := len(name)
		if nameLen > 1000 {
			malformedLogger.Warn("name too long, skipping", zap.Binary("name", name))
			continue LineLoop
		}
		// skip all metrics that do not begin with a letter
		if nameLen > 0 && !byteIsASCIILetter(name[0]) {
			malformedLogger.Warn("name starts with wrong char, skipping", zap.Binary("name", name))
			continue LineLoop
		}

		m, err := urlParse(unsafeString(name))
		if err != nil {
			continue
		}

		newTagged[key] = true

		wb.Reset()
		tagsBuf.Reset()
		tag1 = tag1[:0]

		t := fmt.Sprintf("%s=%s", "__name__", m.Path)
		tag1 = append(tag1, t)
		tagsBuf.WriteString(t)

		// don't upload any other tag but __name__
		// if either main metric (m.Path) or each metric (*) is ignored
		ignoreAllButName := u.ignoredMetrics[m.Path] || u.ignoredMetrics["*"]
		tagsWritten := 1
		for k, v := range m.Query() {
			t := fmt.Sprintf("%s=%s", k, v[0])
			tagsBuf.WriteString(t)
			tagsWritten++

			if !ignoreAllButName {
				tag1 = append(tag1, t)
			}
		}

		for i := 0; i < len(tag1); i++ {
			if !strings.HasPrefix(tag1[i], "__name__") && u.ignoredMetrics["*"] {
				u.logger.Warn("tag1 contains ignored tag", zap.String("tag", tag1[i]))
			}
			wb.WriteUint16(reader.Days())
			wb.WriteString(tag1[i])
			wb.WriteBytes(name)
			wb.WriteUVarint(uint64(tagsWritten))
			wb.Write(tagsBuf.Bytes())
			wb.WriteUint32(version)
		}

		_, err = out.Write(wb.Bytes())
		if err != nil {
			return nil, err
		}
	}

	return newTagged, nil
}

func byteIsASCIILetter(b byte) bool {
	const (
		uppercaseA = 65
		uppercaseZ = 90
		lowercaseA = 97
		lowercaseZ = 122
	)
	return ((uppercaseA <= b) && (b <= uppercaseZ)) || ((lowercaseA <= b) && (b <= lowercaseZ))
}
