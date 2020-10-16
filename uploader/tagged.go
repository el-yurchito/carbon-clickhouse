package uploader

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
)

type Tagged struct {
	*cached

	extraColumns []string          // names of columns which follow after Version column
	routeTags    map[string]string // tag name to column name
}

var (
	_ Uploader          = &Tagged{}
	_ UploaderWithReset = &Tagged{}
)

func NewTagged(base *Base) *Tagged {
	u := &Tagged{}
	u.cached = newCached(base)
	u.cached.parser = u.parseFile

	u.routeTags = make(map[string]string)
	u.extraColumns = make([]string, 0, len(u.config.DedicatedTags))
	for _, tagInfo := range u.config.DedicatedTags {
		u.routeTags[tagInfo.Name] = tagInfo.Column
		u.extraColumns = append(u.extraColumns, tagInfo.Column)
	}

	query := strings.Builder{}
	query.WriteString(u.config.TableName)
	query.WriteString(" (Date, Name, Path, Tags, Version")
	if len(u.extraColumns) > 0 {
		query.WriteString(", ")
		query.WriteString(strings.Join(u.extraColumns, ", "))
	}
	query.WriteString(")")
	u.query = query.String()

	return u
}

func urlParse(rawUrl string) (*url.URL, error) {
	p := strings.IndexByte(rawUrl, '?')
	if p < 0 {
		return url.Parse(rawUrl)
	}
	m, err := url.Parse(rawUrl[p:])
	if m != nil {
		m.Path, err = url.PathUnescape(rawUrl[:p])
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

	extraValues := make(map[string]string)
	newTagged := make(map[string]bool)
	version := uint32(time.Now().Unix())

	wb := RowBinary.GetWriteBuffer()
	defer wb.Release()

	tagsBuf := RowBinary.GetWriteBuffer()
	defer tagsBuf.Release()

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

		nameStr := unsafeString(name)
		key := fmt.Sprintf("%d:%s", reader.Days(), nameStr)

		if u.existsCache.Exists(key) {
			continue LineLoop
		}

		if newTagged[key] {
			continue LineLoop
		}

		// AD-12973: temporary hacks
		// skip all metrics that are too long
		nameLen := len(name)
		if nameLen > 1000 {
			u.logger.Warn("name too long, skipping", zap.Binary("name", name))
			continue LineLoop
		}
		// skip all metrics that do not begin with a letter
		if nameLen > 0 && !byteIsASCIILetter(name[0]) {
			u.logger.Warn("name starts with wrong char, skipping", zap.Binary("name", name))
			continue LineLoop
		}

		m, err := urlParse(nameStr)
		if err != nil {
			continue
		}

		newTagged[key] = true

		wb.Reset()
		tagsBuf.Reset()
		extraValues = make(map[string]string)

		tagsWritten := 1
		for name, values := range m.Query() {
			if column, ok := u.routeTags[name]; ok {
				extraValues[column] = values[0]
				continue
			}

			tagsBuf.WriteString(fmt.Sprintf("%s=%s", name, values[0]))
			tagsWritten++
		}

		// base columns set
		wb.WriteUint16(reader.Days())
		wb.WriteString(m.Path) // m.Path is name part of the metric; it corresponds to the Name column
		wb.WriteBytes(name)    // name is pristine metric; it corresponds to the Path column
		wb.WriteUVarint(uint64(tagsWritten))
		wb.Write(tagsBuf.Bytes())
		wb.WriteUint32(version)

		// extra columns
		for _, column := range u.extraColumns {
			wb.WriteString(extraValues[column])
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
