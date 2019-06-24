package log

type RawLine struct {
	RawLineOffset int64
	RawLineData   string
}

var _ Line = &RawLine{}

func (m RawLine) LineOffset() int64 {
	return m.RawLineOffset
}

func (m RawLine) LineData() string {
	return m.RawLineData
}
