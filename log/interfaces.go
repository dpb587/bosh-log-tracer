package log

type LineParser interface {
	Parse(Line) (Line, error)
}

type Line interface {
	LineOffset() int64
	LineData() string
}
