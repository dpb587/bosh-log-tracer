package log

type multiParser struct {
	parsers []LineParser
}

var _ LineParser = &multiParser{}

func NewMultiParser(parsers ...LineParser) LineParser {
	return &multiParser{
		parsers: parsers,
	}
}

func (lp multiParser) Parse(in Line) (Line, error) {
	var err error

	out := in

	for _, p := range lp.parsers {
		out, err = p.Parse(out)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}
