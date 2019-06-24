package context

type Annotations []Annotation

type Annotation struct {
	Key   string
	Value string
}

func (a Annotations) Contains(b Annotations) bool {
	for _, bv := range b {
		var found bool

		for _, av := range a {
			if bv.Key != av.Key {
				continue
			} else if bv.Value != av.Value {
				continue
			}

			found = true

			break
		}

		if !found {
			return false
		}
	}

	return true
}
