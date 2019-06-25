package context

type Scope struct {
	annotations Annotations
	kv          map[string]interface{}
}

func (s *Scope) AddAnnotation(annotation Annotation) {
	s.annotations = append(s.annotations, annotation)
}

func (s *Scope) Set(k string, v interface{}) {
	s.kv[k] = v
}

func (s *Scope) Get(k string) (interface{}, bool) {
	v, ok := s.kv[k]

	return v, ok
}

func (s *Scope) Keys() []string {
	var res []string

	for k, _ := range s.kv {
		res = append(res, k)
	}

	return res
}
