package context

type Context struct {
	scopes []*Scope
}

func (c *Context) Open(annotations ...Annotation) *Scope {
	found := c.Find(annotations...)
	if found == nil {
		found = &Scope{
			annotations: Annotations(annotations),
			kv:          map[string]interface{}{},
		}

		c.scopes = append(c.scopes, found)
	}

	return found
}

func (c *Context) Find(annotations ...Annotation) *Scope {
	typed := Annotations(annotations)

	for _, s := range c.scopes {
		if s.annotations.Contains(typed) {
			return s
		}
	}

	return nil
}
