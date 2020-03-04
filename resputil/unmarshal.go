package resputil

import "github.com/alxarch/red/resp"

type Tee []resp.Unmarshaler

func (tee Tee) Concat(u ...resp.Unmarshaler) Tee {
	for _, u := range u {
		if t, ok := u.(Tee); ok {
			tee = tee.Concat(t...)
			continue
		}
		tee = append(tee, u)
	}
	return tee
}

func (tee Tee) UnmarshalRESP(v resp.Value) error {
	for _, u := range tee {
		if err := u.UnmarshalRESP(v); err != nil {
			return err
		}
	}
	return nil
}

type onceUnmarshaler struct {
	dest interface{}
}

func Once(dest interface{}) resp.Unmarshaler {
	return &onceUnmarshaler{dest}
}
func (once *onceUnmarshaler) UnmarshalRESP(v resp.Value) error {
	if x := once.dest; x != nil {
		once.dest = nil
		return v.Decode(x)
	}
	return nil
}
