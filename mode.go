package rudder

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type Mode uint64

const (
	CronMode   Mode = 1 << 0
	ScriptMode Mode = 1 << 1
)

var humanMode = map[string]Mode{
	"cron":   CronMode,
	"script": ScriptMode,
}

func (e Mode) String() string {
	if b, err := e.MarshalText(); err == nil {
		return string(b)
	} else {
		return "unknown"
	}
}

func ParseMode(lvl string) (Mode, error) {
	v, err := strconv.ParseInt(lvl, 2, 64)
	if err == nil {
		return Mode(v), err
	}

	var res Mode
	a := strings.Split(lvl, ",")
	for _, item := range a {
		if len(item) == 0 {
			continue
		}
		var (
			ok   bool
			mode Mode
		)
		for k, v := range humanMode {
			if strings.Contains(k, item) {
				ok = true
				mode = v
				break
			}
		}
		if !ok {
			return 0, fmt.Errorf("invalid mode %s, lvl: %s", item, lvl)
		}
		res = res.AddMode(mode)
	}
	return res, nil
}

func (e Mode) HasMode(m Mode) bool {
	return (e & m) == m
}

func (e Mode) AddMode(m Mode) Mode {
	return e | m
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (e *Mode) UnmarshalText(text []byte) error {
	l, err := ParseMode(string(text))
	if err != nil {
		return err
	}

	*e = l

	return nil
}

func (e Mode) MarshalText() ([]byte, error) {
	var res []byte
	for h, m := range humanMode {
		if e.HasMode(m) {
			sp := strings.Split(h, ":")
			res = append(res, append([]byte(sp[len(sp)-1]), ',')...)
		}
	}
	return bytes.TrimSuffix(res, []byte{','}), nil
}
