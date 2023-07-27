package rudder

import (
	"fmt"
	"testing"
)

func TestParseMode(t *testing.T) {
	mode, err := ParseMode("script")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(mode)
}
