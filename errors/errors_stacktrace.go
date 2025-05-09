package errors

import (
	"fmt"

	"github.com/superhooman-corp/go-util/codes"
)

type stacktrace struct {
	message  string
	cause    error
	code     codes.Code
	file     string
	function string
	line     int
}

func (st *stacktrace) Error() string {
	return fmt.Sprint(st.message)
}

func (st *stacktrace) ExitCode() int {
	if st.code == codes.NoCode {
		return 1
	}
	return int(st.code)
}
