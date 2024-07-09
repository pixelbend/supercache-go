package pcerror

import "errors"

type PolyCacheError error

var (
	PolyCacheErrorValueNotFound PolyCacheError = errors.New("PolyCacheError: value not found")
)
