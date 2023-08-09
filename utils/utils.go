package utils

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
)

type statusError int

func (s statusError) Error() string {
	return fmt.Sprintf("%d - %s", int(s), http.StatusText(int(s)))
}

func ErrorHandler(f func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := f(w, r); err != nil {
			cause := errors.Cause(err)
			status := http.StatusInternalServerError
			if cause, ok := cause.(statusError); ok {
				status = int(cause)
			}

			w.WriteHeader(status)
		}
	}
}

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func GetRandomSample(s []string, n int) []string {
	rand.Seed(time.Now().UnixNano())
	if n > len(s) {
		n = len(s) - 1
	}
	rand.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
	return s[:n]
}
