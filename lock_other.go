//go:build !(linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows)

package bolttable

import (
	"errors"
	"fmt"
	"os"
	"runtime"
)

func lockSupported() error {
	return fmt.Errorf("no file lock on %s: %w", runtime.GOOS, errors.ErrUnsupported)
}

func tryLock(*os.File, bool) (bool, error) { return false, lockSupported() }

func unlockFile(*os.File) error { return nil }

func unlockBolt(*os.File) error { return nil }
