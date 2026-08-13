package util

import (
	"os"
	"runtime"
)

// SyncDir fsyncs a directory after an atomic rename so the new name is durable.
// Windows cannot FlushFileBuffers a directory handle (ERROR_ACCESS_DENIED).
func SyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
