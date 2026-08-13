package util

import "testing"

func TestSyncDirAcceptsTempDir(t *testing.T) {
	if err := SyncDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}
