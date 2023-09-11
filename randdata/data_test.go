package randdata_test

import (
	"testing"

	"github.com/google/uuid"
)

func TestSampleUUID(t *testing.T) {
	t.Log(uuid.New().String())
}
