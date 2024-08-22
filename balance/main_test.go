package balance_test

import (
	"os"
	"testing"
)

const (
	spannerProjectID  = "fake"
	spannerInstanceID = "fake"
)

func TestMain(m *testing.M) {
	//if err := spannertest.NewInstance(spannerProjectID, spannerInstanceID); err != nil {
	//	panic(err)
	//}

	ret := m.Run()

	os.Exit(ret)
}
