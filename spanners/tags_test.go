package spanners_test

import (
	"testing"

	"github.com/sinmetal/srunner/spanners"
)

func TestAppTag(t *testing.T) {
	cases := []struct {
		name string
		tags []string
		want string
	}{
		{"empty", []string{}, "app=github.com/sinmetal/srunner"},
		{"additional", []string{"env=dev", "mode=hoge"}, "app=github.com/sinmetal/srunner,env=dev,mode=hoge"},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := spanners.AppTag(tt.tags...)
			if got != tt.want {
				t.Errorf("want %s but got %s", tt.want, got)
			}
		})
	}
}
