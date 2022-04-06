package spanners

import (
	"strings"

	"cloud.google.com/go/spanner"
)

const appTransactionTag = "app=github.com/sinmetal/srunner"

func AppTransactionTagApplyOption(tags ...string) spanner.ApplyOption {
	return spanner.TransactionTag(AppTag(tags...))
}

func AppTag(tags ...string) string {
	l := make([]string, len(tags)+1)
	l[0] = appTransactionTag
	for i, tag := range tags {
		l[i+1] = tag
	}
	return strings.Join(tags, ",")
}
