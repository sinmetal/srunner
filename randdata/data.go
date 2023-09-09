package randdata

import (
	"math/rand"

	stores1 "github.com/sinmetal/srunner/pattern1/stores"
)

var itemsPattern1 [4]*stores1.Item

func init() {
	itemsPattern1[0] = &stores1.Item{
		ItemID:   "gae",
		ItemName: "Google App Engine",
		Price:    100,
	}
	itemsPattern1[1] = &stores1.Item{
		ItemID:   "gce",
		ItemName: "Google Compute Engine",
		Price:    75,
	}
	itemsPattern1[2] = &stores1.Item{
		ItemID:   "bq",
		ItemName: "BigQuery",
		Price:    150,
	}
	itemsPattern1[3] = &stores1.Item{
		ItemID:   "gcs",
		ItemName: "Google Cloud Storage",
		Price:    90,
	}
}

var allAuthor = []string{"gold", "silver", "dia", "ruby", "sapphire"}

// GetAuthor is randomに1人返す
func GetAuthor() string {
	return allAuthor[rand.Intn(len(allAuthor))]
}

// GetAuthors is randomに数人返す
func GetAuthors() []string {
	exists := make(map[string]string)

	count := rand.Intn(4)
	for i := 0; i < count; i++ {
		a := GetAuthor()
		exists[a] = a
	}

	authors := []string{}
	for k, _ := range exists {
		authors = append(authors, k)
	}
	return authors
}

// GetAllAuthors is 全員返す
func GetAllAuthors() []string {
	return allAuthor
}

func GetItemsAllPattern1() [4]*stores1.Item {
	return itemsPattern1
}

func GetItemPattern1() *stores1.Item {
	i := rand.Int31n(int32(len(itemsPattern1) - 1))
	return itemsPattern1[i]
}
