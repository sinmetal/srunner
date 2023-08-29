package randdata

import "math/rand"

func GetAuthor() string {
	c := []string{"gold", "silver", "dia", "ruby", "sapphire"}
	return c[rand.Intn(len(c))]
}

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
