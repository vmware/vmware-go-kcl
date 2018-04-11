package utils

import (
	guuid "github.com/google/uuid"
)

// MustNewUUID generates a new UUID and panics if failed
func MustNewUUID() string {
	id, err := guuid.NewUUID()
	if err != nil {
		panic(err)
	}
	return id.String()
}
