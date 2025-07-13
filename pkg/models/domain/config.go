package domain

import "fmt"

type ProfileType string

const (
	ProfileTypeWorkspace ProfileType = "workspace"
	ProfileTypeAccount   ProfileType = "account"
)

type ConfigProfile struct {
	Name string
	Type ProfileType
}

func (c ConfigProfile) String() string {
	return fmt.Sprintf("%s:%s", c.Type, c.Name)
}
