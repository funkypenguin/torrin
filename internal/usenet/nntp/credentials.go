package nntp

type Credentials struct {
	Host           string
	Port           int
	Username       string
	Password       string
	SSL            bool
	MaxConnections int
}
