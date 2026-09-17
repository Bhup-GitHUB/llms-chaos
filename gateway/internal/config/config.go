package config

import "os"

type Config struct {
	ListenAddr  string
	RegistryURL string
	DialTimeout int
	HeaderTimeout int
	MaxInflight int
}

func Load() Config {
	c := Config{
		ListenAddr:    ":8000",
		RegistryURL:   "http://localhost:8010/v1/cluster/state",
		DialTimeout:   2,
		HeaderTimeout: 5,
		MaxInflight:   256,
	}
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		c.ListenAddr = v
	}
	if v := os.Getenv("REGISTRY_URL"); v != "" {
		c.RegistryURL = v
	}
	return c
}
