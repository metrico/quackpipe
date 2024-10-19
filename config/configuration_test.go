package config

import (
	"fmt"
	"testing"
)

func TestInitConfig(t *testing.T) {
	InitConfig("config_test.yaml")
	fmt.Println(Config)
}
