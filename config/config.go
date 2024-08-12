package config

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"quackpipe/model"
)

// loadConfig reads the configuration from a YAML file
func LoadConfig(filename string) (*model.Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config model.Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
