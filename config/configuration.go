package config

import (
	"github.com/spf13/viper"
)

type QuackPipeConfiguration struct {
	Enabled       bool   `json:"enabled" mapstructure:"enabled" default:"false"`
	Root          string `json:"root" mapstructure:"root" default:""`
	MergeTimeoutS int    `json:"merge_timeout_s" mapstructure:"merge_timeout_s" default:"60"`
	Secret        string `json:"secret" mapstructure:"secret" default:""`
}

type Configuration struct {
	QuackPipe QuackPipeConfiguration `json:"quack_pipe" mapstructure:"quack_pipe" default:""`
	DBPath    string                 `json:"db_path" mapstructure:"db_path" default:"/tmp/db"`
}

var Config *Configuration

func InitConfig(file string) {
	viper.SetConfigFile(file)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
	Config = &Configuration{}
	err = viper.Unmarshal(Config)
	if err != nil {
		panic(err)
	}
}
