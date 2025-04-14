package config

import (
	"fmt"
	"github.com/spf13/viper"
	"strings"
)

type QuackPipeConfiguration struct {
	Enabled       bool    `json:"enabled" mapstructure:"enabled" default:"true"`
	Root          string  `json:"root" mapstructure:"root" default:""`
	MergeTimeoutS int     `json:"merge_timeout_s" mapstructure:"merge_timeout_s" default:"10"`
	Secret        string  `json:"secret" mapstructure:"secret" default:""`
	AllowSaveToHD bool    `json:"allow_save_to_hd" mapstructure:"allow_save_to_hd" default:"true"`
	SaveTimeoutS  float64 `json:"save_timeout_s" mapstructure:"save_timeout_s" default:"1"`
	NoMerges      bool    `json:"no_merges" mapstructure:"no_merges" default:"false"`
}

type Configuration struct {
	QuackPipe QuackPipeConfiguration `json:"gigapi" mapstructure:"gigapi" default:""`
}

var Config *Configuration

func InitConfig(file string) {
	viper.SetEnvPrefix("")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	// If a file is provided, use it as the config file
	if file != "" {
		viper.SetConfigFile(file)
		err := viper.ReadInConfig()
		if err != nil {
			panic(fmt.Errorf("error reading config file: %s", err))
		}
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		fmt.Println("Using environment variables for configuration")
	}

	Config = &Configuration{}
	err := viper.Unmarshal(Config)
	if err != nil {
		panic(fmt.Errorf("unable to decode into struct: %s", err))
	}
	if Config.QuackPipe.SaveTimeoutS == 0 {
		Config.QuackPipe.SaveTimeoutS = 1
	}
	fmt.Printf("Loaded configuration: %+v\n", Config)
}
