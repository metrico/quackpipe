package model

// JobConfig represents a single cron job configuration
type JobConfig struct {
	Queries []string `yaml:"query"`
	Cron    string   `yaml:"cron"`
}
type OnStart struct {
	Queries []string `yaml:"query"`
}

// Config represents the configuration file structure
type Config struct {
	OnStart  OnStart     `yaml:"onStart"`
	CronJobs []JobConfig `yaml:"cronJobs"`
}
