package model

// params for Flags
type CommandLineFlags struct {
	Host   *string `json:"host"`
	Port   *string `json:"port"`
	Stdin  *bool   `json:"stdin"`
	Alias  *bool   `json:"alias"`
	Format *string `json:"format"`
	Params *string `json:"params"`
	DBPath *string `json:"dbpath"`
	Config *string `json:"config"`
}
