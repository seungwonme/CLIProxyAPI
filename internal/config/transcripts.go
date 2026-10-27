package config

// Transcripts is deliberately separate from debug and native request-log.
// Storage always enforces the fixed aggregate 10 GiB logical quota, without age expiry.
// Root and user bindings are startup settings; disabling requires a restart to
// restore the preexisting native request logger configuration.
type Transcripts struct {
	Enabled bool             `yaml:"enabled" json:"enabled"`
	Root    string           `yaml:"root" json:"root"`
	Users   []TranscriptUser `yaml:"users" json:"users"`
}

// TranscriptUser refers to an existing client key in the process environment.
// Keys are never serialized in transcript configuration or storage metadata.
type TranscriptUser struct {
	EnvName string `yaml:"env_name" json:"env_name"`
	UserID  string `yaml:"user_id" json:"user_id"`
}
