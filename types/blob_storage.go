package types

// BlobStorageConfig configures immutable content independently of scheduler
// metadata. Omitted configuration preserves the legacy storage path.
type BlobStorageConfig struct {
	WriteStore string                     `json:"write_store"`
	Stores     map[string]BlobStoreConfig `json:"stores"`
}

type BlobStoreConfig struct {
	Engine        string `json:"engine"`
	BaseDir       string `json:"base_dir,omitempty"`
	Endpoint      string `json:"endpoint,omitempty"`
	Bucket        string `json:"bucket,omitempty"`
	Prefix        string `json:"prefix,omitempty"`
	Region        string `json:"region,omitempty"`
	PathStyle     bool   `json:"path_style,omitempty"`
	AccessKey     string `json:"access_key,omitempty"`
	SecretKey     string `json:"secret_key,omitempty"`
	SessionToken  string `json:"session_token,omitempty"`
	AccessKeyFile string `json:"access_key_file,omitempty"`
	SecretKeyFile string `json:"secret_key_file,omitempty"`
}

type JobLogStorageConfig struct {
	WriteFormat        int   `json:"write_format"`
	SpoolMaxBytes      int64 `json:"spool_max_bytes,omitempty"`
	SpoolResumePercent int   `json:"spool_resume_percent,omitempty"`
	GCGraceSeconds     int   `json:"gc_grace_seconds,omitempty"`
	GCBatchSize        int   `json:"gc_batch_size,omitempty"`
}
