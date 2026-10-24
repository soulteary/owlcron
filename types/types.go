package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// FlexBool accepts bool, number (0/1), or string ("0"/"1"/"true"/"false")
// from JSON, which is necessary because the Cronicle frontend sends
// boolean fields as 1/0 integers.
type FlexBool bool

func (fb *FlexBool) UnmarshalJSON(data []byte) error {
	var raw interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	switch v := raw.(type) {
	case bool:
		*fb = FlexBool(v)
	case float64:
		*fb = FlexBool(v != 0)
	case string:
		*fb = FlexBool(v == "1" || v == "true")
	default:
		return fmt.Errorf("cannot convert %T to bool", raw)
	}
	return nil
}

func (fb FlexBool) Bool() bool { return bool(fb) }

// Event 表示一个调度事件
type Event struct {
	Source           string `json:"source,omitempty"`
	SourceEvent      string `json:"source_event,omitempty"`
	SourceJob        string `json:"source_job,omitempty"`
	ChainCode        int    `json:"chain_code,omitempty"`
	ChainDescription string `json:"chain_description,omitempty"`

	MemoryLimit   int64 `json:"memory_limit,omitempty"`
	MemorySustain int   `json:"memory_sustain,omitempty"`
	CPULimit      int   `json:"cpu_limit,omitempty"`
	CPUSustain    int   `json:"cpu_sustain,omitempty"`
	LogMaxSize    int64 `json:"log_max_size,omitempty"`

	ID            string                 `json:"id"`
	Title         string                 `json:"title"`
	Enabled       FlexBool               `json:"enabled"`
	Category      string                 `json:"category"`
	Plugin        string                 `json:"plugin"`
	Target        string                 `json:"target"` // 服务器组或主机名
	Timing        *Timing                `json:"timing"`
	Params        map[string]interface{} `json:"params"`
	Timezone      string                 `json:"timezone"`
	CatchUp       FlexBool               `json:"catch_up"`
	Queue         FlexBool               `json:"queue"`
	QueueMax      int                    `json:"queue_max"`
	MaxChildren   int                    `json:"max_children"`
	Timeout       int                    `json:"timeout"` // 秒
	Retries       int                    `json:"retries"`
	RetryDelay    int                    `json:"retry_delay"` // 秒
	Multiplex     FlexBool               `json:"multiplex"`
	Stagger       int                    `json:"stagger"`     // 秒
	Algo          string                 `json:"algo"`        // 服务器选择算法
	Chain         json.RawMessage        `json:"chain"`       // 成功链 (string 或 []string)
	ChainError    json.RawMessage        `json:"chain_error"` // 失败链 (string 或 []string)
	ChainData     map[string]interface{} `json:"chain_data,omitempty"`
	ChainParams   map[string]interface{} `json:"chain_params,omitempty"`
	NotifySuccess string                 `json:"notify_success"` // 成功通知邮箱
	NotifyFail    string                 `json:"notify_fail"`    // 失败通知邮箱
	WebHook       string                 `json:"web_hook"`       // Web Hook URL
	Created       int64                  `json:"created"`
	Modified      int64                  `json:"modified"`
	CWD           string                 `json:"cwd,omitempty"` // 工作目录（从插件继承）
	UID           string                 `json:"uid,omitempty"` // 用户ID（从插件继承）
	GID           string                 `json:"gid,omitempty"` // 组ID（从插件继承）
	Detached      FlexBool               `json:"detached,omitempty"`
}

// GetChainIDs 获取成功链事件 ID 列表（兼容 string 和 []string）
func (e *Event) GetChainIDs() []string {
	return parseChainField(e.Chain)
}

// GetChainErrorIDs 获取失败链事件 ID 列表（兼容 string 和 []string）
func (e *Event) GetChainErrorIDs() []string {
	return parseChainField(e.ChainError)
}

func parseChainField(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var single string
	if err := json.Unmarshal(raw, &single); err == nil && single != "" {
		return []string{single}
	}
	var arr []string
	if err := json.Unmarshal(raw, &arr); err == nil {
		return arr
	}
	return nil
}

// Timing 表示事件的时间规则
type Timing struct {
	Minutes  []int `json:"minutes"`  // 0-59
	Hours    []int `json:"hours"`    // 0-23
	Weekdays []int `json:"weekdays"` // 0-6 (0=Sunday)
	Days     []int `json:"days"`     // 1-31
	Months   []int `json:"months"`   // 1-12
	Years    []int `json:"years"`
}

// Job 表示一个运行中的任务
type Job struct {
	Action           string `json:"action,omitempty"`
	Source           string `json:"source,omitempty"`
	SourceEvent      string `json:"source_event,omitempty"`
	SourceJob        string `json:"source_job,omitempty"`
	ChainCode        int    `json:"chain_code,omitempty"`
	ChainDescription string `json:"chain_description,omitempty"`

	StorageError  string `json:"storage_error,omitempty"`
	UpdateVersion uint64 `json:"update_version,omitempty"`
	MemoryLimit   int64  `json:"memory_limit,omitempty"`
	MemorySustain int    `json:"memory_sustain,omitempty"`
	CPULimit      int    `json:"cpu_limit,omitempty"`
	CPUSustain    int    `json:"cpu_sustain,omitempty"`
	LogMaxSize    int64  `json:"log_max_size,omitempty"`

	ID              string                 `json:"id"`
	Event           string                 `json:"event"`
	EventTitle      string                 `json:"event_title"`
	Category        string                 `json:"category"`
	CategoryTitle   string                 `json:"category_title"`
	Plugin          string                 `json:"plugin"`
	PluginTitle     string                 `json:"plugin_title"`
	Hostname        string                 `json:"hostname"`
	Target          string                 `json:"target"`
	Command         string                 `json:"command"`
	CWD             string                 `json:"cwd"`
	UID             string                 `json:"uid"`
	GID             string                 `json:"gid"`
	Env             map[string]string      `json:"env"`
	Params          map[string]interface{} `json:"params"`
	TimeStart       float64                `json:"time_start"`
	TimeEnd         float64                `json:"time_end"`
	Elapsed         float64                `json:"elapsed"`
	Now             int64                  `json:"now"` // 事件应该运行的时间戳
	PID             int                    `json:"pid"`
	ProcessIdentity string                 `json:"process_identity,omitempty"`
	ProcessToken    string                 `json:"process_token,omitempty"`
	Code            int                    `json:"code"`
	Description     string                 `json:"description"`
	Complete        bool                   `json:"complete"`
	Progress        float64                `json:"progress"`
	LogFile         string                 `json:"log_file"`
	LogFileSize     int64                  `json:"log_file_size"`
	Detached        bool                   `json:"detached"`
	CPU             *ResourceUsage         `json:"cpu"`
	Mem             *ResourceUsage         `json:"mem"`
	Perf            interface{}            `json:"perf"`
	Table           interface{}            `json:"table,omitempty"`
	HTML            interface{}            `json:"html,omitempty"`
	Label           string                 `json:"label,omitempty"`
	UpdateEvent     map[string]interface{} `json:"update_event,omitempty"`
	AbortReason     string                 `json:"abort_reason,omitempty"`
	Pending         bool                   `json:"pending,omitempty"`
	When            float64                `json:"when,omitempty"`         // 延迟执行时间
	FlaggedTime     int64                  `json:"flagged_time,omitempty"` // 资源超限标记时间
	Timeout         int                    `json:"timeout,omitempty"`      // 超时时间（秒）
	WebHook         string                 `json:"web_hook,omitempty"`     // Web Hook URL（从事件继承）
	Retries         int                    `json:"retries,omitempty"`      // 允许重试次数
	RetryDelay      int                    `json:"retry_delay,omitempty"`  // 重试延迟（秒）
	RetriesUsed     int                    `json:"retries_used,omitempty"` // 已重试次数
	Chain           string                 `json:"chain,omitempty"`
	ChainError      string                 `json:"chain_error,omitempty"`
	ChainData       map[string]interface{} `json:"chain_data,omitempty"`
	ChainParams     map[string]interface{} `json:"chain_params,omitempty"`
	NotifySuccess   string                 `json:"notify_success,omitempty"`
	NotifyFail      string                 `json:"notify_fail,omitempty"`
}

// ResourceUsage 表示资源使用情况
type ResourceUsage struct {
	Min     float64 `json:"min"`
	Max     float64 `json:"max"`
	Total   float64 `json:"total"`
	Count   int     `json:"count"`
	Current float64 `json:"current"`
}

// Plugin 表示一个插件
type Plugin struct {
	Created  int64             `json:"created,omitempty"`
	Modified int64             `json:"modified,omitempty"`
	Username string            `json:"username,omitempty"`
	ID       string            `json:"id"`
	Title    string            `json:"title"`
	Enabled  FlexBool          `json:"enabled"`
	Command  string            `json:"command"`
	CWD      string            `json:"cwd"`
	UID      string            `json:"uid"`
	GID      string            `json:"gid"`
	Env      map[string]string `json:"env"`
	Params   []*PluginParam    `json:"params"`
}

// PluginParam 表示插件参数
type PluginParam struct {
	Items   []string    `json:"items,omitempty"`
	Size    int         `json:"size,omitempty"`
	Rows    int         `json:"rows,omitempty"`
	ID      string      `json:"id"`
	Title   string      `json:"title"`
	Type    string      `json:"type"` // string, number, boolean, hidden
	Value   interface{} `json:"value"`
	Default interface{} `json:"default"`
}

// Category 表示一个分类
type Category struct {
	Color         string   `json:"color,omitempty"`
	LogMaxSize    int64    `json:"log_max_size,omitempty"`
	Created       int64    `json:"created,omitempty"`
	Modified      int64    `json:"modified,omitempty"`
	ID            string   `json:"id"`
	Title         string   `json:"title"`
	Enabled       FlexBool `json:"enabled"`
	MaxChildren   int      `json:"max_children"`
	NotifySuccess string   `json:"notify_success"`
	NotifyFail    string   `json:"notify_fail"`
	WebHook       string   `json:"web_hook"`
	Description   string   `json:"description"`
	MemoryLimit   int64    `json:"memory_limit"`
	MemorySustain int      `json:"memory_sustain"`
	CPULimit      int      `json:"cpu_limit"`
	CPUSustain    int      `json:"cpu_sustain"`
}

// ServerGroup 表示一个服务器组
type ServerGroup struct {
	Created  int64  `json:"created,omitempty"`
	Modified int64  `json:"modified,omitempty"`
	ID       string `json:"id"`
	Title    string `json:"title"`
	Regexp   string `json:"regexp"` // 主机名匹配正则
	Master   bool   `json:"master"` // 是否有主服务器资格
}

// Server 表示一个服务器
type Server struct {
	Hostname string                 `json:"hostname"`
	IP       string                 `json:"ip"`
	Master   bool                   `json:"master"`
	Uptime   int64                  `json:"uptime"`
	Data     map[string]interface{} `json:"data"`
	Disabled bool                   `json:"disabled"`
}

// State 表示全局状态
type State struct {
	Enabled     bool                        `json:"enabled"`
	Cursors     map[string]int64            `json:"cursors"`      // 事件ID -> 时间戳
	Stats       map[string]float64          `json:"stats"`        // 统计信息
	FlaggedJobs map[string]map[string]int64 `json:"flagged_jobs"` // 被标记的任务
	Robins      map[string]int              `json:"robins"`       // 轮询计数器
	JobCodes    map[string]int              `json:"job_codes"`    // 事件最后执行结果
}

// MultiServer 表示多服务器集群状态
type MultiServer struct {
	Cluster          bool                   `json:"cluster"`
	Master           bool                   `json:"master"`
	Slave            bool                   `json:"slave"`
	MasterHostname   string                 `json:"master_hostname"`
	MasterIP         string                 `json:"master_ip"`
	Eligible         bool                   `json:"eligible"`
	LastPingReceived int64                  `json:"last_ping_received"`
	LastPingSent     int64                  `json:"last_ping_sent"`
	Data             map[string]interface{} `json:"data"`
}

// QueueTask 表示队列任务
type QueueTask struct {
	Action string                 `json:"action"`
	When   float64                `json:"when"` // 执行时间戳
	Data   map[string]interface{} `json:"data"`
}

// JobWorker 表示任务工作进程信息
type JobWorker struct {
	PID           int
	Process       interface{} // *os.Process
	LogFD         interface{} // *os.File
	Stream        interface{} // JSON 流
	ChildExited   bool
	CompleteTimer *time.Timer
	KillTimer     *time.Timer
}

// Config 表示配置
type Config struct {
	ClusterAllowInsecure         bool              `json:"cluster_allow_insecure,omitempty"`
	ClusterTLSCAFile             string            `json:"cluster_tls_ca_file,omitempty"`
	ClusterTLSCertFile           string            `json:"cluster_tls_cert_file,omitempty"`
	ClusterTLSKeyFile            string            `json:"cluster_tls_key_file,omitempty"`
	ClusterTLSRequireClientCert  bool              `json:"cluster_tls_require_client_cert,omitempty"`
	NodeAuthPrivateKey           string            `json:"node_auth_private_key,omitempty"`
	NodeAuthKeyFile              string            `json:"node_auth_key_file,omitempty"`
	NodeAuthPeers                map[string]string `json:"node_auth_peers,omitempty"`
	HTTPStreamTimeoutSeconds     int               `json:"http_stream_timeout_seconds,omitempty"`
	TrustedProxyCIDRs            []string          `json:"trusted_proxy_cidrs,omitempty"`
	SessionCookieSecure          bool              `json:"session_cookie_secure,omitempty"`
	LoginRateLimit               int               `json:"login_rate_limit,omitempty"`
	LoginAccountRateLimit        int               `json:"login_account_rate_limit,omitempty"`
	LoginRateWindowSeconds       int               `json:"login_rate_window_seconds,omitempty"`
	LoginMaxConcurrent           int               `json:"login_max_concurrent,omitempty"`
	LoginLimiterMaxEntries       int               `json:"login_limiter_max_entries,omitempty"`
	HTTPReadHeaderTimeoutSeconds int               `json:"http_read_header_timeout_seconds,omitempty"`
	HTTPReadTimeoutSeconds       int               `json:"http_read_timeout_seconds,omitempty"`
	HTTPWriteTimeoutSeconds      int               `json:"http_write_timeout_seconds,omitempty"`
	HTTPIdleTimeoutSeconds       int               `json:"http_idle_timeout_seconds,omitempty"`
	HTTPRequestTimeoutSeconds    int               `json:"http_request_timeout_seconds,omitempty"`
	JobLogHardMaxSize            int64             `json:"job_log_hard_max_size,omitempty"`
	LogUploadConcurrency         int               `json:"log_upload_concurrency,omitempty"`
	// Retention is opt-in: zero days preserves existing data indefinitely.
	JobDataExpireDays         int               `json:"job_data_expire_days,omitempty"`
	JobLogExpireDays          int               `json:"job_log_expire_days,omitempty"`
	HistoryCleanupInterval    int               `json:"history_cleanup_interval,omitempty"`
	HistoryCleanupBatchSize   int               `json:"history_cleanup_batch_size,omitempty"`
	SharedStorage             bool              `json:"shared_storage,omitempty"`
	AllowEventUpdatesFromJobs bool              `json:"allow_event_updates_from_jobs,omitempty"`
	HTTPPort                  int               `json:"http_port,omitempty"`
	Hostname                  string            `json:"hostname,omitempty"`
	BaseAppURL                string            `json:"base_app_url"`
	SecretKey                 string            `json:"secret_key"`
	LogDir                    string            `json:"log_dir"`
	QueueDir                  string            `json:"queue_dir"`
	MasterPingFreq            int               `json:"master_ping_freq"`
	MasterPingTimeout         int               `json:"master_ping_timeout"`
	UDPBroadcastPort          int               `json:"udp_broadcast_port"`
	BroadcastIP               string            `json:"broadcast_ip"`
	SchedulerStartupGrace     int               `json:"scheduler_startup_grace"`
	MaxJobs                   int               `json:"max_jobs"`
	JobMemoryMax              int64             `json:"job_memory_max"`
	JobMemorySustain          int               `json:"job_memory_sustain"`
	JobCPUMax                 int               `json:"job_cpu_max"`
	JobCPUSustain             int               `json:"job_cpu_sustain"`
	JobLogMaxSize             int64             `json:"job_log_max_size"`
	ChildKillTimeout          int               `json:"child_kill_timeout"`
	DeadJobTimeout            int               `json:"dead_job_timeout"`
	JobEnv                    map[string]string `json:"job_env"`
	WebHookTextTemplates      map[string]string `json:"web_hook_text_templates"`
	UniversalWebHook          string            `json:"universal_web_hook"`
	WebHookAllowedHosts       []string          `json:"web_hook_allowed_hosts,omitempty"`
	WebHookAllowedCIDRs       []string          `json:"web_hook_allowed_cidrs,omitempty"`
	WebHookSigningKeyID       string            `json:"web_hook_signing_key_id,omitempty"`
	WebHookSigningSecret      string            `json:"web_hook_signing_secret,omitempty"`
	WebHookSigningSecretFile  string            `json:"web_hook_signing_secret_file,omitempty"`
	SMTPHostname              string            `json:"smtp_hostname"`
	SMTPPort                  int               `json:"smtp_port"`
	SMTPUsername              string            `json:"smtp_username"`
	SMTPPassword              string            `json:"smtp_password"`
	SMTPTLSMode               string            `json:"smtp_tls_mode"`
	SMTPTimeoutSeconds        int               `json:"smtp_timeout_seconds"`
	SMTPTLSServerName         string            `json:"smtp_tls_server_name"`
	SMTPTLSCAFile             string            `json:"smtp_tls_ca_file"`
	NotificationWorkers       int               `json:"notification_workers"`
	NotificationMaxAttempts   int               `json:"notification_max_attempts"`

	EmailFrom         string                 `json:"email_from"`
	MaxEmailsPerDay   int                    `json:"max_emails_per_day"`
	StorageEngine     string                 `json:"storage_engine"`
	StorageBaseDir    string                 `json:"storage_base_dir"`
	StorageOptions    map[string]interface{} `json:"storage_options,omitempty"`
	WebHookConfigKeys []string               `json:"web_hook_config_keys,omitempty"`
	WebHookCustomData map[string]interface{} `json:"web_hook_custom_data,omitempty"`
}
