package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/soulteary/owlcron/api"
	"github.com/soulteary/owlcron/cli"
	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Owlcron: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configFile := flag.String("config", "config.json", "Configuration file path")
	debug := flag.Bool("debug", false, "Enable debug mode")
	flag.Parse()
	cliArgs := flag.Args()
	// Version inspection must not load configuration or create runtime state.
	if len(cliArgs) > 0 && cliArgs[0] == "version" {
		_, err := cli.Run(cliArgs, nil, nil)
		return err
	}

	// 初始化日志
	var logger *zap.Logger
	var err error
	if *debug {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		return fmt.Errorf("initialize logger: %w", err)
	}
	defer logger.Sync()

	// 加载配置
	config, err := loadConfig(*configFile, logger)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// CLI 子命令处理
	if handled, err := cli.Run(cliArgs, config, logger); handled {
		return err
	}

	// 创建引擎
	eng, err := engine.NewEngine(config, logger)
	if err != nil {
		return fmt.Errorf("create engine: %w", err)
	}

	// Reserve the listener before startup can dispatch plugins.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.HTTPPort))
	if err != nil {
		_ = eng.Shutdown()
		return fmt.Errorf("bind HTTP port: %w", err)
	}
	defer listener.Close()
	// 启动引擎
	if err := eng.Startup(); err != nil {
		_ = eng.Shutdown()
		return fmt.Errorf("start engine: %w", err)
	}

	// 启动 HTTP API + WebSocket + Web UI
	mux := http.NewServeMux()
	apiServer := api.NewServer(eng, logger)
	apiServer.RegisterRoutes(mux)
	eng.GetCommunication().RegisterRoutes(mux)

	// 静态文件服务（Web UI）
	htdocsDir := "htdocs"
	if _, err := os.Stat(htdocsDir); err == nil {
		mux.Handle("/", http.FileServer(http.Dir(htdocsDir)))
	}

	httpServer, err := newHTTPServer(config, api.CORS(apiServer.AuthenticationMiddleware(mux)))
	if err != nil {
		_ = eng.Shutdown()
		return fmt.Errorf("configure HTTP server: %w", err)
	}

	httpServer.TLSConfig = eng.ClusterServerTLSConfig()
	httpErrors := make(chan error, 1)
	go func() {
		logger.Info("HTTP server started", zap.String("addr", httpServer.Addr))
		serve := httpServer.Serve
		if httpServer.TLSConfig != nil {
			serve = func(listener net.Listener) error { return httpServer.ServeTLS(listener, "", "") }
		}
		if err := serve(listener); err != nil && err != http.ErrServerClosed {
			httpErrors <- err
		}
	}()

	logger.Info("OwlCron engine started successfully")

	// 等待系统信号或引擎关闭
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var runErr error
	select {
	case <-eng.ShutdownChan():
	case <-eng.ShutdownRequested():
	case sig := <-sigChan:
		logger.Info("Signal received", zap.String("signal", sig.String()))
	case err := <-httpErrors:
		runErr = fmt.Errorf("serve HTTP: %w", err)
	}
	signal.Stop(sigChan)

	logger.Info("Shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		_ = httpServer.Close()
		if runErr == nil {
			runErr = fmt.Errorf("shutdown HTTP: %w", err)
		}
	}
	if err := eng.Shutdown(); err != nil {
		if runErr == nil {
			runErr = fmt.Errorf("shutdown engine: %w", err)
		}
	}
	if eng.RestartRequested() && runErr == nil {
		executable, err := os.Executable()
		if err != nil {
			return fmt.Errorf("locate restart executable: %w", err)
		}
		replacement := exec.Command(executable, os.Args[1:]...)
		replacement.Env = os.Environ()
		replacement.Stdin = os.Stdin
		replacement.Stdout = os.Stdout
		replacement.Stderr = os.Stderr
		if err = replacement.Start(); err != nil {
			return fmt.Errorf("start replacement: %w", err)
		}
	}
	return runErr
}

// loadConfig 加载配置
func loadConfig(file string, logger *zap.Logger) (*types.Config, error) {
	// 如果文件不存在，使用默认配置
	if _, err := os.Stat(file); os.IsNotExist(err) {
		if logger != nil {
			logger.Warn("Config file not found, using default configuration", zap.String("file", file))
		}
		config := getDefaultConfig()
		if err := initializeSecret(config); err != nil {
			return nil, err
		}
		return config, nil
	}

	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	config := *getDefaultConfig()
	// Resolve an omitted URL after reading the actual listening port.
	config.BaseAppURL = ""
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	// 应用默认值（如果未设置）
	applyDefaults(&config)
	if err := initializeSecret(&config); err != nil {
		return nil, err
	}
	return &config, nil
}

// getDefaultConfig 获取默认配置
func getDefaultConfig() *types.Config {
	config := &types.Config{
		BaseAppURL:            "http://localhost:3012",
		SecretKey:             "",
		HTTPPort:              3012,
		LogDir:                "logs",
		QueueDir:              "queue",
		MasterPingFreq:        20,
		MasterPingTimeout:     60,
		UDPBroadcastPort:      3014,
		BroadcastIP:           "",
		SchedulerStartupGrace: 10,
		MaxJobs:               0,
		JobMemoryMax:          1073741824,
		JobMemorySustain:      0,
		JobCPUMax:             0,
		JobCPUSustain:         0,
		JobLogMaxSize:         0,
		ChildKillTimeout:      10,
		DeadJobTimeout:        120,
		JobEnv:                make(map[string]string),
		WebHookTextTemplates: map[string]string{
			"job_start":          "Job started on [hostname]: [event_title] [job_details_url]",
			"job_complete":       "Job completed successfully on [hostname]: [event_title] [job_details_url]",
			"job_failure":        "Job failed on [hostname]: [event_title]: Error [code]: [description] [job_details_url]",
			"job_launch_failure": "Failed to launch scheduled event: [event_title]: [description] [edit_event_url]",
		},
	}
	return config
}

// applyDefaults 应用默认值
func applyDefaults(config *types.Config) {
	if config.HTTPPort == 0 {
		config.HTTPPort = 3012
	}
	if config.BaseAppURL == "" {
		scheme := "http"
		if config.ClusterTLSCertFile != "" {
			scheme = "https"
		}
		config.BaseAppURL = fmt.Sprintf("%s://localhost:%d%s", scheme, config.HTTPPort, config.BasePath)
	}
	if config.MasterPingFreq == 0 {
		config.MasterPingFreq = 20
	}
	if config.MasterPingTimeout == 0 {
		config.MasterPingTimeout = 60
	}
	if config.UDPBroadcastPort < 0 {
		config.UDPBroadcastPort = 0
	}
	if config.SchedulerStartupGrace < 0 {
		config.SchedulerStartupGrace = 0
	}
	if config.ChildKillTimeout == 0 {
		config.ChildKillTimeout = 10
	}
	if config.DeadJobTimeout == 0 {
		config.DeadJobTimeout = 120
	}
	if config.LogDir == "" {
		config.LogDir = "logs"
	}
	if config.QueueDir == "" {
		config.QueueDir = "queue"
	}
	if config.JobEnv == nil {
		config.JobEnv = make(map[string]string)
	}
	if config.WebHookTextTemplates == nil {
		config.WebHookTextTemplates = map[string]string{
			"job_start":          "Job started on [hostname]: [event_title] [job_details_url]",
			"job_complete":       "Job completed successfully on [hostname]: [event_title] [job_details_url]",
			"job_failure":        "Job failed on [hostname]: [event_title]: Error [code]: [description] [job_details_url]",
			"job_launch_failure": "Failed to launch scheduled event: [event_title]: [description] [edit_event_url]",
		}
	}
}

// Persist generated secrets privately so a restart does not invalidate node
// authentication. Multi-node deployments supply the same explicit secret.
func initializeSecret(config *types.Config) error {
	if secret := os.Getenv("OWLCRON_SECRET_KEY"); secret != "" {
		config.SecretKey = secret
	}
	if config.HTTPPort < 1 || config.HTTPPort > 65535 {
		return fmt.Errorf("http_port must be 1-65535")
	}
	if config.SecretKey != "" && config.SecretKey != "CHANGE_ME_TO_RANDOM_STRING" {
		if len(config.SecretKey) < 32 {
			return fmt.Errorf("secret_key must contain at least 32 bytes")
		}
		return nil
	}
	base := config.StorageBaseDir
	if base == "" {
		base = "data"
	}
	if err := os.MkdirAll(base, 0700); err != nil {
		return err
	}
	path := filepath.Join(base, "cluster-secret")
	// Publish a fully synced file atomically; another process never reads an
	// empty secret from a concurrent initializer.
	file, err := os.CreateTemp(base, ".cluster-secret-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	bytes := make([]byte, 32)
	if _, err = rand.Read(bytes); err != nil {
		return err
	}
	secret := hex.EncodeToString(bytes)
	if _, err = file.WriteString(secret); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Link(file.Name(), path); err != nil && !os.IsExist(err) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("cluster-secret must be a private regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) != 64 {
		return fmt.Errorf("invalid persisted cluster secret")
	}
	config.SecretKey = string(data)
	directory, err := os.Open(base)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
