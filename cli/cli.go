package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

// Version and Commit may be set by release builds using Go linker flags.
var Version = "dev"
var Commit = "unknown"

func VersionString() string {
	return fmt.Sprintf("OwlCron %s (commit %s)", Version, Commit)
}

func Run(args []string, config *types.Config, logger *zap.Logger) (handled bool, err error) {
	if len(args) == 0 {
		return false, nil
	}
	switch args[0] {
	case "version":
		fmt.Println(VersionString())
		return true, nil
	case "help":
		printHelp()
		return true, nil
	case "status":
		return true, cmdStatus(config)
	case "setup":
		return true, cmdSetup(config, logger)
	case "admin":
		return true, cmdAdmin(args[1:], config, logger)
	case "export":
		return true, cmdExport(args[1:], config, logger)
	case "import":
		return true, cmdImport(args[1:], config, logger)
	case "maint":
		return true, cmdMaint(config, logger)
	case "storage-cli":
		return true, cmdStorageCLI(args[1:], config, logger)
	case "storage-migrate":
		return true, cmdStorageMigrate(args[1:], config, logger)
	case "storage-repair":
		return true, cmdStorageRepair(config, logger)
	}
	return true, fmt.Errorf("unknown command %q; use help to list commands", args[0])
}

func printHelp() {
	fmt.Println(`OwlCron CLI

Commands:
  version               Print version
  status                Check if the daemon is running
  setup                 Initialize storage for first run
  admin <user> --password-stdin
                        Create/reset admin account from standard input
  export [file]         Export core data (stdout if no file)
  import <file>         Import data from file
  maint                 Run daily storage maintenance
  storage-cli <action>  Storage operations (get/put/delete/list/list_get)
  storage-migrate <dir> Migrate data to a new storage directory
  storage-repair        Check and repair storage integrity
  help                  Show this help

Usage:
  owlcron-engine --config config.json <command> [args]`)
}

func cmdStatus(config *types.Config) (err error) {
	url := config.BaseAppURL
	if url == "" {
		port := config.HTTPPort
		if port == 0 {
			port = 3012
		}
		url = fmt.Sprintf("http://localhost:%d", port)
	}
	endpoint := strings.TrimRight(url, "/") + "/api/app/ping"
	fmt.Printf("Checking %s ...\n", endpoint)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		return fmt.Errorf("server is not running: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("status response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status check failed: %s", resp.Status)
	}
	var payload struct {
		Code *int `json:"code"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Code == nil || *payload.Code != 0 {
		return fmt.Errorf("status check returned an invalid or unsuccessful Owlcron response")
	}
	fmt.Println("Server is running:", string(body))
	return nil
}

func cmdSetup(config *types.Config, logger *zap.Logger) (err error) {
	store, err := openStorage(config)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer finishStorage(store, &err)
	ctx := context.Background()
	hostname := config.Hostname
	if hostname == "" {
		hostname, _ = os.Hostname()
	}
	seeds := map[string][]interface{}{
		"categories": {map[string]interface{}{"id": "general", "title": "General", "enabled": true}},
		"plugins": {map[string]interface{}{"id": "echoplug", "title": "Echo Plugin", "enabled": true, "command": "bin/echo-plugin/echo-plugin", "params": []map[string]interface{}{
			{"id": "message", "title": "Message", "type": "text", "value": "Hello from Owlcron", "size": 40},
			{"id": "repeat", "title": "Repeat", "type": "text", "value": "3", "size": 8},
			{"id": "delay_ms", "title": "Delay (milliseconds)", "type": "text", "value": "500", "size": 8},
		}}},
		"server_groups": {map[string]interface{}{"id": "all", "title": "All Servers", "regexp": ".", "master": true}},
		"servers":       {map[string]interface{}{"hostname": hostname, "ip": "127.0.0.1"}},
	}
	for key, items := range seeds {
		for _, item := range items {
			m := item.(map[string]interface{})
			recordKey := key + ":" + fmt.Sprint(m["id"])
			if key == "servers" {
				recordKey = key + ":" + hostname
			}
			if err = storage.UpdateList(ctx, store, key, func(existing []interface{}) ([]interface{}, error) {
				if len(existing) > 0 {
					return existing, nil
				}
				return items, nil
			}); err != nil {
				return fmt.Errorf("setup %s index: %w", key, err)
			}
			exists, e := store.Exists(ctx, recordKey)
			if e != nil {
				return fmt.Errorf("check %s: %w", recordKey, e)
			}
			if !exists {
				data, marshalErr := json.Marshal(item)
				if marshalErr != nil {
					return fmt.Errorf("encode %s: %w", recordKey, marshalErr)
				}
				if err = store.Put(ctx, recordKey, data); err != nil {
					return fmt.Errorf("save %s: %w", recordKey, err)
				}
			}
		}
	}
	fmt.Println("Storage initialized. Set OWLCRON_ADMIN_PASSWORD before first startup, or use admin <username> --password-stdin.")
	return nil
}

func cmdAdmin(args []string, config *types.Config, logger *zap.Logger) error {
	return cmdAdminWithInput(args, config, logger, os.Stdin)
}

func cmdAdminWithInput(args []string, config *types.Config, logger *zap.Logger, input io.Reader) (err error) {
	if len(args) != 2 {
		return fmt.Errorf("usage: admin <username> --password-stdin")
	}
	if len(args[0]) < 3 || len(args[0]) > 50 || !regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`).MatchString(args[0]) {
		return fmt.Errorf("invalid username")
	}
	password := args[1]
	if password == "--password-stdin" {
		// A single optional line ending supports secret files without trimming
		// significant spaces or silently accepting multiple input lines.
		data, readErr := io.ReadAll(io.LimitReader(input, 75))
		if readErr != nil {
			return fmt.Errorf("read password from standard input")
		}
		password = strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
		if strings.ContainsAny(password, "\r\n") {
			return fmt.Errorf("password input must contain a single line")
		}
	}
	if len(password) < 12 || len(password) > 72 {
		return fmt.Errorf("password must contain 12-72 bytes")
	}
	eng, err := engine.NewEngine(config, logger)
	if err != nil {
		return fmt.Errorf("open engine storage: %w", err)
	}
	defer finishStorage(eng.GetStorage(), &err)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}
	user, lookupErr := eng.LoadUserByUsernameFromStorage(args[0])
	if lookupErr != nil && !errors.Is(lookupErr, storage.ErrNotFound) {
		return fmt.Errorf("read existing user: %w", lookupErr)
	}
	if user == nil {
		user = &types.User{ID: args[0], Username: args[0]}
	}
	user.Password = string(hash)
	user.Enabled = true
	user.Privileges = types.Privileges{"admin": true}
	if err = eng.SaveUserToStorage(user); err != nil {
		return fmt.Errorf("admin setup: %w", err)
	}
	fmt.Printf("Admin user '%s' created/reset.\n", args[0])
	return nil
}

func closeStorage(store storage.Storage) error {
	if closer, ok := store.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func finishStorage(store storage.Storage, commandErr *error) {
	if err := closeStorage(store); err != nil && *commandErr == nil {
		*commandErr = fmt.Errorf("close storage: %w", err)
	}
}

func writeBackupFile(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".owlcron-backup-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func cmdExport(args []string, config *types.Config, logger *zap.Logger) (err error) {
	store, err := openStorage(config)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer finishStorage(store, &err)
	backup, err := storage.Export(context.Background(), store)
	if err != nil {
		return fmt.Errorf("export: %w", err)
	}
	data, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		return fmt.Errorf("encode backup: %w", err)
	}
	if len(args) > 0 && args[0] != "-" {
		if err = writeBackupFile(args[0], data); err != nil {
			return fmt.Errorf("write backup: %w", err)
		} else {
			fmt.Printf("Exported %d records to %s\n", len(backup.Records), args[0])
		}
	} else {
		if _, err := fmt.Println(string(data)); err != nil {
			return fmt.Errorf("write backup to stdout: %w", err)
		}
	}
	return nil
}
func readBackup(data []byte) (*storage.Backup, error) {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil, fmt.Errorf("backup must be a JSON object")
	}
	var backup storage.Backup
	if err := json.Unmarshal(data, &backup); err != nil {
		return nil, err
	}
	if backup.Format == "owlcron-storage-v1" {
		if backup.Records == nil {
			return nil, fmt.Errorf("backup has no records object")
		}
		return &backup, nil
	}
	if backup.Format != "" {
		return nil, fmt.Errorf("unsupported backup format %q", backup.Format)
	}
	var legacy map[string][]json.RawMessage
	if err := json.Unmarshal(data, &legacy); err != nil {
		return nil, fmt.Errorf("invalid backup: %w", err)
	}
	backup = storage.Backup{Format: "owlcron-storage-v1", Records: map[string]storage.BackupEntry{}}
	for key, items := range legacy {
		for index, item := range items {
			var record map[string]interface{}
			if err := json.Unmarshal(item, &record); err != nil {
				return nil, err
			}
			id, _ := record["id"].(string)
			if key == "servers" {
				id, _ = record["hostname"].(string)
			}
			if key == "users" && id == "" {
				id, _ = record["username"].(string)
				record["id"] = id
				item, _ = json.Marshal(record)
				items[index] = item
			}
			switch key {
			case "users", "api_keys", "events", "plugins", "categories", "server_groups", "servers":
				if id == "" {
					return nil, fmt.Errorf("backup %s record has no identifier", key)
				}
				backup.Records[key+":"+id] = storage.BackupEntry{Data: item}
			}
		}
		raw, err := json.Marshal(items)
		if err != nil {
			return nil, err
		}
		backup.Records[key] = storage.BackupEntry{Data: raw}
	}
	return &backup, nil
}
func cmdImport(args []string, config *types.Config, logger *zap.Logger) (err error) {
	if len(args) < 1 {
		return fmt.Errorf("usage: import <file>")
	}
	data, err := os.ReadFile(args[0])
	if err != nil {
		return fmt.Errorf("read backup: %w", err)
	}
	backup, err := readBackup(data)
	if err != nil {
		return fmt.Errorf("decode backup: %w", err)
	}
	store, err := openStorage(config)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer finishStorage(store, &err)
	if err = storage.Import(context.Background(), store, backup); err != nil {
		return fmt.Errorf("import: %w", err)
	}
	fmt.Printf("Imported %d records.\n", len(backup.Records))
	return nil
}
func cmdMaint(config *types.Config, logger *zap.Logger) (err error) {
	store, err := openStorage(config)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer finishStorage(store, &err)
	if err = storage.UpdateList(context.Background(), store, "logs/activity", func(items []interface{}) ([]interface{}, error) {
		if len(items) > 10000 {
			return items[:10000], nil
		}
		return items, nil
	}); err != nil {
		return fmt.Errorf("maintenance: %w", err)
	}
	fmt.Println("Maintenance complete.")
	return nil
}

func cmdStorageCLI(args []string, config *types.Config, logger *zap.Logger) (err error) {
	if len(args) == 0 {
		return fmt.Errorf(`usage: storage-cli <action> [key] [value]

Actions:
  get <key>           Get a value by key
  put <key> <file>    Put value from file
  delete <key>        Delete a key
  list <key>          List items in a list key
  list_get <key>      Get list with info`)
	}

	store, err := openStorage(config)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer finishStorage(store, &err)
	ctx := context.Background()

	switch args[0] {
	case "get":
		if len(args) < 2 {
			return fmt.Errorf("usage: storage-cli get <key>")
		}
		data, err := store.Get(ctx, args[1])
		if err != nil {
			return fmt.Errorf("get %s: %w", args[1], err)
		}
		fmt.Println(string(data))

	case "put":
		if len(args) < 3 {
			return fmt.Errorf("usage: storage-cli put <key> <file>")
		}
		data, err := os.ReadFile(args[2])
		if err != nil {
			return fmt.Errorf("read value: %w", err)
		}
		if err := store.Put(ctx, args[1], data); err != nil {
			return fmt.Errorf("put %s: %w", args[1], err)
		}
		fmt.Printf("Stored %d bytes at key '%s'\n", len(data), args[1])

	case "delete":
		if len(args) < 2 {
			return fmt.Errorf("usage: storage-cli delete <key>")
		}
		if err := store.Delete(ctx, args[1]); err != nil {
			return fmt.Errorf("delete %s: %w", args[1], err)
		}
		fmt.Printf("Deleted key '%s'\n", args[1])

	case "list", "list_get":
		if len(args) < 2 {
			return fmt.Errorf("usage: storage-cli list <key>")
		}
		info, err := store.ListGetInfo(ctx, args[1])
		if err != nil {
			return fmt.Errorf("inspect list %s: %w", args[1], err)
		}
		fmt.Printf("List '%s': %d items\n", args[1], info.Length)
		items, err := store.ListGet(ctx, args[1], 0, 100)
		if err != nil {
			return fmt.Errorf("read list %s: %w", args[1], err)
		}
		data, err := json.MarshalIndent(items, "", "  ")
		if err != nil {
			return fmt.Errorf("encode list: %w", err)
		}
		fmt.Println(string(data))

	default:
		return fmt.Errorf("unknown storage action %q", args[0])
	}

	return nil
}

func cmdStorageMigrate(args []string, config *types.Config, logger *zap.Logger) (err error) {
	if len(args) < 1 {
		return fmt.Errorf("usage: storage-migrate <target-dir>")
	}
	src, err := openStorage(config)
	if err != nil {
		return fmt.Errorf("open source storage: %w", err)
	}
	defer finishStorage(src, &err)
	dst, err := storage.NewStorage(&storage.StorageConfig{Engine: "Filesystem", BaseDir: args[0]})
	if err != nil {
		return fmt.Errorf("open target storage: %w", err)
	}
	defer finishStorage(dst, &err)
	backup, err := storage.Export(context.Background(), src)
	if err == nil {
		err = storage.Import(context.Background(), dst, backup)
	}
	if err != nil {
		return fmt.Errorf("migration: %w", err)
	}
	fmt.Printf("Migrated %d records.\n", len(backup.Records))
	return nil
}

func cmdStorageRepair(config *types.Config, logger *zap.Logger) (err error) {
	store, err := openStorage(config)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}

	defer finishStorage(store, &err)
	ctx := context.Background()
	lists := []string{"users", "api_keys", "categories", "plugins", "server_groups", "servers", "events", "logs/activity", "logs/completed"}

	repaired := 0
	for _, key := range lists {
		info, err := store.ListGetInfo(ctx, key)
		if err != nil {
			if !errors.Is(err, storage.ErrNotFound) {
				return fmt.Errorf("inspect %s: %w", key, err)
			}
			fmt.Printf("List '%s': does not exist (OK for first run)\n", key)
			continue
		}
		items, err := store.ListGet(ctx, key, 0, 100000)
		if err != nil {
			return fmt.Errorf("read %s: %w", key, err)
		}

		// Verify each item is valid JSON
		valid := make([]interface{}, 0, len(items))
		for _, item := range items {
			raw, err := json.Marshal(item)
			if err != nil {
				repaired++
				continue
			}
			var check interface{}
			if err := json.Unmarshal(raw, &check); err != nil {
				repaired++
				continue
			}
			valid = append(valid, item)
		}

		if len(valid) != int(info.Length) {
			fmt.Printf("List '%s': repaired (%d -> %d items)\n", key, info.Length, len(valid))
			if err := storage.UpdateList(ctx, store, key, func(current []interface{}) ([]interface{}, error) {
				if len(current) != len(items) {
					return nil, fmt.Errorf("list changed while repairing")
				}
				before, _ := json.Marshal(items)
				now, _ := json.Marshal(current)
				if string(before) != string(now) {
					return nil, fmt.Errorf("list changed while repairing")
				}
				return valid, nil
			}); err != nil {
				return fmt.Errorf("repair %s: %w", key, err)
			}
		} else {
			fmt.Printf("List '%s': OK (%d items)\n", key, len(valid))
		}
	}

	if repaired > 0 {
		fmt.Printf("\nRepaired %d invalid entries.\n", repaired)
	} else {
		fmt.Println("\nNo repairs needed.")
	}
	return nil
}

func openStorage(config *types.Config) (storage.Storage, error) {
	eng := config.StorageEngine
	if eng == "" {
		eng = "Filesystem"
	}
	baseDir := config.StorageBaseDir
	if baseDir == "" {
		baseDir = "data"
	}
	return storage.NewStorage(&storage.StorageConfig{
		Engine:  eng,
		BaseDir: baseDir,
		Options: config.StorageOptions,
	})
}
