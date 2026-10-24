package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

type failedPasswordReader struct{}

func (failedPasswordReader) Read([]byte) (int, error) {
	return 0, errors.New("sensitive underlying input error")
}

func TestAdminPasswordFromStdin(t *testing.T) {
	config := &types.Config{StorageBaseDir: t.TempDir(), LogDir: t.TempDir(), QueueDir: t.TempDir(), SecretKey: "test-secret-0123456789abcdef0123456789"}
	password := " protected password "
	if err := cmdAdminWithInput([]string{"admin", "--password-stdin"}, config, zap.NewNop(), strings.NewReader(password+"\r\n")); err != nil {
		t.Fatal(err)
	}
	eng, err := engine.NewEngine(config, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer closeStorage(eng.GetStorage())
	user, err := eng.LoadUserByUsernameFromStorage("admin")
	if err != nil || !eng.VerifyUserPassword(user, password) {
		t.Fatalf("password was not preserved: %v", err)
	}
	for _, input := range []string{"", "short", strings.Repeat("x", 73), strings.Repeat("x", 74) + "\n", "long-enough-first\nsecond"} {
		if err := cmdAdminWithInput([]string{"admin", "--password-stdin"}, config, zap.NewNop(), strings.NewReader(input)); err == nil {
			t.Fatal("invalid password accepted")
		}
	}
	if err := cmdAdminWithInput([]string{"admin", "--password-stdin"}, config, zap.NewNop(), failedPasswordReader{}); err == nil || strings.Contains(err.Error(), "sensitive") {
		t.Fatal("input error was exposed or ignored")
	}
}
