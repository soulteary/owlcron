//go:build !linux && !darwin

package engine

import "fmt"

func processIdentity(pid int) (string, error) {
	return "", fmt.Errorf("process identity unsupported on this platform")
}
func processGroupMembers(group int) ([]int, error) {
	return nil, fmt.Errorf("process enumeration unsupported on this platform")
}
func processHasRecoveryToken(pid int, token string) (bool, error) {
	return false, fmt.Errorf("process environment unsupported on this platform")
}
