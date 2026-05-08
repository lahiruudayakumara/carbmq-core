package services

import (
	"errors"
	"regexp"
	"strings"
)

var deviceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

func ValidateDeviceID(deviceID string) error {
	trimmed := strings.TrimSpace(deviceID)
	if trimmed == "" {
		return errors.New("device id is required")
	}
	if !deviceIDPattern.MatchString(trimmed) {
		return errors.New("device id must be 1-64 chars and only contain letters, numbers, hyphens, or underscores")
	}

	return nil
}
