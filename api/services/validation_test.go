package services

import "testing"

func TestValidateDeviceID(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		deviceID string
		wantErr  bool
	}{
		{name: "valid simple id", deviceID: "device-001"},
		{name: "valid underscore", deviceID: "device_001"},
		{name: "empty", deviceID: "", wantErr: true},
		{name: "leading whitespace only", deviceID: "   ", wantErr: true},
		{name: "spaces are invalid", deviceID: "device 001", wantErr: true},
		{name: "slashes are invalid", deviceID: "device/001", wantErr: true},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateDeviceID(testCase.deviceID)
			if testCase.wantErr && err == nil {
				t.Fatalf("expected error for %q", testCase.deviceID)
			}
			if !testCase.wantErr && err != nil {
				t.Fatalf("did not expect error for %q: %v", testCase.deviceID, err)
			}
		})
	}
}
