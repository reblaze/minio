// Copyright (c) 2015-2026 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package notify

import (
	"testing"

	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/event/target"
	xnet "github.com/minio/pkg/v3/net"
	"github.com/rabbitmq/amqp091-go"
)

// T5 (NATS): a config produced by the legacy migration must survive validation
// and round-trip back through the parser unchanged. Before the fix the
// migration wrote an env var name as a config key, so every migrated NATS
// target failed CheckValidKeys on the next config load.
func TestSetNotifyNATSRoundTrip(t *testing.T) {
	addr, err := xnet.ParseHost(testNATSAddr)
	if err != nil {
		t.Fatalf("ParseHost: %v", err)
	}

	args := target.NATSArgs{
		Enable:            true,
		Address:           *addr,
		Subject:           testNATSSubj,
		UserCredentials:   testCredsPath,
		NKeySeed:          testNKeyPath,
		TLSHandshakeFirst: true,
	}

	s := config.Config{config.NotifyNATSSubSys: map[string]config.KVS{}}
	if err := SetNotifyNATS(s, testTargetName, args); err != nil {
		t.Fatalf("SetNotifyNATS: %v", err)
	}

	if err := checkValidNotificationKeysForSubSys(config.NotifyNATSSubSys, s[config.NotifyNATSSubSys]); err != nil {
		t.Fatalf("migrated NATS config must pass key validation, got: %v", err)
	}

	targets, err := GetNotifyNATS(s[config.NotifyNATSSubSys], nil)
	if err != nil {
		t.Fatalf("GetNotifyNATS: %v", err)
	}
	got, ok := targets[testTargetName]
	if !ok {
		t.Fatalf("target %q missing after round trip: %v", testTargetName, targets)
	}
	if got.UserCredentials != args.UserCredentials {
		t.Errorf("UserCredentials = %q, want %q", got.UserCredentials, args.UserCredentials)
	}
	if got.NKeySeed != args.NKeySeed {
		t.Errorf("NKeySeed = %q, want %q", got.NKeySeed, args.NKeySeed)
	}
	if got.TLSHandshakeFirst != args.TLSHandshakeFirst {
		t.Errorf("TLSHandshakeFirst = %v, want %v", got.TLSHandshakeFirst, args.TLSHandshakeFirst)
	}
}

// T5 (AMQP): the migration mapped cfg.Immediate onto the `internal` key and
// dropped cfg.Internal entirely, so a migrated target came back with both
// fields wrong.
func TestSetNotifyAMQPRoundTrip(t *testing.T) {
	uri, err := amqp091.ParseURI(testAMQPURL)
	if err != nil {
		t.Fatalf("ParseURI: %v", err)
	}

	args := target.AMQPArgs{
		Enable:    true,
		URL:       uri,
		Immediate: true,
		Internal:  false,
	}

	s := config.Config{config.NotifyAMQPSubSys: map[string]config.KVS{}}
	if err := SetNotifyAMQP(s, testTargetName, args); err != nil {
		t.Fatalf("SetNotifyAMQP: %v", err)
	}

	if err := checkValidNotificationKeysForSubSys(config.NotifyAMQPSubSys, s[config.NotifyAMQPSubSys]); err != nil {
		t.Fatalf("migrated AMQP config must pass key validation, got: %v", err)
	}

	targets, err := GetNotifyAMQP(s[config.NotifyAMQPSubSys])
	if err != nil {
		t.Fatalf("GetNotifyAMQP: %v", err)
	}
	got, ok := targets[testTargetName]
	if !ok {
		t.Fatalf("target %q missing after round trip: %v", testTargetName, targets)
	}
	if !got.Immediate {
		t.Errorf("Immediate = false, want true")
	}
	if got.Internal {
		t.Errorf("Internal = true, want false (immediate must not be written to the internal key)")
	}
}
