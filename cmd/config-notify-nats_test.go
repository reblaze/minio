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

package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/notify"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/event/target"
)

func natsTestKVS(subject, queueDir string) config.KVS {
	kvs := notify.DefaultNATSKVS.Clone()
	kvs.Set(config.Enable, config.EnableOn)
	kvs.Set(target.NATSAddress, "127.0.0.1:4222")
	kvs.Set(target.NATSSubject, subject)
	kvs.Set(target.NATSQueueDir, queueDir)
	return kvs
}

func natsTestConfig(targets map[string]config.KVS) config.Config {
	cfg := config.New()
	for id, kvs := range targets {
		cfg[config.NotifyNATSSubSys][id] = kvs
	}
	return cfg
}

// setupNotifyNATSTest resets the notification globals and simulates the
// startup sequence of lookupConfigs: create the targets, then apply the
// dynamic config. It returns the targets created at startup.
func setupNotifyNATSTest(t *testing.T, cfg config.Config) *event.TargetList {
	t.Helper()

	oldNotifier, oldList, oldArgs, oldRegistered := globalEventNotifier, globalNotifyTargetList, lastNotifyNATSArgs, bootNotifyTargetsRegistered
	t.Cleanup(func() {
		for _, tgt := range globalEventNotifier.Targets() {
			tgt.Close()
		}
		globalEventNotifier, globalNotifyTargetList, lastNotifyNATSArgs, bootNotifyTargetsRegistered = oldNotifier, oldList, oldArgs, oldRegistered
	})

	ctx := t.Context()
	globalEventNotifier = NewEventNotifier(ctx)
	globalNotifyTargetList, lastNotifyNATSArgs, bootNotifyTargetsRegistered = nil, nil, false

	initNotifyTargets(ctx, cfg, NewHTTPTransport())
	if err := reloadNotifyNATSTargets(ctx, cfg); err != nil {
		t.Fatalf("dynamic config apply at startup failed: %v", err)
	}
	return globalNotifyTargetList
}

func natsTargets(t *testing.T) map[string]event.Target {
	t.Helper()
	targets := make(map[string]event.Target)
	for id, tgt := range globalEventNotifier.targetList.TargetMap() {
		if id.Name != natsTargetName {
			t.Fatalf("unexpected target %v", id)
		}
		targets[id.ID] = tgt
	}
	return targets
}

func TestReloadNotifyNATSTargets(t *testing.T) {
	ctx := t.Context()
	cfg := natsTestConfig(map[string]config.KVS{
		"a": natsTestKVS("subject-a", ""),
		"b": natsTestKVS("subject-b", ""),
	})
	bootTargets := setupNotifyNATSTest(t, cfg)

	// Startup must not create the targets a second time, and registering
	// the startup targets must not fail with "target already exists".
	if n := len(globalEventNotifier.targetList.TargetMap()); n != 0 {
		t.Fatalf("dynamic config apply at startup registered %d duplicate targets", n)
	}
	if err := registerBootNotifyTargets(globalEventNotifier.targetList, bootTargets.Targets()); err != nil {
		t.Fatal(err)
	}
	before := natsTargets(t)
	if len(before) != 2 {
		t.Fatalf("expected 2 targets, got %v", before)
	}
	for id, tgt := range bootTargets.TargetMap() {
		if before[id.ID] != tgt {
			t.Fatalf("target %v is not the instance created at startup", id)
		}
	}

	// Unchanged configuration must leave the running targets untouched.
	if err := reloadNotifyNATSTargets(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	after := natsTargets(t)
	if len(after) != 2 || after["a"] != before["a"] || after["b"] != before["b"] {
		t.Fatal("unchanged configuration replaced running targets")
	}

	// Changing "a" and adding "c" must only replace "a" and add "c".
	cfg = natsTestConfig(map[string]config.KVS{
		"a": natsTestKVS("subject-a2", ""),
		"b": natsTestKVS("subject-b", ""),
		"c": natsTestKVS("subject-c", ""),
	})
	if err := reloadNotifyNATSTargets(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	after = natsTargets(t)
	if len(after) != 3 || after["a"] == before["a"] || after["b"] != before["b"] || after["c"] == nil {
		t.Fatalf("unexpected targets after changing a and adding c: %v", after)
	}
	before = after

	// Disabling "b" must only remove "b".
	disabledKVS := natsTestKVS("subject-b", "")
	disabledKVS.Set(config.Enable, config.EnableOff)
	cfg[config.NotifyNATSSubSys]["b"] = disabledKVS
	if err := reloadNotifyNATSTargets(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	after = natsTargets(t)
	if len(after) != 2 || after["a"] != before["a"] || after["c"] != before["c"] {
		t.Fatalf("unexpected targets after disabling b: %v", after)
	}
	before = after

	// A target that cannot be created must keep its running instance and
	// must be retried on the next reload.
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg[config.NotifyNATSSubSys]["a"] = natsTestKVS("subject-a3", file)
	for range 2 {
		if err := reloadNotifyNATSTargets(ctx, cfg); err == nil {
			t.Fatal("expected an error for an invalid queue_dir")
		}
		after = natsTargets(t)
		if len(after) != 2 || after["a"] != before["a"] || after["c"] != before["c"] {
			t.Fatalf("failed reload changed running targets: %v", after)
		}
	}
	cfg[config.NotifyNATSSubSys]["a"] = natsTestKVS("subject-a3", t.TempDir())
	if err := reloadNotifyNATSTargets(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	after = natsTargets(t)
	if len(after) != 2 || after["a"] == before["a"] || after["c"] != before["c"] {
		t.Fatalf("fixed configuration was not applied: %v", after)
	}

	// Removing everything must remove all NATS targets.
	if err := reloadNotifyNATSTargets(ctx, natsTestConfig(nil)); err != nil {
		t.Fatal(err)
	}
	if after = natsTargets(t); len(after) != 0 {
		t.Fatalf("expected no targets, got %v", after)
	}
}

func TestRegisterBootNotifyTargetsAfterReload(t *testing.T) {
	ctx := t.Context()
	bootTargets := setupNotifyNATSTest(t, natsTestConfig(map[string]config.KVS{
		"a": natsTestKVS("subject-a", ""),
		"b": natsTestKVS("subject-b", ""),
		"c": natsTestKVS("subject-c", ""),
	}))

	// A reload that happens before the startup targets are registered:
	// "a" changes, "b" is removed and "c" is unchanged.
	cfg := natsTestConfig(map[string]config.KVS{
		"a": natsTestKVS("subject-a2", ""),
		"c": natsTestKVS("subject-c", ""),
	})
	if err := reloadNotifyNATSTargets(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	reloadedA := natsTargets(t)["a"]

	if err := registerBootNotifyTargets(globalEventNotifier.targetList, bootTargets.Targets()); err != nil {
		t.Fatal(err)
	}
	targets := natsTargets(t)
	bootC := bootTargets.TargetMap()[event.TargetID{ID: "c", Name: natsTargetName}]
	if len(targets) != 2 || targets["a"] != reloadedA || targets["c"] != bootC {
		t.Fatalf("unexpected targets: %v", targets)
	}
}

func TestReloadNotifyNATSTargetsStartupFailure(t *testing.T) {
	cfg := natsTestConfig(map[string]config.KVS{
		"a": natsTestKVS("subject-a", ""),
	})
	// Make creating the startup targets fail on another sub-system.
	webhookKVS := notify.DefaultWebhookKVS.Clone()
	webhookKVS.Set(config.Enable, config.EnableOn)
	webhookKVS.Set(target.WebhookEndpoint, "http://127.0.0.1:1")
	webhookKVS.Set(target.WebhookQueueDir, "relative/dir")
	cfg[config.NotifyWebhookSubSys]["w"] = webhookKVS
	bootTargets := setupNotifyNATSTest(t, cfg)
	if bootTargets != nil {
		t.Fatal("expected creating the startup targets to fail")
	}

	// The dynamic config apply must still create the NATS targets.
	if err := registerBootNotifyTargets(globalEventNotifier.targetList, bootTargets.Targets()); err != nil {
		t.Fatal(err)
	}
	if targets := natsTargets(t); len(targets) != 1 || targets["a"] == nil {
		t.Fatalf("unexpected targets: %v", targets)
	}
}

func TestInitNotifyTargetsAfterRegistration(t *testing.T) {
	cfg := natsTestConfig(map[string]config.KVS{
		"a": natsTestKVS("subject-a", ""),
	})
	setupNotifyNATSTest(t, cfg)

	// Before registration, a repeated startup replaces the startup targets.
	first := globalNotifyTargetList
	initNotifyTargets(t.Context(), cfg, NewHTTPTransport())
	if globalNotifyTargetList == first {
		t.Fatal("expected new startup targets before registration")
	}

	if err := registerBootNotifyTargets(globalEventNotifier.targetList, globalNotifyTargetList.Targets()); err != nil {
		t.Fatal(err)
	}
	registered := globalNotifyTargetList
	running := natsTargets(t)["a"]

	// After registration, lookupConfigs must not create targets again.
	initNotifyTargets(t.Context(), cfg, NewHTTPTransport())
	if globalNotifyTargetList != registered || natsTargets(t)["a"] != running {
		t.Fatal("notification targets were created again after registration")
	}
}

func TestNotifyNATSUserCredentials(t *testing.T) {
	kvs := natsTestKVS("subject-a", "")
	kvs.Set(target.NATSUserCredentials, "/etc/nats/a.creds")
	args, err := notify.GetNotifyNATS(map[string]config.KVS{"a": kvs}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := args["a"].UserCredentials; got != "/etc/nats/a.creds" {
		t.Fatalf("expected user credentials from config, got %q", got)
	}

	t.Setenv(target.EnvNATSUserCredentials+config.Default+"a", "/env/a.creds")
	args, err = notify.GetNotifyNATS(map[string]config.KVS{"a": kvs}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := args["a"].UserCredentials; got != "/env/a.creds" {
		t.Fatalf("expected user credentials from environment, got %q", got)
	}
}
