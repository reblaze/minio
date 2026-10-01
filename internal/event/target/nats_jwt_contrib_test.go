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

package target

import (
	"os"
	"path/filepath"
	"testing"

	xnet "github.com/minio/pkg/v3/net"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nkeys"
)

// runNATSOperatorServer starts a NATS server in operator (JWT) mode and
// returns its port, a user JWT and the user's nkey seed.
func runNATSOperatorServer(t *testing.T, port int) (userJWT string, userSeed []byte) {
	t.Helper()

	operatorKP, _ := nkeys.CreateOperator()
	operatorPub, _ := operatorKP.PublicKey()
	operatorJWT, err := jwt.NewOperatorClaims(operatorPub).Encode(operatorKP)
	if err != nil {
		t.Fatal(err)
	}
	operatorClaims, err := jwt.DecodeOperatorClaims(operatorJWT)
	if err != nil {
		t.Fatal(err)
	}

	accountKP, _ := nkeys.CreateAccount()
	accountPub, _ := accountKP.PublicKey()
	accountJWT, err := jwt.NewAccountClaims(accountPub).Encode(operatorKP)
	if err != nil {
		t.Fatal(err)
	}

	userKP, _ := nkeys.CreateUser()
	userPub, _ := userKP.PublicKey()
	userClaims := jwt.NewUserClaims(userPub)
	userClaims.Name = "minio"
	userJWT, err = userClaims.Encode(accountKP)
	if err != nil {
		t.Fatal(err)
	}
	userSeed, _ = userKP.Seed()

	resolver := &server.MemAccResolver{}
	if err := resolver.Store(accountPub, accountJWT); err != nil {
		t.Fatal(err)
	}
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.TrustedOperators = []*jwt.OperatorClaims{operatorClaims}
	opts.AccountResolver = resolver
	s := natsserver.RunServer(&opts)
	t.Cleanup(s.Shutdown)

	return userJWT, userSeed
}

func natsTestArgs(port int) NATSArgs {
	return NATSArgs{
		Enable:  true,
		Address: xnet.Host{Name: "localhost", Port: xnet.Port(port), IsPortSet: true},
		Subject: "test",
	}
}

func TestNatsConnUserCredentialsJWT(t *testing.T) {
	const port = 14240
	userJWT, userSeed := runNATSOperatorServer(t, port)
	dir := t.TempDir()

	creds, err := jwt.FormatUserConfig(userJWT, userSeed)
	if err != nil {
		t.Fatal(err)
	}
	credsFile := filepath.Join(dir, "user.creds")
	jwtFile := filepath.Join(dir, "user.jwt")
	seedFile := filepath.Join(dir, "user.nk")
	for file, data := range map[string][]byte{credsFile: creds, jwtFile: []byte(userJWT), seedFile: userSeed} {
		if err := os.WriteFile(file, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	testCases := map[string]func(*NATSArgs){
		"chained credentials file": func(a *NATSArgs) { a.UserCredentials = credsFile },
		"JWT file with nkey seed":  func(a *NATSArgs) { a.UserCredentials = jwtFile; a.NKeySeed = seedFile },
		"credentials file with nkey seed": func(a *NATSArgs) {
			a.UserCredentials = credsFile
			a.NKeySeed = seedFile
		},
	}
	for name, setup := range testCases {
		t.Run(name, func(t *testing.T) {
			args := natsTestArgs(port)
			setup(&args)
			conn, err := args.connectNats()
			if err != nil {
				t.Fatalf("could not connect to nats: %v", err)
			}
			defer conn.Close()
			if err := conn.Publish("test", []byte("event")); err != nil {
				t.Fatal(err)
			}
			if err := conn.Flush(); err != nil {
				t.Fatal(err)
			}
		})
	}

	// A JWT without a seed to sign the server nonce must not connect.
	args := natsTestArgs(port)
	args.UserCredentials = jwtFile
	if conn, err := args.connectNats(); err == nil {
		conn.Close()
		t.Fatal("expected a JWT without nkey seed to fail")
	}
}
