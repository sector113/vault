package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/hashicorp/vault/helper/testhelpers/docker"
)

func PrepareTestContainer(t *testing.T, version string) (*docker.Runner, func(), string, string) {
	return prepareTestContainer(t, "postgres", "postgres", version, "secret", "POSTGRES_DB=database", true, false, nil)
}

func PrepareTestContainerWithPassword(t *testing.T, version, password string) (*docker.Runner, func(), string, string) {
	return prepareTestContainer(t, "postgres", "postgres", version, password, "POSTGRES_DB=database", true, false, nil)
}

func PrepareTestContainerRepmgr(t *testing.T, name, version string, envVars []string) (*docker.Runner, func(), string, string) {
	return prepareTestContainer(t, name, "bitnami/postgresql-repmgr", version, "secret", "", false, true, envVars)
}

func StopContainer(t *testing.T, ctx context.Context, runner *docker.Runner, containerID string) {
	err := runner.Stop(ctx, containerID)
	if err != nil {
		t.Fatalf("Could not stop docker Postgres: %s", err)
	}
}

func RestartContainer(t *testing.T, ctx context.Context, runner *docker.Runner, containerID string) {
	err := runner.Restart(ctx, containerID)
	if err != nil {
		t.Fatalf("Could not restart docker Postgres: %s", err)
	}
}

func prepareTestContainer(t *testing.T, name, repo, version, password, db string, addSuffix, doNotAutoRemove bool, envVars []string) (*docker.Runner, func(), string, string) {
	if os.Getenv("PG_URL") != "" {
		return nil, func() {}, "", os.Getenv("PG_URL")
	}

	if version == "" {
		version = "11"
	}

	runOpts := docker.RunOptions{
		ContainerName:   name,
		ImageRepo:       repo,
		ImageTag:        version,
		Env:             envVars,
		NetworkID:       "postgres-repmgr-net",
		Ports:           []string{"5432/tcp"},
		DoNotAutoRemove: doNotAutoRemove,
	}
	if repo == "bitnami/postgresql-repmgr" {
		runOpts.NetworkID = os.Getenv("POSTGRES_MULTIHOST_NET")
	}

	runner, err := docker.NewServiceRunner(runOpts)
	if err != nil {
		t.Fatalf("Could not start docker Postgres: %s", err)
	}

	svc, containerID, err := runner.StartService(context.Background(), addSuffix, connectPostgres(password))
	if err != nil {
		t.Fatalf("Could not start docker Postgres: %s", err)
	}

	return runner, svc.Cleanup, svc.Config.URL().String(), containerID
}

func connectPostgres(password string) docker.ServiceAdapter {
	return func(ctx context.Context, host string, port int) (docker.ServiceConfig, error) {
		u := url.URL{
			Scheme:   "postgres",
			User:     url.UserPassword("postgres", password),
			Host:     fmt.Sprintf("%s:%d", host, port),
			Path:     "postgres",
			RawQuery: "sslmode=disable",
		}

		db, err := sql.Open("pgx", u.String())
		if err != nil {
			return nil, err
		}
		defer db.Close()

		err = db.Ping()
		if err != nil {
			return nil, err
		}
		return docker.NewServiceURL(u), nil
	}
}
