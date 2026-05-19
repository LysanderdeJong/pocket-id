package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/go-kit/servicerunner"
	"gorm.io/gorm"

	"github.com/pocket-id/pocket-id/backend/internal/common"
	"github.com/pocket-id/pocket-id/backend/internal/instanceid"
	"github.com/pocket-id/pocket-id/backend/internal/job"
	"github.com/pocket-id/pocket-id/backend/internal/service"
	"github.com/pocket-id/pocket-id/backend/internal/storage"
	"github.com/pocket-id/pocket-id/backend/internal/utils"
)

func Bootstrap(ctx context.Context) error {
	const (
		appConfigReloadInterval      = 30 * time.Second
		databaseVersionCheckInterval = 10 * time.Second
	)

	// List of services to run
	services := make([]servicerunner.Service, 0, 6)
	shutdowns := &shutdownManager{
		fns: make([]servicerunner.Service, 0, 4),
	}

	// Initialize the observability stack, including the logger, distributed tracing, and metrics
	shutdownFns, httpClient, err := initObservability(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize OpenTelemetry: %w", err)
	}
	shutdowns.Add(shutdownFns...)

	slog.InfoContext(ctx, "Pocket ID is starting")

	// Init database
	db, pg, err := NewDatabase(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}
	if pg != nil {
		defer func() {
			// Close the database connection pool only after the shutdown functions have run: some of them (e.g. releasing the application lock) still need to query the database.
			pg.Close()
		}()
	}

	sqlDb, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}

	// Load the instance ID
	// This is stored in the "kv" table, and generated on first startup
	instanceID, err := instanceid.Load(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to initialize instance ID: %w", err)
	}

	// Init storage
	fileStorage, err := InitStorage(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to initialize file storage (backend: %s): %w", common.EnvConfig.FileBackend, err)
	}

	// Init application images
	imageExtensions, err := initApplicationImages(ctx, fileStorage)
	if err != nil {
		return fmt.Errorf("failed to initialize application images: %w", err)
	}

	scimScheduler := job.NewSwitchableScheduler()

	// Init the actors
	// The actor host is created and started before the services, so services can depend on it once it's ready
	actorsOpts := NewActorsOpts{
		Postgres: pg,

		EnvConfig:   &common.EnvConfig,
		InstanceID:  instanceID,
		HttpClient:  httpClient,
		DB:          db,
		FileStorage: fileStorage,
	}
	if pg == nil {
		actorsOpts.SQLite, err = db.DB()
		if err != nil {
			return fmt.Errorf("failed to get *sql.DB connection from Gorm: %w", err)
		}
	}
	actors, rateLimitServices, err := NewActors(actorsOpts)
	if err != nil {
		return fmt.Errorf("failed to initialize actors: %w", err)
	}

	// Run the actor host as a background service and get a "ready" signal that other services can wait on
	actorsRun, actorsReady := actorsRunServiceFn(actors)
	services = append(services, actorsRun)

	// Create all services
	svc, err := initServices(ctx, db, instanceID, httpClient, imageExtensions, fileStorage, scimScheduler)
	if err != nil {
		return fmt.Errorf("failed to initialize services: %w", err)
	}
	// Init the router
	// The rate-limit middleware invokes the actor host with each request's own context, so the setup context is intentionally not threaded through the router
	//nolint:contextcheck
	router, err := initRouter(db, svc, rateLimitServices)
	if err != nil {
		return fmt.Errorf("failed to initialize router: %w", err)
	}

	leaderRunner := service.NewLeaderElectionRunner(svc.appLockService, func(leaderCtx context.Context) ([]servicerunner.Service, func(), error) {
		scheduler, err := job.NewScheduler()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create job scheduler: %w", err)
		}

		scimScheduler.SetActive(scheduler)
		cleanup := func() {
			scimScheduler.ClearActive(scheduler)
		}

		if err := registerScheduledJobs(leaderCtx, db, svc, scheduler); err != nil {
			cleanup()
			return nil, nil, err
		}

		leaderServices := []servicerunner.Service{svc.appLockService.RunRenewal}
		if common.EnvConfig.AppEnv != "test" || len(registerTestControllers) > 0 {
			leaderServices = append(leaderServices, scheduler.Run)
		}

		return leaderServices, cleanup, nil
	})

	// The router must wait on the actor host being ready, since the rate-limit middleware invokes actors
	services = append(services,
		actorsReady.Await(router),
		leaderRunner.Run,
		svc.appConfigService.RunReloadLoop(appConfigReloadInterval),
		utils.RunDatabaseVersionMonitor(sqlDb, databaseVersionCheckInterval),
	)

	// Run all background services
	// This call blocks until the context is canceled
	err = servicerunner.NewServiceRunner(services...).Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run services: %w", err)
	}

	// Run all shutdown functions
	shutdowns.Run(ctx)

	return nil
}

// actorsRunServiceFn wraps the actor host's Run method in a background service and returns a "ready" signal that other services can wait on
func actorsRunServiceFn(actors *local.Host) (servicerunner.Service, *servicerunner.Ready) {
	actorsReady := servicerunner.NewReady()
	fn := func(ctx context.Context) error {
		runErrCh := make(chan error, 1)
		go func() {
			runErrCh <- actors.Run(ctx)
		}()

		// Wait for the right signal
		select {
		case <-actors.Ready():
			// Actor host is ready, signal actorsReady
			actorsReady.Signal()
		case runErr := <-runErrCh:
			// Run returned with an error
			return runErr
		case <-ctx.Done():
			// Context canceled
			return ctx.Err()
		}

		// Now the actor host is running
		// This goroutine must stay up until the actor host returns
		// Here, context cancellation will surface through this channel too
		return <-runErrCh
	}

	return fn, actorsReady
}

func InitStorage(ctx context.Context, db *gorm.DB) (fileStorage storage.FileStorage, err error) {
	switch common.EnvConfig.FileBackend {
	case storage.TypeFileSystem:
		fileStorage, err = storage.NewFilesystemStorage(common.EnvConfig.UploadPath)
	case storage.TypeDatabase:
		fileStorage, err = storage.NewDatabaseStorage(db)
	case storage.TypeS3:
		s3Cfg := storage.S3Config{
			Bucket:                        common.EnvConfig.S3Bucket,
			Region:                        common.EnvConfig.S3Region,
			Endpoint:                      common.EnvConfig.S3Endpoint,
			AccessKeyID:                   common.EnvConfig.S3AccessKeyID,
			SecretAccessKey:               common.EnvConfig.S3SecretAccessKey,
			ForcePathStyle:                common.EnvConfig.S3ForcePathStyle,
			DisableDefaultIntegrityChecks: common.EnvConfig.S3DisableDefaultIntegrityChecks,
			Root:                          common.EnvConfig.UploadPath,
		}
		fileStorage, err = storage.NewS3Storage(ctx, s3Cfg)
	default:
		err = fmt.Errorf("unknown file storage backend: %s", common.EnvConfig.FileBackend)
	}
	if err != nil {
		return fileStorage, err
	}

	return fileStorage, nil
}

type shutdownManager struct {
	fns []servicerunner.Service
}

func (s *shutdownManager) Add(fns ...servicerunner.Service) {
	for _, fn := range fns {
		if fn == nil {
			continue
		}

		s.fns = append(s.fns, fn)
	}
}

func (s *shutdownManager) Run(ctx context.Context) {
	// Cleanup functions are one-shot and must each run to completion independently, so we set WaitAll to true
	sr := servicerunner.NewServiceRunner(s.fns...)
	sr.WaitAll = true

	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()
	err := sr.Run(shutdownCtx)
	if err != nil {
		slog.ErrorContext(ctx, "Error shutting down services", slog.Any("error", err))
	}
}
