package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	rand "math/rand/v2"
	"time"

	"github.com/italypaleale/go-kit/servicerunner"
)

type LeaderServiceFactory func(ctx context.Context) (services []servicerunner.Service, cleanup func(), err error)

type LeaderElectionRunner struct {
	appLock          *AppLockService
	serviceFactory   LeaderServiceFactory
	acquireTimeout   time.Duration
	electionInterval time.Duration
	electionJitter   time.Duration
}

func NewLeaderElectionRunner(appLock *AppLockService, serviceFactory LeaderServiceFactory) *LeaderElectionRunner {
	return &LeaderElectionRunner{
		appLock:          appLock,
		serviceFactory:   serviceFactory,
		acquireTimeout:   10 * time.Second,
		electionInterval: 10 * time.Second,
		electionJitter:   5 * time.Second,
	}
}

func (r *LeaderElectionRunner) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		acquired, waitUntil, err := r.tryAcquire(ctx)
		if err != nil {
			slog.WarnContext(ctx, "Failed to acquire application leadership", slog.Any("error", err))
			if !r.waitForNextElection(ctx) {
				return nil
			}
			continue
		}
		if !acquired {
			if !r.waitForNextElection(ctx) {
				return nil
			}
			continue
		}

		if wait := time.Until(waitUntil); wait > 0 {
			slog.InfoContext(ctx, "Waiting for previous application leadership lease to expire", slog.Duration("wait", wait))
			if !sleepContext(ctx, wait) {
				r.releaseLock()
				return nil
			}
		}

		err = r.runLeaderTerm(ctx)
		r.releaseLock()

		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			slog.WarnContext(ctx, "Application leadership term ended", slog.Any("error", err))
		}

		if !r.waitForNextElection(ctx) {
			return nil
		}
	}
}

func (r *LeaderElectionRunner) tryAcquire(ctx context.Context) (acquired bool, waitUntil time.Time, err error) {
	acquireCtx := ctx
	var cancel context.CancelFunc
	if r.acquireTimeout > 0 {
		acquireCtx, cancel = context.WithTimeout(ctx, r.acquireTimeout)
		defer cancel()
	}

	waitUntil, err = r.appLock.Acquire(acquireCtx, false)
	if errors.Is(err, ErrLockUnavailable) {
		return false, time.Time{}, nil
	}
	if err != nil {
		return false, time.Time{}, err
	}

	return true, waitUntil, nil
}

func (r *LeaderElectionRunner) runLeaderTerm(ctx context.Context) error {
	leaderCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	services, cleanup, err := r.serviceFactory(leaderCtx)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return fmt.Errorf("failed to initialize leader services: %w", err)
	}

	slog.InfoContext(ctx, "Became application leader")
	return servicerunner.NewServiceRunner(services...).Run(leaderCtx)
}

func (r *LeaderElectionRunner) releaseLock() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.appLock.Release(ctx); err != nil {
		slog.Warn("Failed to release application leadership lock", slog.Any("error", err))
	}
}

func (r *LeaderElectionRunner) waitForNextElection(ctx context.Context) bool {
	interval := r.electionInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if r.electionJitter > 0 {
		interval += time.Duration(rand.Int64N(int64(r.electionJitter)))
	}

	return sleepContext(ctx, interval)
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
