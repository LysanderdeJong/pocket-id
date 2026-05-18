package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pocket-id/pocket-id/backend/internal/utils"
	testutils "github.com/pocket-id/pocket-id/backend/internal/utils/testing"
)

func TestLeaderElectionRunner(t *testing.T) {
	t.Run("standby does not run leader services while lock is held", func(t *testing.T) {
		db := testutils.NewDatabaseForTest(t)
		leaderLock := NewAppLockService(db)
		standbyLock := NewAppLockService(db)

		_, err := leaderLock.Acquire(t.Context(), false)
		require.NoError(t, err)

		called := make(chan struct{})
		runner := newTestLeaderElectionRunner(standbyLock, func(ctx context.Context) ([]utils.Service, func(), error) {
			close(called)
			return nil, nil, nil
		})

		ctx, cancel := context.WithCancel(t.Context())
		errCh := make(chan error, 1)
		go func() {
			errCh <- runner.Run(ctx)
		}()

		select {
		case <-called:
			t.Fatal("standby ran leader services while the lock was held")
		case <-time.After(75 * time.Millisecond):
		}

		cancel()
		require.NoError(t, <-errCh)
		require.NoError(t, leaderLock.Release(t.Context()))
	})

	t.Run("standby becomes leader after current leader releases lock", func(t *testing.T) {
		db := testutils.NewDatabaseForTest(t)
		leaderLock := NewAppLockService(db)
		standbyLock := NewAppLockService(db)

		leaderStarted := make(chan struct{})
		standbyStarted := make(chan struct{})
		var leaderOnce sync.Once
		var standbyOnce sync.Once

		leaderRunner := newTestLeaderElectionRunner(leaderLock, func(ctx context.Context) ([]utils.Service, func(), error) {
			return []utils.Service{func(ctx context.Context) error {
				leaderOnce.Do(func() { close(leaderStarted) })
				<-ctx.Done()
				return ctx.Err()
			}}, nil, nil
		})
		standbyRunner := newTestLeaderElectionRunner(standbyLock, func(ctx context.Context) ([]utils.Service, func(), error) {
			return []utils.Service{func(ctx context.Context) error {
				standbyOnce.Do(func() { close(standbyStarted) })
				<-ctx.Done()
				return ctx.Err()
			}}, nil, nil
		})

		leaderCtx, leaderCancel := context.WithCancel(t.Context())
		standbyCtx, standbyCancel := context.WithCancel(t.Context())
		defer standbyCancel()

		leaderErrCh := make(chan error, 1)
		standbyErrCh := make(chan error, 1)
		go func() { leaderErrCh <- leaderRunner.Run(leaderCtx) }()

		require.Eventually(t, func() bool {
			select {
			case <-leaderStarted:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond)

		go func() { standbyErrCh <- standbyRunner.Run(standbyCtx) }()

		leaderCancel()
		require.NoError(t, <-leaderErrCh)

		require.Eventually(t, func() bool {
			select {
			case <-standbyStarted:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond)

		standbyCancel()
		require.NoError(t, <-standbyErrCh)
	})
}

func newTestLeaderElectionRunner(appLock *AppLockService, factory LeaderServiceFactory) *LeaderElectionRunner {
	runner := NewLeaderElectionRunner(appLock, factory)
	runner.acquireTimeout = 100 * time.Millisecond
	runner.electionInterval = 10 * time.Millisecond
	runner.electionJitter = 0
	return runner
}
