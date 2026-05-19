package job

import (
	"context"
	"testing"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/stretchr/testify/require"

	"github.com/pocket-id/pocket-id/backend/internal/service"
)

func TestSwitchableScheduler(t *testing.T) {
	t.Run("stores jobs until scheduler becomes active", func(t *testing.T) {
		scheduler := NewSwitchableScheduler()
		err := scheduler.RegisterJob(t.Context(), "test", gocron.DurationJob(time.Hour), func(ctx context.Context) error { return nil }, service.RegisterJobOpts{})
		require.NoError(t, err)

		active := &fakeScheduler{}
		scheduler.SetActive(active)
		require.Equal(t, []string{"test"}, active.registered)
	})

	t.Run("remove deletes pending job before activation", func(t *testing.T) {
		scheduler := NewSwitchableScheduler()
		require.NoError(t, scheduler.RegisterJob(t.Context(), "test", gocron.DurationJob(time.Hour), func(ctx context.Context) error { return nil }, service.RegisterJobOpts{}))
		require.NoError(t, scheduler.RemoveJob("test"))

		active := &fakeScheduler{}
		scheduler.SetActive(active)
		require.Empty(t, active.registered)
	})

	t.Run("replays pending jobs after original request context is canceled", func(t *testing.T) {
		scheduler := NewSwitchableScheduler()
		ctx, cancel := context.WithCancel(t.Context())
		require.NoError(t, scheduler.RegisterJob(ctx, "test", gocron.DurationJob(time.Hour), func(ctx context.Context) error { return nil }, service.RegisterJobOpts{}))
		cancel()

		active := &fakeScheduler{}
		scheduler.SetActive(active)
		require.Equal(t, []string{"test"}, active.registered)
		require.NoError(t, active.contextErr)
	})
}

type fakeScheduler struct {
	registered []string
	removed    []string
	contextErr error
}

func (s *fakeScheduler) RegisterJob(ctx context.Context, name string, def gocron.JobDefinition, job func(ctx context.Context) error, opts service.RegisterJobOpts) error {
	s.registered = append(s.registered, name)
	s.contextErr = ctx.Err()
	return nil
}

func (s *fakeScheduler) RemoveJob(name string) error {
	s.removed = append(s.removed, name)
	return nil
}
