package job

import (
	"context"
	"log/slog"
	"sync"

	"github.com/go-co-op/gocron/v2"

	"github.com/pocket-id/pocket-id/backend/internal/service"
)

type SwitchableScheduler struct {
	mu      sync.RWMutex
	active  service.Scheduler
	pending map[string]pendingJob
}

type pendingJob struct {
	ctx   context.Context
	name  string
	def   gocron.JobDefinition
	jobFn func(ctx context.Context) error
	opts  service.RegisterJobOpts
}

func NewSwitchableScheduler() *SwitchableScheduler {
	return &SwitchableScheduler{pending: make(map[string]pendingJob)}
}

func (s *SwitchableScheduler) SetActive(active service.Scheduler) {
	s.mu.Lock()
	s.active = active
	pending := make([]pendingJob, 0, len(s.pending))
	for _, job := range s.pending {
		pending = append(pending, job)
	}
	s.pending = make(map[string]pendingJob)
	s.mu.Unlock()

	for _, job := range pending {
		if err := active.RegisterJob(job.ctx, job.name, job.def, job.jobFn, job.opts); err != nil {
			slog.WarnContext(job.ctx, "Failed to register pending job", slog.String("name", job.name), slog.Any("error", err))
		}
	}
}

func (s *SwitchableScheduler) ClearActive(active service.Scheduler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == active {
		s.active = nil
	}
}

func (s *SwitchableScheduler) RegisterJob(ctx context.Context, name string, def gocron.JobDefinition, jobFn func(ctx context.Context) error, opts service.RegisterJobOpts) error {
	active := s.getActiveOrStorePending(pendingJob{ctx: ctx, name: name, def: def, jobFn: jobFn, opts: opts})
	if active == nil {
		return nil
	}

	return active.RegisterJob(ctx, name, def, jobFn, opts)
}

func (s *SwitchableScheduler) RemoveJob(name string) error {
	active := s.getActiveAndRemovePending(name)
	if active == nil {
		return nil
	}

	return active.RemoveJob(name)
}

func (s *SwitchableScheduler) getActiveOrStorePending(job pendingJob) service.Scheduler {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		s.pending[job.name] = job
	}
	return s.active
}

func (s *SwitchableScheduler) getActiveAndRemovePending(name string) service.Scheduler {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, name)
	return s.active
}

var _ service.Scheduler = (*SwitchableScheduler)(nil)
