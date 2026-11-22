package worker

import (
	"context"
	"fmt"
	"sync"
)

type Service struct {
	runner   *Runner
	recovery *RecoveryLoop
}

func NewService(
	runner *Runner,
	recovery *RecoveryLoop,
) (*Service, error) {
	if runner == nil || recovery == nil {
		return nil, fmt.Errorf("runner and recovery loop are required")
	}

	return &Service{
		runner:   runner,
		recovery: recovery,
	}, nil
}

// Run starts notification processing and expired-lease recovery
func (s *Service) Run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		s.recovery.Run(ctx)
	}()

	s.runner.Run(ctx)

	wg.Wait()
}
