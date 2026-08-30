package nvidia

import (
	"fmt"
	"time"

	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
)

type Option func(*config) error

type config struct {
	driver       Driver
	now          func() time.Time
	retention    int
	probeTimeout time.Duration
}

// WithProbeTimeout bounds initial hardware discovery.
func WithProbeTimeout(timeout time.Duration) Option {
	return func(cfg *config) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: probe timeout must be positive", base.ErrInvalidArgument)
		}
		cfg.probeTimeout = timeout
		return nil
	}
}

// WithDriverV2 configures a composed v2 driver without changing the Provider API.
func WithDriverV2(options LocalDriverV2Options) Option {
	return func(cfg *config) error {
		driver, err := NewLocalDriverV2(options)
		if err != nil {
			return err
		}
		cfg.driver = driver
		return nil
	}
}

func WithDriver(driver Driver) Option {
	return func(cfg *config) error {
		if driver == nil {
			return fmt.Errorf("%w: driver is required", base.ErrInvalidArgument)
		}
		cfg.driver = driver
		return nil
	}
}

func WithNow(now func() time.Time) Option {
	return func(cfg *config) error {
		if now == nil {
			return fmt.Errorf("%w: clock is required", base.ErrInvalidArgument)
		}
		cfg.now = now
		return nil
	}
}

func WithEventRetention(retention int) Option {
	return func(cfg *config) error {
		if retention <= 0 {
			return fmt.Errorf("%w: retention must be positive", base.ErrInvalidArgument)
		}
		cfg.retention = retention
		return nil
	}
}
