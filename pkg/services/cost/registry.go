package cost

import (
	"fmt"
	"sync"
)

// ControllerFactory is a function type that creates a Controller from a config path
type ControllerFactory func(configPath string) (Controller, error)

// Registry manages platform controller factories
type Registry interface {
	// Register adds a new platform controller factory
	Register(platform string, factory ControllerFactory) error
	// Create instantiates a controller for the specified platform using the provided config
	Create(platform, configPath string) (Controller, error)
	// ListPlatforms returns a list of registered platforms
	ListPlatforms() []string
}

type registry struct {
	mu        sync.RWMutex
	factories map[string]ControllerFactory
}

// NewRegistry creates a new controller registry
func NewRegistry() Registry {
	return &registry{
		factories: make(map[string]ControllerFactory),
	}
}

func (r *registry) Register(platform string, factory ControllerFactory) error {
	if platform == "" {
		return fmt.Errorf("platform name cannot be empty")
	}
	if factory == nil {
		return fmt.Errorf("factory cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.factories[platform]; exists {
		return fmt.Errorf("platform %q is already registered", platform)
	}

	r.factories[platform] = factory
	return nil
}

func (r *registry) Create(platform, configPath string) (Controller, error) {
	r.mu.RLock()
	factory, exists := r.factories[platform]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("platform %q is not registered", platform)
	}

	return factory(configPath)
}

func (r *registry) ListPlatforms() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	platforms := make([]string, 0, len(r.factories))
	for platform := range r.factories {
		platforms = append(platforms, platform)
	}
	return platforms
}
