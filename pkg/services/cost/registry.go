package cost

import (
	"fmt"
)

// ControllerFactory is a function type that creates a Controller from a config path
type ControllerFactory func(configPath string) (Controller, error)

// Registry manages platform controller factories
type Registry interface {
	// Create instantiates a controller for the specified platform using the provided config
	Create(platform, configPath string) (Controller, error)
	// ListPlatforms returns a list of registered platforms
	ListPlatforms() []string
}

type registry struct {
	factories map[string]ControllerFactory
}

// NewRegistry creates a new controller registry
func NewRegistry(factories map[string]ControllerFactory) Registry {
	return &registry{
		factories: factories,
	}
}

func (r *registry) Create(platform, configPath string) (Controller, error) {
	factory, exists := r.factories[platform]

	if !exists {
		return nil, fmt.Errorf("platform %q is not registered", platform)
	}

	return factory(configPath)
}

func (r *registry) ListPlatforms() []string {
	platforms := make([]string, 0, len(r.factories))
	for platform := range r.factories {
		platforms = append(platforms, platform)
	}
	return platforms
}
