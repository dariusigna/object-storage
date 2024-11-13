package registry

import (
	"fmt"
	log "log/slog"

	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/zeromicro/go-zero/core/hash"
)

// ServiceMetadata represents the metadata of Minio service
type ServiceMetadata struct {
	Name      string
	IPAddress string
	AccessKey string
	SecretKey string
}

// Registry is a service registry
type Registry struct {
	hash      *hash.ConsistentHash                        // The hash and instances can be combined into a single data structure in production
	instances cmap.ConcurrentMap[string, ServiceMetadata] // This can be database in production, and we can also use a cache
}

// NewRegistry creates a new registry
func NewRegistry(hash *hash.ConsistentHash) *Registry {
	return &Registry{hash: hash, instances: cmap.New[ServiceMetadata]()}
}

// RegisterService registers a service
func (r *Registry) RegisterService(service ServiceMetadata) {
	log.Debug("Registering", "instance", service.IPAddress)
	r.instances.Set(service.IPAddress, service)
	r.hash.Add(service.IPAddress)
}

// DeregisterService deregisters a service
func (r *Registry) DeregisterService(ipAddress string) {
	log.Debug("Deregistering", "instance", ipAddress)
	r.instances.Remove(ipAddress)
	r.hash.Remove(ipAddress)
}

// MatchService matches a service for a given key
// It returns an error if the service is not found
// for the given key it returns the service metadata if the service is found in the registry
// The key is used for finding the service in the consistent hash store
func (r *Registry) MatchService(key string) (ServiceMetadata, error) {
	serviceIP, ok := r.hash.Get(key)
	if !ok {
		return ServiceMetadata{}, fmt.Errorf("could not match service for key %s", key)
	}

	service, ok := r.instances.Get(serviceIP.(string))
	if !ok {
		return ServiceMetadata{}, fmt.Errorf("could not find service with IP %s", service)
	}
	return service, nil
}

// GetAllServices returns all the services in the registry
func (r *Registry) GetAllServices() []ServiceMetadata {
	var services []ServiceMetadata
	for v := range r.instances.IterBuffered() {
		services = append(services, v.Val)
	}

	return services
}
