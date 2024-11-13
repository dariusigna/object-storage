package registrar

import (
	"context"
	log "log/slog"
	"strings"

	"github.com/dariusigna/object-storage/internal/registry"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/moby/moby/client"
)

const (
	// NamePrefix is the prefix of the container names that are considered for registration
	NamePrefix = "amazin-object-storage-node"
	// MinioAccessKeyVarName is the name of the environment variable that contains the MinIO access key
	MinioAccessKeyVarName = "MINIO_ACCESS_KEY"
	// MinioSecretKeyVarName is the name of the environment variable that contains the MinIO secret key
	MinioSecretKeyVarName = "MINIO_SECRET_KEY"
)

// Registrar listens for docker events and registers/deregisters instances in the registry
type Registrar struct {
	dockerClient *client.Client
	listener     chan *events.Message
	registry     *registry.Registry
}

// NewRegistrar creates a new Registrar instance
func NewRegistrar(dockerClient *client.Client, registry *registry.Registry) *Registrar {
	return &Registrar{dockerClient: dockerClient, registry: registry}
}

// ListenForDockerEvents listens for docker events and registers/deregisters instances in the registry
func (r *Registrar) ListenForDockerEvents(ctx context.Context) {
	// initial refresh
	err := r.refreshInstances(ctx)
	if err != nil {
		log.Error("Error refreshing instances", err)
	}

	filter := filters.NewArgs()
	filter.Add("name", NamePrefix)
	filter.Add("type", "container")
	for {
		messageChan, errChan := r.dockerClient.Events(ctx, events.ListOptions{Filters: filter})
	secondLoop: // Use it when we need to break out of the listening loop and retry the connection to the docker daemon
		for {
			select {
			case <-ctx.Done():
				log.Debug("Shutting down docker event listener")
				return
			case event := <-messageChan:
				log.Debug("Received docker event", "action", event.Action, "event", event.Type)
				if err = r.handleDockerEvent(ctx, event); err != nil {
					log.Error("Error handling docker event", err)
				}
			case e := <-errChan:
				log.Error("Error while listening for docker events", e)
				break secondLoop
			}
		}
	}
}

func (r *Registrar) handleDockerEvent(ctx context.Context, event events.Message) error {
	if shouldRefresh(event) {
		return r.refreshInstances(ctx)
	}
	return nil
}

func shouldRefresh(event events.Message) bool {
	return event.Action == events.ActionCreate ||
		event.Action == events.ActionDestroy ||
		event.Action == events.ActionStart ||
		event.Action == events.ActionStop ||
		event.Action == events.ActionKill ||
		event.Action == events.ActionPause ||
		event.Action == events.ActionUnPause ||
		event.Action == events.ActionRestart ||
		event.Action == events.ActionDie
}

func (r *Registrar) refreshInstances(ctx context.Context) error {
	containerFilters := filters.NewArgs()
	containerFilters.Add("name", NamePrefix)
	containers, err := r.dockerClient.ContainerList(ctx, container.ListOptions{Filters: containerFilters})
	if err != nil {
		return err
	}

	var availableInstances []registry.ServiceMetadata
	for _, c := range containers { // This can be parallelized
		info, err := r.dockerClient.ContainerInspect(ctx, c.ID)
		if err != nil {
			return err
		}

		if info.State.Status != "running" {
			log.Debug("Skipping instance", "name", info.Name, "status", info.State.Status)
			continue
		}

		serviceMetadata := getServiceMetadataFromContainer(info)
		if !isValidServiceMetadata(serviceMetadata) {
			log.Debug("Skipping instance", "name", info.Name, "reason", "missing metadata")
			continue
		}

		availableInstances = append(availableInstances, serviceMetadata)
	}

	r.diffAndUpdateInstances(availableInstances)
	return nil
}

func getServiceMetadataFromContainer(c types.ContainerJSON) registry.ServiceMetadata {
	var accessKey, secretKey string
	for _, env := range c.Config.Env {
		split := strings.SplitN(env, "=", 2)
		if len(split) != 2 {
			continue
		}

		switch split[0] {
		case MinioAccessKeyVarName:
			accessKey = split[1]
		case MinioSecretKeyVarName:
			secretKey = split[1]
		}
	}

	var ipAddress string
	for _, network := range c.NetworkSettings.Networks {
		ipAddress = network.IPAddress
		break
	}

	return registry.ServiceMetadata{
		Name:      c.Name,
		IPAddress: ipAddress,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
}

func (r *Registrar) diffAndUpdateInstances(newInstances []registry.ServiceMetadata) {
	currentInstances := r.registry.GetAllServices()
	currentSet := make(map[string]struct{})
	newSet := make(map[string]registry.ServiceMetadata)
	for _, i := range currentInstances {
		currentSet[i.IPAddress] = struct{}{}
	}

	for _, i := range newInstances {
		newSet[i.IPAddress] = i
	}

	// Identify instances to be added
	for ip, instance := range newSet {
		if _, exists := currentSet[ip]; !exists {
			r.registry.RegisterService(instance)
		}
	}

	// Identify instances to be removed
	for i := range currentSet {
		if _, exists := newSet[i]; !exists {
			r.registry.DeregisterService(i)
		}
	}
}

func isValidServiceMetadata(serviceMetadata registry.ServiceMetadata) bool {
	return serviceMetadata.IPAddress != "" && serviceMetadata.AccessKey != "" && serviceMetadata.SecretKey != ""
}
