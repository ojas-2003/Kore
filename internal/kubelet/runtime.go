package kubelet

import (
	"context"
	"fmt"
	"io"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

// Runtime is the abstraction over a container engine.
// Today it's Docker. Tomorrow it could be containerd via CRI.
// The kubelet only knows about this interface, never about Docker directly.
type Runtime interface {
	// StartContainer pulls the image if needed and starts a container.
	// Returns the runtime's container ID.
	StartContainer(ctx context.Context, podKey, image string) (string, error)

	// StopContainer stops and removes a container by its runtime ID.
	StopContainer(ctx context.Context, containerID string) error

	// IsRunning reports whether a container is currently running.
	IsRunning(ctx context.Context, containerID string) (bool, error)
}

// DockerRuntime implements Runtime against the local Docker daemon.
type DockerRuntime struct {
	cli *client.Client
}

func NewDockerRuntime() (*DockerRuntime, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	return &DockerRuntime{cli: cli}, nil
}

func (d *DockerRuntime) StartContainer(ctx context.Context, podKey, img string) (string, error) {
	// Pull the image. ImagePull returns a reader streaming pull progress;
	// we must drain it or the pull won't complete.
	reader, err := d.cli.ImagePull(ctx, img, image.PullOptions{})
	if err != nil {
		return "", fmt.Errorf("pulling %s: %w", img, err)
	}
	io.Copy(io.Discard, reader) // drain progress output
	reader.Close()

	// Create the container. We label it with the pod key so we can
	// find it again later (the kubelet's "actual state" lookup).
	resp, err := d.cli.ContainerCreate(ctx,
		&container.Config{
			Image: img,
			Labels: map[string]string{
				"kore.pod": podKey,
			},
		},
		&container.HostConfig{},
		nil, nil, "",
	)
	if err != nil {
		return "", fmt.Errorf("creating container: %w", err)
	}

	if err := d.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("starting container: %w", err)
	}

	return resp.ID, nil
}

func (d *DockerRuntime) StopContainer(ctx context.Context, containerID string) error {
	timeout := 10 // seconds for graceful SIGTERM before SIGKILL
	if err := d.cli.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout}); err != nil {
		return fmt.Errorf("stopping container: %w", err)
	}
	// Remove it so we don't accumulate dead containers.
	return d.cli.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true})
}

func (d *DockerRuntime) IsRunning(ctx context.Context, containerID string) (bool, error) {
	info, err := d.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return false, nil // container doesn't exist → not running
	}
	return info.State.Running, nil
}

func (d *DockerRuntime) ContainerIP(ctx context.Context, containerID string) (string, error) {
	info, err := d.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return "", err
	}
	// Docker's default bridge network IP for this container.
	return info.NetworkSettings.IPAddress, nil
}
