package cluster

import (
	"fmt"
	"interface_hash_server/tools"
	"log"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	ctx "golang.org/x/net/context"
)

type DockerWrapper struct {
	Context ctx.Context
	Client  *client.Client
}

var docker DockerWrapper

var restartTimeOutDuration = 10 * time.Second

func init() {
	if err := docker.checkInit(); err != nil {
		tools.ErrorLogger.Println("docker client init error")
		log.Fatal(err)
	}
}

func (docker DockerWrapper) restartRedisContainer(targetIP string) error {

	redisContainer, err := docker.getContainerWithIP(targetIP)
	if err != nil {
		return err
	}

	tools.InfoLogger.Printf("RestartRedisContainer() : Redis container(%s) status : %s", targetIP, redisContainer.Status)

	switch redisContainer.Status {

	case "restarting", "running":
		break

	default:
		tools.InfoLogger.Printf("RestartRedisContainer() : Redis container(%s) restart", targetIP)
		err = docker.Client.ContainerRestart(
			docker.Context,
			redisContainer.ID,
			&restartTimeOutDuration,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func dockerClientSetUp() error {

	var err error

	docker.Context = ctx.Background()

	// Use Env For docker sdk client setup
	docker.Client, err = client.NewEnvClient()
	if err != nil {
		return err
	}

	return nil
}

func (docker DockerWrapper) checkInit() error {
	if docker.Client == nil || docker.Context == nil {
		if err := dockerClientSetUp(); err != nil {
			return err
		}
	}

	return nil
}

func (docker DockerWrapper) getContainerWithIP(targetIP string) (types.Container, error) {

	var err error

	if err := docker.checkInit(); err != nil {
		tools.ErrorLogger.Println("docker client init error")
		return types.Container{}, err
	}

	containers, err := docker.Client.ContainerList(docker.Context, types.ContainerListOptions{})
	if err != nil {
		return types.Container{}, err
	}

	tools.InfoLogger.Println("getContainerWithIP() : get container list : ", len(containers))

	for _, eachContainer := range containers {
		for key, endPointSetting := range eachContainer.NetworkSettings.Networks {
			if strings.Contains(targetIP, endPointSetting.IPAddress) {
				return eachContainer, nil
			}

			tools.InfoLogger.Println("getContainerWithIP() : endpoint key : ", key)
		}
	}

	return types.Container{}, fmt.Errorf("No Such Container with IP : %s", targetIP)
}
