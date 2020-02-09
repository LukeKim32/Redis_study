package redisWrapper

import (
	"fmt"
	"interface_hash_server/tools"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	ctx "golang.org/x/net/context"
)

var dockerContext ctx.Context

var dockerClient *client.Client

const (
//restartTimeOutDuration is time.Duration of 20s(in nanosec)

)

var restartTimeOutDuration = 10 * time.Second

func RestartRedisContainer(address string) error {

	// if dockerClient == nil || dockerContext == nil {
	// 	if err := dockerClientSetUp(); err != nil {
	// 		return err
	// 	}
	// }

	redisContainer, err := getContainerWithIP(address)
	if err != nil {
		return err
	}

	tools.InfoLogger.Printf("RestartRedisContainer() : Redis container(%s) status : %s", address, redisContainer.Status)

	switch redisContainer.Status {

	case "restarting", "running":
		break

	default:
		tools.InfoLogger.Println("RestartRedisContainer() : Redis container(%s) restart", address)
		err = dockerClient.ContainerRestart(dockerContext, redisContainer.ID, &restartTimeOutDuration)
		if err != nil {
			return err
		}
	}

	return nil
}

func dockerClientSetUp() error {

	var err error

	dockerContext = ctx.Background()

	// Use Env For docker sdk client setup
	dockerClient, err = client.NewEnvClient()
	if err != nil {
		return err
	}

	return nil
}

func getContainerWithIP(address string) (types.Container, error) {
	tools.InfoLogger.Println("getContainerWithIP() : Called!")

	var err error

	dockerContext = ctx.Background()

	// Use Env For docker sdk client setup
	dockerClient, err = client.NewEnvClient()
	if err != nil {
		return types.Container{}, err
	}

	tools.InfoLogger.Println("getContainerWithIP() : NewEnvClient() Called!")

	containers, err := dockerClient.ContainerList(dockerContext, types.ContainerListOptions{})
	if err != nil {
		return types.Container{}, err
	}

	tools.InfoLogger.Println("getContainerWithIP() : get container list : ", len(containers))

	for _, eachContainer := range containers {
		for key, endPointSetting := range eachContainer.NetworkSettings.Networks {
			if endPointSetting.IPAddress == address {
				return eachContainer, nil
			}

			tools.InfoLogger.Println("getContainerWithIP() : endpoint key : ", key)
		}
	}

	return types.Container{}, fmt.Errorf("No Such Container with IP : %s", address)
}
