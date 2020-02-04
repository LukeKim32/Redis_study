package redisWrapper

import (
	"interface_hash_server/tools"
	"fmt"
	"time"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	ctx "golang.org/x/net/context"
)

var DockerContext ctx.Context

var DockerClient *client.Client

const (
	//restartTimeOutDuration is time.Duration of 20s(in nanosec)
	
)

var restartTimeOutDuration = 20 * time.Second

func DockerClientSetUp() error {

	var err error

	DockerContext = ctx.Background()

	// Use Env For docker sdk client setup
	DockerClient, err = client.NewEnvClient()
	if err != nil {
		return err
	}

	return nil
}

func getContainerWithIP(address string) (types.Container, error){
	containers, err := DockerClient.ContainerList(DockerContext, types.ContainerListOptions{})
	if err != nil {
		return types.Container{}, err
	}

	for _, eachContainer := range containers {
		for key, endPointSetting := range eachContainer.NetworkSettings.Networks {
			if endPointSetting.IPAddress == address {
				return eachContainer, nil
			}

			tools.InfoLogger.Println("getContainerWithIP() : endpoint key : ",key)
		}
	}

	return types.Container{}, fmt.Errorf("No Such Container with IP : %s",address)
}

func RestartRedisContainer(address string) error{

	redisContainer, err := getContainerWithIP(address)
	if err != nil {
		return err
	}

	err = DockerClient.ContainerRestart(DockerContext, redisContainer.ID, &restartTimeOutDuration)
	if err != nil {
		return err
	}

	return nil
}