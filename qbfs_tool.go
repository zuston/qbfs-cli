package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/user"
)

const ServerUrlKey = "server_url"
const ServerToken = "server_token"

const Logo = `
██████╗ ██████╗ ███████╗███████╗
██╔═══██╗██╔══██╗██╔════╝██╔════╝
██║   ██║██████╔╝█████╗  ███████╗
██║▄▄ ██║██╔══██╗██╔══╝  ╚════██║
╚██████╔╝██████╔╝██║     ███████║
 ╚══▀▀═╝ ╚═════╝ ╚═╝     ╚══════╝
`

const DefaultConfYamlPathDir = ".qbfs_tool/conf.yaml"

func main() {

	var url, token = getConfFromDefaultFile()

	var urlFlag = &cli.StringFlag{Name: ServerUrlKey, Aliases: []string{"u"}, Required: true}
	var tokenFlag = &cli.StringFlag{Name: ServerToken, Aliases: []string{"t"}, Required: true}
	if url != nil && token != nil {
		urlFlag.Required = false
		urlFlag.Value = *url

		tokenFlag.Required = false
		tokenFlag.Value = *token
	}

	app := &cli.App{
		UseShortOptionHandling: true,
		Flags: []cli.Flag{
			urlFlag,
			tokenFlag,
		},
		Commands: []*cli.Command{
			{
				Name:    "cluster",
				Aliases: []string{"c"},
				Usage:   "options for clusters",
				Subcommands: []*cli.Command{
					{
						Name:  "list",
						Usage: "list cluster info",
						Action: func(context *cli.Context) error {
							client := newRouterServerClient(context)
							clusterInfos, err := client.listClusterInfos()
							if err != nil {
								return err
							}

							table := tablewriter.NewWriter(os.Stdout)
							table.SetHeader([]string{"FS Scheme", "FS Authority", "Trash FS Path", "State"})

							for _, info := range clusterInfos {
								trashPath := fmt.Sprintf("%s://%s%s", info.ClusterIdentifier.FsScheme, info.ClusterIdentifier.FsAuthority, info.TrashPrefix)
								table.Append([]string{info.ClusterIdentifier.FsScheme, info.ClusterIdentifier.FsAuthority, trashPath, info.State})
							}
							table.Render() // Send output

							return nil
						},
					},
				},
			},
			{
				Name:    "mount",
				Aliases: []string{"m"},
				Usage:   "options for mount entry",
				Subcommands: []*cli.Command{
					{
						Name:  "add",
						Usage: "add a new mount entry",
						Action: func(c *cli.Context) error {
							fmt.Println("Not be supported yet.")
							return nil
						},
					},
					{
						Name:  "remove",
						Usage: "remove a mount entry",
						Action: func(c *cli.Context) error {
							fmt.Println("Not be supported yet.")
							return nil
						},
					},
					{
						Name:  "list",
						Usage: "list all mount entries",
						Flags: []cli.Flag{
							&cli.BoolFlag{Name: "with-replica-path", Aliases: []string{"r"}},
							&cli.StringFlag{Name: "filter-cluster-id", Aliases: []string{"c"}},
						},
						Action: func(context *cli.Context) error {
							var mounts, err = newRouterServerClient(context).listMounts()
							if err != nil {
								return err
							}

							fmt.Println("-------------------------------")
							fmt.Println("Mount entry size: ", len(mounts))

							table := tablewriter.NewWriter(os.Stdout)
							table.SetHeader([]string{"QBFS URI", "Target FS Path", "Target FS ClusterID"})

							for _, v := range mounts {
								table.Append([]string{v.Path, v.TargetFsPath, v.TargetClusterID})
							}
							table.Render() // Send output
							return nil
						},
					},
				},
			},
		},
	}

	cli.AppHelpTemplate = Logo + "\n" + cli.AppHelpTemplate

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func getConfFromDefaultFile() (*string, *string) {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	path := u.HomeDir + "/" + DefaultConfYamlPathDir

	yamlFile, err := ioutil.ReadFile(path)

	if err != nil {
		return nil, nil
	}

	type Conf struct {
		ServerUrl   string `yaml:"ServerUrl"`
		ServerToken string `yaml:"ServerToken"`
	}

	var conf *Conf
	err = yaml.Unmarshal(yamlFile, &conf)

	if err != nil {
		log.Fatal(err)
		return nil, nil
	}

	return &conf.ServerUrl, &conf.ServerToken
}

func newRouterServerClient(context *cli.Context) *RouterMetastoreClient {
	return &RouterMetastoreClient{
		routerApiPrefix: context.String(ServerUrlKey),
		routerToken:     context.String(ServerToken),
	}
}

/**
---------------------------Response Model----------------------------
*/

type MountInfo struct {
	Path       string `json:"path"`
	Attributes int    `json:"attributes"`

	TargetClusterID string `json:"targetClusterID"`
	TargetFsPath    string `json:"targetFsPath"`
	TargetFsConfig  string

	ReplicaFsPath    string `json:"replicaFsPath"`
	ReplicaClusterID string `json:"replicaClusterID"`
	ReplicaFsConfig  string

	SwitchMode byte `json:"switchMode"`
}

type ClusterInfo struct {
	ClusterIdentifier ClusterIdentifier `json:"clusterIdentifier"`
	TrashPrefix       string            `json:"trashPrefix"`
	State             string            `json:"state"`
}

type ClusterIdentifier struct {
	FsAuthority string
	FsScheme    string
}

type RouterMetastoreClient struct {
	routerApiPrefix string
	routerToken     string
}

/**
The method is to retrieve the remote mount table
*/
func (metastoreClient *RouterMetastoreClient) listMounts() ([]MountInfo, error) {
	bytesResult, err := httpPost(metastoreClient.routerApiPrefix+"/mount/list", metastoreClient.routerToken)

	if err != nil {
		return []MountInfo{}, err
	}

	type AggMountInfos struct {
		FsConfigs map[string]string `json:"fsConfigs"`
		Mounts    []MountInfo
	}

	var aggMountInfos *AggMountInfos
	if err := json.Unmarshal(bytesResult, &aggMountInfos); err != nil {
		return []MountInfo{}, err
	}

	return aggMountInfos.Mounts, nil
}

/**
The method is to retrieve the mounting cluster infos
*/
func (metastoreClient *RouterMetastoreClient) listClusterInfos() ([]ClusterInfo, error) {
	bytesResult, err := httpGet(metastoreClient.routerApiPrefix+"/cluster/meta/list", metastoreClient.routerToken)

	if err != nil {
		return []ClusterInfo{}, err
	}

	var clusterInfos []ClusterInfo
	if err := json.Unmarshal(bytesResult, &clusterInfos); err != nil {
		return []ClusterInfo{}, err
	}

	return clusterInfos, nil
}

func httpCall(method string, url string, token string) ([]byte, error) {
	httpClient := &http.Client{}

	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	req.Header.Set("token", token)

	resp, err := httpClient.Do(req)
	defer func() {
		if err == nil {
			resp.Body.Close()
		}
	}()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("Errors on requesting url of [%s], status code: [%d]", url, resp.StatusCode))
	}

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Errors on reading all data."))
	}

	return body, nil
}

func httpPost(url string, token string) ([]byte, error) {
	return httpCall("POST", url, token)
}

func httpGet(url string, token string) ([]byte, error) {
	return httpCall("GET", url, token)
}
