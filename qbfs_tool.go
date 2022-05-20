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
	"time"
)

const ServerUrlKey = "server_url"
const ServerToken = "server_token"
const ConfPath = "conf_path"

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
			&cli.StringFlag{Name: ConfPath, Aliases: []string{"p"}, Required: false,
				Usage: "conf path to store server connection url"},
		},
		Commands: []*cli.Command{
			{
				Name:    "service",
				Aliases: []string{"s"},
				Usage:   "options for router-server service",
				Subcommands: []*cli.Command{
					{
						Name:  "state",
						Usage: "show the router-server service state",
						Flags: []cli.Flag{
							&cli.IntFlag{Name: "check-number", Aliases: []string{"n"}, Value: 5},
						},
						Action: func(context *cli.Context) error {
							serviceHealthCheck(context)
							return nil
						},
					},
				},
			},
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
						Name:  "dump",
						Usage: "dump the mount table to local file",
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "output-file-path", Aliases: []string{"o"}},
						},
						Action: func(context *cli.Context) error {
							return mountDump(context)
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

							table := tablewriter.NewWriter(os.Stdout)
							table.SetHeader([]string{"QBFS URI", "Target FS Path", "Target FS ClusterID"})

							clusterFilterCondition := context.String("filter-cluster-id")
							lineCount := 0
							for _, v := range mounts {
								if clusterFilterCondition != "" && v.TargetClusterID != clusterFilterCondition {
									continue
								}
								table.Append([]string{v.Path, v.TargetFsPath, v.TargetClusterID})
								lineCount += 1
							}

							fmt.Println("-------------------------------")
							fmt.Println("Mount entry size: ", lineCount)

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

func mountDump(context *cli.Context) error {
	var outputFilePath = context.String("output-file-path")
	if outputFilePath == "" {
		outputFilePath, _ = os.Getwd()
	}
	mounts, err := newRouterServerClient(context).listMounts()
	if err != nil {
		return err
	}

	if mountsJsonBytes, err := json.Marshal(mounts); err == nil {
		filename := fmt.Sprintf("%s/%s.%d", outputFilePath, "mounts.dump", time.Now().Unix())
		fmt.Println("-------------------------------")
		if err := ioutil.WriteFile(filename, mountsJsonBytes, 0644); err != nil {
			fmt.Println("ERROR: Fail to dump mounts.")
			return err
		} else {
			fmt.Println("SUCCEED: dump mount tables to ", filename)
		}
	} else {
		return err
	}

	return nil
}

func calTimeFunc(action func() error, number int) (int64, error) {
	stime := time.Now()
	for i := 0; i < number; i++ {
		err := action()
		if err != nil {
			return -1, err
		}
	}
	cost := time.Since(stime)
	return cost.Milliseconds() / int64(number), nil
}

func serviceHealthCheck(ctx *cli.Context) {
	client := newRouterServerClient(ctx)

	type ApiState struct {
		apiName        string
		err            error
		requestAvgTime int64
	}
	resultChan := make(chan ApiState, 2)

	number := ctx.Int("check-number")

	go func() {
		avgTime, err := calTimeFunc(
			func() error {
				_, err := client.listMounts()
				return err
			}, number)

		resultChan <- ApiState{
			"mount list",
			err,
			avgTime,
		}
	}()
	go func() {
		avgTime, err := calTimeFunc(
			func() error {
				_, err := client.listClusterInfos()
				return err
			}, number)

		resultChan <- ApiState{
			"cluster list",
			err,
			avgTime,
		}
	}()

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"API Name", "State", "Avg Time(ms)/Number"})

	for i := 0; i < 2; i++ {
		result := <-resultChan
		var state = "OK"
		if result.err != nil {
			state = "FAIL"
		}
		table.Append([]string{result.apiName, state, fmt.Sprintf("%v(ms)/%d", result.requestAvgTime, number)})
	}

	table.Render() // Send output
}

func getConfFromFile(confPath string) (*string, *string) {
	yamlFile, err := ioutil.ReadFile(confPath)

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

func getConfFromDefaultFile() (*string, *string) {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	path := u.HomeDir + "/" + DefaultConfYamlPathDir

	return getConfFromFile(path)
}

func newRouterServerClient(context *cli.Context) *RouterMetastoreClient {
	var url = context.String(ServerUrlKey)
	var token = context.String(ServerToken)
	if len(context.String(ConfPath)) != 0 {
		url1, token2 := getConfFromFile(context.String(ConfPath))
		url = *url1
		token = *token2
	}

	return &RouterMetastoreClient{
		routerApiPrefix: url,
		routerToken:     token,
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
