package main

import (
	"encoding/json"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/user"
	"qbfs-cli/core"
	"strings"
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
				Name:  "fs",
				Usage: "options for qbfs path",
				Subcommands: []*cli.Command{
					{
						Name:  "resolve",
						Usage: "resolve the qbfs path to real dest path, including reverse resolve",
						Flags: []cli.Flag{
							&cli.BoolFlag{Name: "reverse", Aliases: []string{"x"}, Value: false},
						},
						Action: func(context *cli.Context) error {
							return fsResolve(context)
						},
					},
				},
			},
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
							return clusterList(context)
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
							return mountList(context)
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

func fsResolve(context *cli.Context) error {
	mounts, err := newRouterServerClient(context).ListMounts()
	if err != nil {
		return err
	}

	reserve := context.Bool("reverse")
	args := context.Args()

	resultMap := make(map[string]string, args.Len())

	for i := 0; i < args.Len(); i++ {
		path := args.Get(i)
		resultMap[path] = resolvePath(mounts, path, reserve)
	}

	// Show it.

	table := tablewriter.NewWriter(os.Stdout)

	headers := []string{
		"QBFS URI",
		"Resolved Target FS Path",
	}
	if reserve {
		headers = []string{
			"Target FS Path",
			"Reverse Resolved QBFS URI",
		}
	}
	table.SetHeader(headers)

	for k, v := range resultMap {
		table.Append([]string{k, v})
	}

	table.Render() // Send output

	return nil
}

/**
This function is to resolve the QBFS Path to real dest path.

The mount table as follows:
/c1/a	=>	hdfs://cluster-1/system
/c1/a/b	=>	hdfs://cluster-1/log
/c2/b	=>	hdfs://cluster-2/system

QBFS Path will be resolved as follows

qbfs://c1/a/example.txt		=> 	hdfs://cluster-1/system/example.txt
qbfs://c1/a/b/example.txt	=>	hdfs://cluster-1/log/example.txt
qbfs://c2/b/example.txt		=>	hdfs://cluster-2/system/example.txt

The same as 'reverse resolve'
*/
func resolvePath(mounts []core.MountInfo, path string, reserve bool) string {
	urlpath, err := url.Parse(path)
	if err != nil {
		return "Path is not standard fs uri."
	}

	var bestMatchedMount core.MountInfo
	var bestMatchedLen = -1

	if !reserve {
		if urlpath.Scheme != "qbfs" {
			return "Path should be QBFS scheme."
		}

		urlWithoutScheme := urlpath.Host + urlpath.Path
		for _, mount := range mounts {
			mountPath := strings.TrimSpace(mount.Path)
			if strings.HasPrefix(urlWithoutScheme, mountPath) && bestMatchedLen < len(mountPath) {
				bestMatchedLen = len(mountPath)
				bestMatchedMount = mount
			}
		}

		if bestMatchedLen == -1 {
			return "Not found."
		}

		matchedPathPrefix := bestMatchedMount.Path
		return bestMatchedMount.TargetFsPath + strings.TrimPrefix(urlWithoutScheme, matchedPathPrefix)
	}

	/**
	Reverse resolve to get the virtual QBFS path.
	*/
	for _, mount := range mounts {
		targetPath := mount.TargetFsPath
		if strings.HasPrefix(path, targetPath) && bestMatchedLen < len(targetPath) {
			bestMatchedLen = len(targetPath)
			bestMatchedMount = mount
		}
	}
	if bestMatchedLen == -1 {
		return "Not found."
	}

	return "qbfs://" + bestMatchedMount.Path + "" + strings.TrimPrefix(path, bestMatchedMount.TargetFsPath)
}

func clusterList(context *cli.Context) error {
	client := newRouterServerClient(context)
	clusterInfos, err := client.ListClusterInfos()
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
}

func mountList(context *cli.Context) error {
	var mounts, err = newRouterServerClient(context).ListMounts()
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
}

func mountDump(context *cli.Context) error {
	var outputFilePath = context.String("output-file-path")
	if outputFilePath == "" {
		outputFilePath, _ = os.Getwd()
	}
	mounts, err := newRouterServerClient(context).ListMounts()
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
				_, err := client.ListMounts()
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
				_, err := client.ListClusterInfos()
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

func newRouterServerClient(context *cli.Context) *core.RouterMetastoreClient {
	var url = context.String(ServerUrlKey)
	var token = context.String(ServerToken)
	if len(context.String(ConfPath)) != 0 {
		url1, token2 := getConfFromFile(context.String(ConfPath))
		url = *url1
		token = *token2
	}

	return &core.RouterMetastoreClient{
		RouterApiPrefix: url,
		RouterToken:     token,
	}
}

func getConfFromDefaultFile() (*string, *string) {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	path := u.HomeDir + "/" + DefaultConfYamlPathDir

	return getConfFromFile(path)
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
