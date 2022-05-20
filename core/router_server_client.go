package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

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
	RouterApiPrefix string
	RouterToken     string
}

/**
The method is to retrieve the remote mount table
*/
func (metastoreClient *RouterMetastoreClient) ListMounts() ([]MountInfo, error) {
	bytesResult, err := httpPost(metastoreClient.RouterApiPrefix+"/mount/list", metastoreClient.RouterToken)

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
func (metastoreClient *RouterMetastoreClient) ListClusterInfos() ([]ClusterInfo, error) {
	bytesResult, err := httpGet(metastoreClient.RouterApiPrefix+"/cluster/meta/list", metastoreClient.RouterToken)

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
