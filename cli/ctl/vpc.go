package ctl

import (
	"fmt"

	"github.com/ghodss/yaml"
	"github.com/spf13/cobra"

	"github.com/deepflowys/deepflow/cli/ctl/common"
)

func RegisterVPCCommend() *cobra.Command {
	vpc := &cobra.Command{
		Use:   "vpc",
		Short: "vpc operation commands",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("please run with 'list'.\n")
		},
	}

	var listOutput string
	list := &cobra.Command{
		Use:     "list",
		Short:   "list vpc info",
		Example: "deepflow-ctl vpc list -o yaml",
		Run: func(cmd *cobra.Command, args []string) {
			listVPC(cmd, args, listOutput)
		},
	}
	list.Flags().StringVarP(&listOutput, "output", "o", "", "output format")

	vpc.AddCommand(list)
	return vpc
}

func listVPC(cmd *cobra.Command, args []string, output string) {
	server := common.GetServerInfo(cmd)
	url := fmt.Sprintf("http://%s:%d/v2/vpc/", server.IP, server.Port)
	var name string
	if len(args) > 0 {
		name = args[0]
	}
	if name != "" {
		url += fmt.Sprintf("?name=%s", name)
	}

	response, err := common.CURLPerform("GET", url, nil, "")
	if err != nil {
		fmt.Println(err)
		return
	}

	if output == "yaml" {
		dataJson, _ := response.Get("DATA").MarshalJSON()
		dataYaml, _ := yaml.JSONToYAML(dataJson)
		fmt.Printf(string(dataYaml))
		return
	}
	nameMaxSize := 0
	for i := range response.Get("DATA").MustArray() {
		vpc := response.Get("DATA").GetIndex(i)
		l := len(vpc.Get("NAME").MustString())
		if l > nameMaxSize {
			nameMaxSize = l
		}
	}
	cmdFormat := "%-*s %-20s\n"
	fmt.Printf(cmdFormat, nameMaxSize, "NAME", "LCUUID")
	for i := range response.Get("DATA").MustArray() {
		vpc := response.Get("DATA").GetIndex(i)
		fmt.Printf(cmdFormat, nameMaxSize, vpc.Get("NAME").MustString(), vpc.Get("LCUUID").MustString())
	}
}
