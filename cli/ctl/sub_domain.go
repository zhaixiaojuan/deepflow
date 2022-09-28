package ctl

import (
	"fmt"
	"os"

	"github.com/ghodss/yaml"
	"github.com/spf13/cobra"

	"github.com/deepflowys/deepflow/cli/ctl/common"
)

func RegisterSubDomainCommand() *cobra.Command {
	subDomain := &cobra.Command{
		Use:   "sub-domain",
		Short: "sub-domain operation commands",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("please run with 'list | create | update | delete | example.'\n")
		},
	}

	var listOutput string
	list := &cobra.Command{
		Use:     "list [name]",
		Short:   "list sub-domain info",
		Example: "deepflow-ctl sub-domain list",
		Run: func(cmd *cobra.Command, args []string) {
			listSubDomain(cmd, args, listOutput)
		},
	}
	list.Flags().StringVarP(&listOutput, "output", "o", "", "output format")

	subDomain.AddCommand(list)
	return subDomain
}

func listSubDomain(cmd *cobra.Command, args []string, output string) {
	name := ""
	if len(args) > 0 {
		name = args[0]
	}

	server := common.GetServerInfo(cmd)
	url := fmt.Sprintf("http://%s:%d/v2/sub-domains/", server.IP, server.Port)
	if name != "" {
		url += fmt.Sprintf("?name=%s", name)
	}

	response, err := common.CURLPerform("GET", url, nil, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	if output == "yaml" {
		jData, _ := response.Get("DATA").MarshalJSON()
		yData, _ := yaml.JSONToYAML(jData)
		fmt.Printf(string(yData))
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
		sb := response.Get("DATA").GetIndex(i)
		fmt.Printf(cmdFormat, nameMaxSize, sb.Get("NAME").MustString(), sb.Get("LUCCID").MustString())
	}
}
