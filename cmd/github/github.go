package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	neturl "net/url"

	"github.com/LF-Engineering/insights-datasource-github/gen/models"
	shared "github.com/LF-Engineering/insights-datasource-shared"
	"github.com/go-openapi/strfmt"
	jsoniter "github.com/json-iterator/go"
)

// Init - initialize github data source
func (j *DSGitHub) Init(ctx *shared.Ctx) (err error) {
	shared.NoSSLVerify()
	ctx.InitEnv("GitHub")
	j.AddFlags()
	ctx.Init()
	err = j.ParseArgs(ctx)
	if err != nil {
		return
	}
	err = j.Validate()
	if err != nil {
		return
	}
	if ctx.Debug > 1 {
		shared.Printf("GitHub: %+v\nshared context: %s\n", j, ctx.Info())
	}
	return
}

// Sync - sync github data source
func (j *DSGitHub) Sync(ctx *shared.Ctx) (err error) {
	thrN := shared.GetThreadsNum(ctx)
	if ctx.DateFrom != nil {
		shared.Printf("%s fetching from %v (%d threads)\n", j.GroupName, ctx.DateFrom, thrN)
	}
	if ctx.DateFrom == nil {
		ctx.DateFrom = shared.GetLastUpdate(ctx, j.GroupName)
		if ctx.DateFrom != nil {
			shared.Printf("%s resuming from %v (%d threads)\n", j.GroupName, ctx.DateFrom, thrN)
		}
	}
	if ctx.DateTo != nil {
		shared.Printf("%s fetching till %v (%d threads)\n", j.GroupName, ctx.DateTo, thrN)
	}
	// NOTE: Non-generic starts here
	// NOTE: Non-generic ends here
	gMaxCreatedAtMtx.Lock()
	defer gMaxCreatedAtMtx.Unlock()
	shared.SetLastUpdate(ctx, j.GroupName, gMaxCreatedAt)
	return
}

// GetModelData - return data in swagger format
func (j *DSGitHub) GetModelData(ctx *shared.Ctx, docs []interface{}) (data *models.Data) {
	url := GitHubURLRoot + j.GroupName
	data = &models.Data{
		DataSource: GitHubDataSource,
		MetaData:   gGitHubMetaData,
		Endpoint:   url,
	}
	source := data.DataSource.Slug
	for _, iDoc := range docs {
		doc, _ := iDoc.(map[string]interface{})
		shared.Printf("rich %+v\n", doc)
		// Event
		event := &models.Event{}
		data.Events = append(data.Events, event)
		gMaxCreatedAtMtx.Lock()
		if createdOn.After(gMaxCreatedAt) {
			gMaxCreatedAt = createdOn
		}
		gMaxCreatedAtMtx.Unlock()
	}
	return
}

func main() {
	var (
		ctx    shared.Ctx
		github DSGitHub
	)
	err := github.Init(&ctx)
	if err != nil {
		shared.Printf("Error: %+v\n", err)
		return
	}
	err = github.Sync(&ctx)
	if err != nil {
		shared.Printf("Error: %+v\n", err)
		return
	}
}
