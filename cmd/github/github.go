package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/LF-Engineering/insights-datasource-github/gen/models"
	shared "github.com/LF-Engineering/insights-datasource-shared"
	"github.com/google/go-github/v38/github"
	"golang.org/x/oauth2"
)

const (
	// GitHubBackendVersion - backend version
	GitHubBackendVersion = "0.1.0"
	// GitHubURLRoot - GitHub URL root
	GitHubURLRoot = "https://github.com/"
	// GitHubDefaultCachePath - default path where github users cache files are stored
	GitHubDefaultCachePath = "/tmp/github-cache"
	// MaxGitHubUsersFileCacheAge 90 days (in seconds) - file is considered too old anywhere between 90-180 days
	MaxGitHubUsersFileCacheAge = 7776000
	// MaxCommentBodyLength - max comment body length
	MaxCommentBodyLength = 4096
	// MaxIssueBodyLength - max issue body length
	MaxIssueBodyLength = 4096
	// MaxPullBodyLength - max pull request body length
	MaxPullBodyLength = 4096
	// MaxReviewBodyLength - max review body length
	MaxReviewBodyLength = 4096
	// MaxReviewCommentBodyLength - max review comment body length
	MaxReviewCommentBodyLength = 4096
	// ItemsPerPage - how many items in a page
	ItemsPerPage = 100
	// AbuseWaitSeconds - N - wait random(N:2N) seconds if GitHub detected abuse
	// 7 means from 7 to 13 seconds, 10 on average
	AbuseWaitSeconds = 7
	// CacheGitHubRepo - cache this?
	CacheGitHubRepo = true
	// CacheGitHubIssues - cache this?
	CacheGitHubIssues = false
	// CacheGitHubUser - cache this?
	CacheGitHubUser = true
	// CacheGitHubUserFiles - cache this in files?
	CacheGitHubUserFiles = true
	// CacheGitHubIssueComments - cache this?
	CacheGitHubIssueComments = false
	// CacheGitHubCommentReactions - cache this?
	CacheGitHubCommentReactions = false
	// CacheGitHubIssueReactions - cache this?
	CacheGitHubIssueReactions = false
	// CacheGitHubPull - cache this?
	CacheGitHubPull = false
	// CacheGitHubPulls - cache this?
	CacheGitHubPulls = false
	// CacheGitHubPullReviews - cache this?
	CacheGitHubPullReviews = false
	// CacheGitHubPullReviewComments - cache this?
	CacheGitHubPullReviewComments = false
	// CacheGitHubReviewCommentReactions - cache this?
	CacheGitHubReviewCommentReactions = false
	// CacheGitHubPullRequestedReviewers - cache this?
	CacheGitHubPullRequestedReviewers = false
	// CacheGitHubPullCommits - cache this?
	CacheGitHubPullCommits = false
	// CacheGitHubUserOrgs - cache this?
	CacheGitHubUserOrgs = true
	// WantEnrichIssueAssignees - do we want to create rich documents for issue assignees (it contains identity data too).
	WantEnrichIssueAssignees = true
	// WantEnrichIssueCommentReactions - do we want to create rich documents for issue comment reactions (it contains identity data too).
	WantEnrichIssueCommentReactions = true
	// WantEnrichIssueReactions - do we want to create rich documents for issue reactions (it contains identity data too).
	WantEnrichIssueReactions = true
	// WantEnrichPullRequestAssignees - do we want to create rich documents for pull request assignees (it contains identity data too).
	WantEnrichPullRequestAssignees = true
	// WantEnrichPullRequestCommentReactions - do we want to create rich documents for pull request comment reactions (it contains identity data too).
	WantEnrichPullRequestCommentReactions = true
	// WantEnrichPullRequestRequestedReviewers - do we want to create rich documents for pull request requested reviewers (it contains identity data too).
	WantEnrichPullRequestRequestedReviewers = true
)

var (
	// GitHubCategories - categories defined for GitHub
	GitHubCategories = map[string]struct{}{"issue": {}, "pull_request": {}, "repository": {}}
	// GitHubIssueRoles - roles to fetch affiliation data for github issue
	GitHubIssueRoles = []string{"user_data", "assignee_data"}
	// GitHubIssueCommentRoles - roles to fetch affiliation data for github issue comment
	GitHubIssueCommentRoles = []string{"user_data"}
	// GitHubIssueAssigneeRoles - roles to fetch affiliation data for github issue comment
	GitHubIssueAssigneeRoles = []string{"assignee"}
	// GitHubIssueReactionRoles - roles to fetch affiliation data for github issue reactions or issue comment reactions
	GitHubIssueReactionRoles = []string{"user_data"}
	// GitHubPullRequestRoles - roles to fetch affiliation data for github pull request
	GitHubPullRequestRoles = []string{"user_data", "assignee_data", "merged_by_data"}
	// GitHubPullRequestCommentRoles - roles to fetch affiliation data for github pull request comment
	GitHubPullRequestCommentRoles = []string{"user_data"}
	// GitHubPullRequestAssigneeRoles - roles to fetch affiliation data for github pull request comment
	GitHubPullRequestAssigneeRoles = []string{"assignee"}
	// GitHubPullRequestReactionRoles - roles to fetch affiliation data for github pull request comment reactions
	GitHubPullRequestReactionRoles = []string{"user_data"}
	// GitHubPullRequestRequestedReviewerRoles - roles to fetch affiliation data for github pull request requested reviewer
	GitHubPullRequestRequestedReviewerRoles = []string{"requested_reviewer"}
	// GitHubPullRequestReviewRoles - roles to fetch affiliation data for github pull request comment
	GitHubPullRequestReviewRoles = []string{"user_data"}
	gGitHubDataSource            = &models.DataSource{Name: "GitHub", Slug: "github", Categories: []string{}}
	gGitHubMetaData              = &models.MetaData{BackendName: "github", BackendVersion: GitHubBackendVersion}
	gMaxUpstreamDt               time.Time
	gMaxUpstreamDtMtx            = &sync.Mutex{}
)

// DSGitHub - DS implementation for GitHub
type DSGitHub struct {
	Org      string // github org
	Repo     string // github repo
	Tokens   string // "," separated list of OAuth tokens
	CacheDir string // path to store github users cache, defaults to /tmp/github-cache
	// Flags
	FlagOrg       *string
	FlagRepo      *string
	FlagTokens    *string
	FlagCachePath *string
	// Others (calculated)
	URL                             string
	Categories                      []string
	Clients                         []*github.Client
	Context                         context.Context
	OAuthKeys                       []string
	ThrN                            int
	Hint                            int
	RateHandled                     bool
	CanCache                        bool
	GitHubMtx                       *sync.RWMutex
	GitHubRepoMtx                   *sync.RWMutex
	GitHubIssuesMtx                 *sync.RWMutex
	GitHubUserMtx                   *sync.RWMutex
	GitHubIssueCommentsMtx          *sync.RWMutex
	GitHubCommentReactionsMtx       *sync.RWMutex
	GitHubIssueReactionsMtx         *sync.RWMutex
	GitHubPullMtx                   *sync.RWMutex
	GitHubPullsMtx                  *sync.RWMutex
	GitHubPullReviewsMtx            *sync.RWMutex
	GitHubPullReviewCommentsMtx     *sync.RWMutex
	GitHubReviewCommentReactionsMtx *sync.RWMutex
	GitHubPullRequestedReviewersMtx *sync.RWMutex
	GitHubPullCommitsMtx            *sync.RWMutex
	GitHubUserOrgsMtx               *sync.RWMutex
	GitHubRateMtx                   *sync.RWMutex
	GitHubRepo                      map[string]map[string]interface{}
	GitHubIssues                    map[string][]map[string]interface{}
	GitHubUser                      map[string]map[string]interface{}
	GitHubIssueComments             map[string][]map[string]interface{}
	GitHubCommentReactions          map[string][]map[string]interface{}
	GitHubIssueReactions            map[string][]map[string]interface{}
	GitHubPull                      map[string]map[string]interface{}
	GitHubPulls                     map[string][]map[string]interface{}
	GitHubPullReviews               map[string][]map[string]interface{}
	GitHubPullReviewComments        map[string][]map[string]interface{}
	GitHubReviewCommentReactions    map[string][]map[string]interface{}
	GitHubPullRequestedReviewers    map[string][]map[string]interface{}
	GitHubPullCommits               map[string][]map[string]interface{}
	GitHubUserOrgs                  map[string][]map[string]interface{}
}

// AddFlags - add GitHub specific flags
func (j *DSGitHub) AddFlags() {
	j.FlagOrg = flag.String("github-org", "", "GitHub org, example cncf")
	j.FlagRepo = flag.String("github-repo", "", "GitHub repo, example devstats")
	j.FlagTokens = flag.String("github-tokens", "", "\",\" separated list of OAuth tokens")
	j.FlagCachePath = flag.String("github-cache-path", GitHubDefaultCachePath, "path to store github users cache, defaults to"+GitHubDefaultCachePath)
}

// ParseArgs - parse GitHub specific environment variables
func (j *DSGitHub) ParseArgs(ctx *shared.Ctx) (err error) {
	// GitHub org
	if shared.FlagPassed(ctx, "org") && *j.FlagOrg != "" {
		j.Org = *j.FlagOrg
	}
	if ctx.EnvSet("ORG") {
		j.Org = ctx.Env("ORG")
	}

	// GitHub repo
	if shared.FlagPassed(ctx, "repo") && *j.FlagRepo != "" {
		j.Repo = *j.FlagRepo
	}
	if ctx.EnvSet("REPO") {
		j.Repo = ctx.Env("REPO")
	}

	// GitHub OAuth tokens
	if shared.FlagPassed(ctx, "tokens") && *j.FlagTokens != "" {
		j.Tokens = *j.FlagTokens
	}
	if ctx.EnvSet("TOKENS") {
		j.Tokens = ctx.Env("TOKENS")
	}

	// git cache path
	j.CacheDir = GitHubDefaultCachePath
	if shared.FlagPassed(ctx, "cache-path") && *j.FlagCachePath != "" {
		j.CacheDir = *j.FlagCachePath
	}
	if ctx.EnvSet("CACHE_PATH") {
		j.CacheDir = ctx.Env("CACHE_PATH")
	}

	// NOTE: don't forget this
	for cat := range ctx.Categories {
		j.Categories = append(j.Categories, cat)
	}
	gGitHubDataSource.Categories = j.Categories
	gGitHubMetaData.Project = ctx.Project
	gGitHubMetaData.Tags = ctx.Tags
	return
}

// Validate - is current DS configuration OK?
func (j *DSGitHub) Validate(ctx *shared.Ctx) (err error) {
	j.Org = strings.TrimSpace(j.Org)
	if j.Org == "" {
		err = fmt.Errorf("github org must be set")
		return
	}
	j.Repo = strings.TrimSpace(j.Repo)
	if strings.HasSuffix(j.Repo, ".git") {
		lRepo := len(j.Repo)
		j.Repo = j.Repo[:lRepo-4]
	}
	if j.Repo == "" {
		err = fmt.Errorf("github repo must be set")
		return
	}
	j.URL = GitHubURLRoot + j.Org + "/" + j.Repo
	defer func() {
		shared.Printf("configured %d GitHub OAuth clients\n", len(j.Clients))
	}()
	j.Tokens = strings.TrimSpace(j.Tokens)
	// Get GitHub OAuth from env or from file
	oAuth := j.Tokens
	if strings.Contains(oAuth, "/") {
		bytes, err := ioutil.ReadFile(oAuth)
		shared.FatalOnError(err)
		oAuth = strings.TrimSpace(string(bytes))
	}
	// GitHub authentication or use public access
	j.Context = context.Background()
	if oAuth == "" {
		client := github.NewClient(nil)
		j.Clients = append(j.Clients, client)
	} else {
		oAuths := strings.Split(oAuth, ",")
		for _, auth := range oAuths {
			j.OAuthKeys = append(j.OAuthKeys, auth)
			ts := oauth2.StaticTokenSource(
				&oauth2.Token{AccessToken: auth},
			)
			tc := oauth2.NewClient(j.Context, ts)
			client := github.NewClient(tc)
			j.Clients = append(j.Clients, client)
		}
	}
	if CacheGitHubRepo {
		j.GitHubRepo = make(map[string]map[string]interface{})
	}
	if CacheGitHubIssues {
		j.GitHubIssues = make(map[string][]map[string]interface{})
	}
	if CacheGitHubUser {
		j.GitHubUser = make(map[string]map[string]interface{})
	}
	if CacheGitHubIssueComments {
		j.GitHubIssueComments = make(map[string][]map[string]interface{})
	}
	if CacheGitHubCommentReactions {
		j.GitHubCommentReactions = make(map[string][]map[string]interface{})
	}
	if CacheGitHubIssueReactions {
		j.GitHubIssueReactions = make(map[string][]map[string]interface{})
	}
	if CacheGitHubPull {
		j.GitHubPull = make(map[string]map[string]interface{})
	}
	if CacheGitHubPulls {
		j.GitHubPulls = make(map[string][]map[string]interface{})
	}
	if CacheGitHubPullReviews {
		j.GitHubPullReviews = make(map[string][]map[string]interface{})
	}
	if CacheGitHubPullReviewComments {
		j.GitHubPullReviewComments = make(map[string][]map[string]interface{})
	}
	if CacheGitHubReviewCommentReactions {
		j.GitHubReviewCommentReactions = make(map[string][]map[string]interface{})
	}
	if CacheGitHubPullRequestedReviewers {
		j.GitHubPullRequestedReviewers = make(map[string][]map[string]interface{})
	}
	if CacheGitHubPullCommits {
		j.GitHubPullCommits = make(map[string][]map[string]interface{})
	}
	if CacheGitHubUserOrgs {
		j.GitHubUserOrgs = make(map[string][]map[string]interface{})
	}
	// Multithreading
	j.ThrN = shared.GetThreadsNum(ctx)
	if j.ThrN > 1 {
		j.GitHubMtx = &sync.RWMutex{}
		j.GitHubRateMtx = &sync.RWMutex{}
		if CacheGitHubRepo {
			j.GitHubRepoMtx = &sync.RWMutex{}
		}
		if CacheGitHubIssues {
			j.GitHubIssuesMtx = &sync.RWMutex{}
		}
		if CacheGitHubUser {
			j.GitHubUserMtx = &sync.RWMutex{}
		}
		if CacheGitHubIssueComments {
			j.GitHubIssueCommentsMtx = &sync.RWMutex{}
		}
		if CacheGitHubCommentReactions {
			j.GitHubCommentReactionsMtx = &sync.RWMutex{}
		}
		if CacheGitHubIssueReactions {
			j.GitHubIssueReactionsMtx = &sync.RWMutex{}
		}
		if CacheGitHubPull {
			j.GitHubPullMtx = &sync.RWMutex{}
		}
		if CacheGitHubPulls {
			j.GitHubPullsMtx = &sync.RWMutex{}
		}
		if CacheGitHubPullReviews {
			j.GitHubPullReviewsMtx = &sync.RWMutex{}
		}
		if CacheGitHubPullReviewComments {
			j.GitHubPullReviewCommentsMtx = &sync.RWMutex{}
		}
		if CacheGitHubReviewCommentReactions {
			j.GitHubReviewCommentReactionsMtx = &sync.RWMutex{}
		}
		if CacheGitHubPullRequestedReviewers {
			j.GitHubPullRequestedReviewersMtx = &sync.RWMutex{}
		}
		if CacheGitHubPullCommits {
			j.GitHubPullCommitsMtx = &sync.RWMutex{}
		}
		if CacheGitHubUserOrgs {
			j.GitHubUserOrgsMtx = &sync.RWMutex{}
		}
	}
	j.Hint, _ = j.handleRate(ctx)
	j.CacheDir = os.ExpandEnv(j.CacheDir)
	if strings.HasSuffix(j.CacheDir, "/") {
		j.CacheDir = j.CacheDir[:len(j.CacheDir)-1]
	}
	_ = os.MkdirAll(j.CacheDir, 0777)
	return
}

// Init - initialize GitHub data source
func (j *DSGitHub) Init(ctx *shared.Ctx) (err error) {
	// shared.NoSSLVerify()
	ctx.InitEnv("GitHub")
	j.AddFlags()
	ctx.Init()
	err = j.ParseArgs(ctx)
	if err != nil {
		return
	}
	err = j.Validate(ctx)
	if err != nil {
		return
	}
	if ctx.Debug > 1 {
		shared.Printf("GitHub: %+v\nshared context: %s\n", j, ctx.Info())
	}
	return
}

func (j *DSGitHub) getRateLimits(gctx context.Context, ctx *shared.Ctx, gcs []*github.Client, core bool) (int, []int, []int, []time.Duration) {
	var (
		limits     []int
		remainings []int
		durations  []time.Duration
	)
	display := false
	for idx, gc := range gcs {
		rl, _, err := gc.RateLimits(gctx)
		if err != nil {
			rem, ok := shared.PeriodParse(err.Error())
			if ok {
				shared.Printf("Parsed wait time from api non-success response message: %v: %s\n", rem, err.Error())
				limits = append(limits, -1)
				remainings = append(remainings, -1)
				durations = append(durations, rem)
				display = true
				continue
			}
			shared.Printf("GetRateLimit(%d): %v\n", idx, err)
		}
		if rl == nil {
			limits = append(limits, -1)
			remainings = append(remainings, -1)
			durations = append(durations, time.Duration(5)*time.Second)
			continue
		}
		if core {
			limits = append(limits, rl.Core.Limit)
			remainings = append(remainings, rl.Core.Remaining)
			durations = append(durations, rl.Core.Reset.Time.Sub(time.Now())+time.Duration(1)*time.Second)
			continue
		}
		limits = append(limits, rl.Search.Limit)
		remainings = append(remainings, rl.Search.Remaining)
		durations = append(durations, rl.Search.Reset.Time.Sub(time.Now())+time.Duration(1)*time.Second)
	}
	hint := 0
	for idx := range limits {
		if remainings[idx] > remainings[hint] {
			hint = idx
		} else if idx != hint && remainings[idx] == remainings[hint] && durations[idx] < durations[hint] {
			hint = idx
		}
	}
	if display || ctx.Debug > 0 {
		shared.Printf("GetRateLimits: hint: %d, limits: %+v, remaining: %+v, reset: %+v\n", hint, limits, remainings, durations)
	}
	return hint, limits, remainings, durations
}

func (j *DSGitHub) handleRate(ctx *shared.Ctx) (aHint int, canCache bool) {
	if j.GitHubRateMtx != nil {
		j.GitHubRateMtx.RLock()
	}
	handled := j.RateHandled
	if handled {
		aHint = j.Hint
		canCache = j.CanCache
	}
	if j.GitHubRateMtx != nil {
		j.GitHubRateMtx.RUnlock()
	}
	if handled {
		shared.Printf("%s/%v: rate is already handled elsewhere, returning #%d token\n", j.URL, j.Categories, aHint)
		return
	}
	if j.GitHubRateMtx != nil {
		j.GitHubRateMtx.Lock()
		defer j.GitHubRateMtx.Unlock()
	}
	h, _, rem, wait := j.getRateLimits(j.Context, ctx, j.Clients, true)
	for {
		if ctx.Debug > 1 {
			shared.Printf("Checking token %d %+v %+v\n", h, rem, wait)
		}
		if rem[h] <= 5 {
			shared.Printf("All GH API tokens are overloaded, maximum points %d, waiting %+v\n", rem[h], wait[h])
			time.Sleep(time.Duration(1) * time.Second)
			time.Sleep(wait[h])
			h, _, rem, wait = j.getRateLimits(j.Context, ctx, j.Clients, true)
			continue
		}
		if rem[h] >= 500 {
			canCache = true
		}
		break
	}
	aHint = h
	j.Hint = aHint
	j.CanCache = canCache
	if ctx.Debug > 1 {
		shared.Printf("Found usable token %d/%d/%v, cache enabled: %v\n", aHint, rem[h], wait[h], canCache)
	}
	j.RateHandled = true
	shared.Printf("%s/%v: selected new token #%d\n", j.URL, j.Categories, j.Hint)
	return
}

// Sync - sync GitHub data source
func (j *DSGitHub) Sync(ctx *shared.Ctx) (err error) {
	thrN := shared.GetThreadsNum(ctx)
	if ctx.DateFrom != nil {
		shared.Printf("%s fetching from %v (%d threads)\n", j.URL, ctx.DateFrom, thrN)
	}
	if ctx.DateFrom == nil {
		ctx.DateFrom = shared.GetLastUpdate(ctx, j.URL)
		if ctx.DateFrom != nil {
			shared.Printf("%s resuming from %v (%d threads)\n", j.URL, ctx.DateFrom, thrN)
		}
	}
	if ctx.DateTo != nil {
		shared.Printf("%s fetching till %v (%d threads)\n", j.URL, ctx.DateTo, thrN)
	}
	// NOTE: Non-generic starts here
	// NOTE: Non-generic ends here
	gMaxUpstreamDtMtx.Lock()
	defer gMaxUpstreamDtMtx.Unlock()
	shared.SetLastUpdate(ctx, j.URL, gMaxUpstreamDt)
	return
}

// GetModelData - return data in swagger format
func (j *DSGitHub) GetModelData(ctx *shared.Ctx, docs []interface{}) (data *models.Data) {
	endpoint := &models.DataEndpoint{
		Org:  j.Org,
		Repo: j.Repo,
		URL:  j.URL,
	}
	data = &models.Data{
		DataSource: gGitHubDataSource,
		MetaData:   gGitHubMetaData,
		Endpoint:   endpoint,
	}
	source := data.DataSource.Slug
	for _, iDoc := range docs {
		var updatedOn time.Time
		doc, _ := iDoc.(map[string]interface{})
		shared.Printf("%s: %+v\n", source, doc)
		// FIXME
		// Event
		event := &models.Event{}
		data.Events = append(data.Events, event)
		gMaxUpstreamDtMtx.Lock()
		if updatedOn.After(gMaxUpstreamDt) {
			gMaxUpstreamDt = updatedOn
		}
		gMaxUpstreamDtMtx.Unlock()
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
