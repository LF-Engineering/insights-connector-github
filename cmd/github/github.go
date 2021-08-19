package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LF-Engineering/insights-datasource-github/gen/models"
	shared "github.com/LF-Engineering/insights-datasource-shared"
	"github.com/go-openapi/strfmt"
	"github.com/google/go-github/v38/github"
	jsoniter "github.com/json-iterator/go"
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
	CurrentCategory                 string
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
	if j.Tokens != "" {
		shared.AddRedacted(j.Tokens, false)
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
	if j.Tokens == "" {
		err = fmt.Errorf("at least one github oauth token must be provided")
		return
	}
	if j.Tokens != "" {
		shared.AddRedacted(j.Tokens, false)
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
		if oAuth != "" {
			shared.AddRedacted(oAuth, false)
		}
	}
	// GitHub authentication or use public access
	j.Context = context.Background()
	if oAuth == "" {
		client := github.NewClient(nil)
		j.Clients = append(j.Clients, client)
	} else {
		oAuths := strings.Split(oAuth, ",")
		for _, auth := range oAuths {
			if auth != "" {
				shared.AddRedacted(auth, false)
			}
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
	j.CacheDir += "/"
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

// Endpoint - return unique endpoint string representation
func (j *DSGitHub) Endpoint() string {
	if j.CurrentCategory == "" {
		return j.URL
	}
	return j.URL + " " + j.CurrentCategory
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
		shared.Printf("%s/%s: rate is already handled elsewhere, returning #%d token\n", j.URL, j.CurrentCategory, aHint)
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
	shared.Printf("%s/%s: selected new token #%d\n", j.URL, j.CurrentCategory, j.Hint)
	return
}

func (j *DSGitHub) isAbuse(e error) (abuse, rateLimit bool) {
	if e == nil {
		return
	}
	defer func() {
		// if abuse || rateLimit {
		// Clear rate handled flag on every error - chances are that next rate handle will recover
		shared.Printf("%s/%s: GitHub error: abuse:%v, rate limit:%v\n", j.URL, j.CurrentCategory, abuse, rateLimit)
		if e != nil {
			if j.GitHubRateMtx != nil {
				j.GitHubRateMtx.Lock()
			}
			j.RateHandled = false
			if j.GitHubRateMtx != nil {
				j.GitHubRateMtx.Unlock()
			}
		}
	}()
	errStr := e.Error()
	// GitHub can return '401 Bad credentials' when token(s) was/were revoken
	// abuse = strings.Contains(errStr, "403 You have triggered an abuse detection mechanism") || strings.Contains(errStr, "401 Bad credentials")
	abuse = strings.Contains(errStr, "403 You have triggered an abuse detection mechanism")
	rateLimit = strings.Contains(errStr, "403 API rate limit")
	return
}

func (j *DSGitHub) githubUserOrgs(ctx *shared.Ctx, login string) (orgsData []map[string]interface{}, err error) {
	var found bool
	// Try memory cache 1st
	if CacheGitHubUserOrgs {
		if j.GitHubUserOrgsMtx != nil {
			j.GitHubUserOrgsMtx.RLock()
		}
		orgsData, found = j.GitHubUserOrgs[login]
		if j.GitHubUserOrgsMtx != nil {
			j.GitHubUserOrgsMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("user orgs found in cache: %+v\n", orgsData)
			}
			return
		}
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	opt := &github.ListOptions{}
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response      *github.Response
			organizations []*github.Organization
			e             error
		)
		organizations, response, e = c.Organizations.List(j.Context, login, opt)
		if ctx.Debug > 2 {
			shared.Printf("GET %s -> {%+v, %+v, %+v}\n", login, organizations, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubUserOrgs {
				if j.GitHubUserOrgsMtx != nil {
					j.GitHubUserOrgsMtx.Lock()
				}
				j.GitHubUserOrgs[login] = []map[string]interface{}{}
				if j.GitHubUserOrgsMtx != nil {
					j.GitHubUserOrgsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubUserOrgs: orgs not found %s: %v\n", login, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s user orgs: response: %+v, because: %+v, retrying rate\n", login, response, e)
			shared.Printf("githubUserOrgs: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get user orgs %s), waiting for %ds\n", login, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get user orgs %s) waiting 1s before token switch\n", login)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		for _, organization := range organizations {
			org := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(organization)
			_ = jsoniter.Unmarshal(jm, &org)
			orgsData = append(orgsData, org)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			shared.Printf("processing next user orgs page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		shared.Printf("user orgs got from API: %+v\n", orgsData)
	}
	if CacheGitHubUserOrgs {
		if j.GitHubUserOrgsMtx != nil {
			j.GitHubUserOrgsMtx.Lock()
		}
		j.GitHubUserOrgs[login] = orgsData
		if j.GitHubUserOrgsMtx != nil {
			j.GitHubUserOrgsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubUser(ctx *shared.Ctx, login string) (user map[string]interface{}, found bool, err error) {
	var ok bool
	// Try memory cache 1st
	if CacheGitHubUser {
		if j.GitHubUserMtx != nil {
			j.GitHubUserMtx.RLock()
		}
		user, ok = j.GitHubUser[login]
		if j.GitHubUserMtx != nil {
			j.GitHubUserMtx.RUnlock()
		}
		if ok {
			found = len(user) > 0
			if ctx.Debug > 1 {
				shared.Printf("user found in memory cache: %+v\n", user)
			}
			return
		}
		// Try file cache 2nd
		if CacheGitHubUserFiles {
			path := j.CacheDir + login + ".json"
			lockPath := path + ".lock"
			file, e := os.Stat(path)
			if e == nil {
				for {
					waited := 0
					_, e := os.Stat(lockPath)
					if e == nil {
						if ctx.Debug > 0 {
							shared.Printf("user %s lock file %s present, waiting 1s\n", user, lockPath)
						}
						time.Sleep(time.Duration(1) * time.Second)
						waited++
						continue
					}
					if waited > 0 {
						if ctx.Debug > 0 {
							shared.Printf("user %s lock file %s was present, waited %ds\n", user, lockPath, waited)
						}
					}
					file, _ = os.Stat(path)
					break
				}
				modified := file.ModTime()
				age := int(time.Now().Sub(modified).Seconds())
				allowedAge := MaxGitHubUsersFileCacheAge + rand.Intn(MaxGitHubUsersFileCacheAge)
				if age <= allowedAge {
					bts, e := ioutil.ReadFile(path)
					if e == nil {
						e = jsoniter.Unmarshal(bts, &user)
						bts = nil
						if e == nil {
							found = len(user) > 0
							if found {
								if j.GitHubUserMtx != nil {
									j.GitHubUserMtx.Lock()
								}
								j.GitHubUser[login] = user
								if j.GitHubUserMtx != nil {
									j.GitHubUserMtx.Unlock()
								}
								if ctx.Debug > 1 {
									shared.Printf("user found in files cache: %+v\n", user)
								}
								return
							}
							shared.Printf("githubUser: unmarshaled %s cache file is empty\n", path)
						}
						shared.Printf("githubUser: cannot unmarshal %s cache file: %v\n", path, e)
					} else {
						shared.Printf("githubUser: cannot read %s user cache file: %v\n", path, e)
					}
				} else {
					shared.Printf("githubUser: %s user cache file is too old: %v (allowed %v)\n", path, time.Duration(age)*time.Second, time.Duration(allowedAge)*time.Second)
				}
			} else {
				if ctx.Debug > 1 {
					shared.Printf("githubUser: no %s user cache file: %v\n", path, e)
				}
			}
			locked := false
			lockFile, e := os.Create(lockPath)
			if e != nil {
				shared.Printf("githubUser: create %s lock file failed: %v\n", lockPath, e)
			} else {
				locked = true
				_ = lockFile.Close()
			}
			defer func() {
				if locked {
					defer func() {
						if ctx.Debug > 1 {
							shared.Printf("remove lock file %s\n", lockPath)
						}
						_ = os.Remove(lockPath)
					}()
				}
				if err != nil {
					return
				}
				// path := j.CacheDir + login + ".json"
				bts, err := jsoniter.Marshal(user)
				if err != nil {
					shared.Printf("githubUser: cannot marshal user %s to file %s\n", login, path)
					return
				}
				err = ioutil.WriteFile(path, bts, 0644)
				if err != nil {
					shared.Printf("githubUser: cannot write file %s, %d bytes: %v\n", path, len(bts), err)
					return
				}
				if ctx.Debug > 0 {
					shared.Printf("githubUser: saved %s user file\n", path)
				}
			}()
		}
	}
	// Try GitHub API 3rd
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	retry := false
	for {
		var (
			response *github.Response
			usr      *github.User
			e        error
		)
		usr, response, e = c.Users.Get(j.Context, login)
		if ctx.Debug > 2 {
			shared.Printf("GET %s -> {%+v, %+v, %+v}\n", login, usr, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubUser {
				if j.GitHubUserMtx != nil {
					j.GitHubUserMtx.Lock()
				}
				if ctx.Debug > 0 {
					shared.Printf("user not found using API: %s\n", login)
				}
				j.GitHubUser[login] = map[string]interface{}{}
				if j.GitHubUserMtx != nil {
					j.GitHubUserMtx.Unlock()
				}
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s user: response: %+v, because: %+v, retrying rate\n", login, response, e)
			shared.Printf("githubUser: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get user %s), waiting for %ds\n", login, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get user %s) waiting 1s before token switch\n", login)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		if usr != nil {
			jm, _ := jsoniter.Marshal(usr)
			_ = jsoniter.Unmarshal(jm, &user)
			user["organizations"], err = j.githubUserOrgs(ctx, login)
			if err != nil {
				return
			}
			if ctx.Debug > 1 {
				shared.Printf("user found using API: %+v\n", user)
			}
			found = true
		}
		break
	}
	if CacheGitHubUser {
		if j.GitHubUserMtx != nil {
			j.GitHubUserMtx.Lock()
		}
		j.GitHubUser[login] = user
		if j.GitHubUserMtx != nil {
			j.GitHubUserMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubRepo(ctx *shared.Ctx, org, repo string) (repoData map[string]interface{}, err error) {
	var found bool
	origin := org + "/" + repo
	// Try memory cache 1st
	if CacheGitHubRepo {
		if j.GitHubRepoMtx != nil {
			j.GitHubRepoMtx.RLock()
		}
		repoData, found = j.GitHubRepo[origin]
		if j.GitHubRepoMtx != nil {
			j.GitHubRepoMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("repos found in cache: %+v\n", repoData)
			}
			return
		}
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	retry := false
	for {
		var (
			response *github.Response
			rep      *github.Repository
			e        error
		)
		rep, response, e = c.Repositories.Get(j.Context, org, repo)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, rep, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubRepo {
				if j.GitHubRepoMtx != nil {
					j.GitHubRepoMtx.Lock()
				}
				j.GitHubRepo[origin] = nil
				if j.GitHubRepoMtx != nil {
					j.GitHubRepoMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubRepos: repo not found %s: %v\n", origin, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s repo: response: %+v, because: %+v, retrying rate\n", origin, response, e)
			shared.Printf("githubRepos: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get repo %s), waiting for %ds\n", origin, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get repo %s) waiting 1s before token switch\n", origin)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		jm, _ := jsoniter.Marshal(rep)
		_ = jsoniter.Unmarshal(jm, &repoData)
		if ctx.Debug > 2 {
			shared.Printf("repos got from API: %+v\n", repoData)
		}
		break
	}
	if CacheGitHubRepo {
		if j.GitHubRepoMtx != nil {
			j.GitHubRepoMtx.Lock()
		}
		j.GitHubRepo[origin] = repoData
		if j.GitHubRepoMtx != nil {
			j.GitHubRepoMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubIssues(ctx *shared.Ctx, org, repo string, since *time.Time) (issuesData []map[string]interface{}, err error) {
	var found bool
	origin := org + "/" + repo
	// Try memory cache 1st
	if CacheGitHubIssues {
		if j.GitHubIssuesMtx != nil {
			j.GitHubIssuesMtx.RLock()
		}
		issuesData, found = j.GitHubIssues[origin]
		if j.GitHubIssuesMtx != nil {
			j.GitHubIssuesMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("issues found in cache: %+v\n", issuesData)
			}
			return
		}
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	opt := &github.IssueListByRepoOptions{
		State:     "all",
		Sort:      "updated",
		Direction: "asc",
	}
	opt.PerPage = ItemsPerPage
	if since != nil {
		opt.Since = *since
	}
	retry := false
	for {
		var (
			response *github.Response
			issues   []*github.Issue
			e        error
		)
		issues, response, e = c.Issues.ListByRepo(j.Context, org, repo, opt)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, issues, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubIssues {
				if j.GitHubIssuesMtx != nil {
					j.GitHubIssuesMtx.Lock()
				}
				j.GitHubIssues[origin] = []map[string]interface{}{}
				if j.GitHubIssuesMtx != nil {
					j.GitHubIssuesMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubIssues: issues not found %s: %v\n", origin, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s issues: response: %+v, because: %+v, retrying rate\n", origin, response, e)
			shared.Printf("githubIssues: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get issues %s), waiting for %ds\n", origin, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get issues %s) waiting 1s before token switch\n", origin)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		for _, issue := range issues {
			iss := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(issue)
			_ = jsoniter.Unmarshal(jm, &iss)
			body, ok := shared.Dig(iss, []string{"body"}, false, true)
			if ok {
				nBody := len(body.(string))
				if nBody > MaxIssueBodyLength {
					iss["body"] = body.(string)[:MaxIssueBodyLength]
				}
			}
			iss["body_analyzed"], _ = iss["body"]
			iss["is_pull"] = issue.IsPullRequest()
			issuesData = append(issuesData, iss)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			runtime.GC()
			shared.Printf("%s/%s: processing next issues page: %d\n", j.URL, j.CurrentCategory, opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		shared.Printf("issues got from API: %+v\n", issuesData)
	}
	if CacheGitHubIssues {
		if j.GitHubIssuesMtx != nil {
			j.GitHubIssuesMtx.Lock()
		}
		j.GitHubIssues[origin] = issuesData
		if j.GitHubIssuesMtx != nil {
			j.GitHubIssuesMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubIssueComments(ctx *shared.Ctx, org, repo string, number int) (comments []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubIssueComments {
		if j.GitHubIssueCommentsMtx != nil {
			j.GitHubIssueCommentsMtx.RLock()
		}
		comments, found = j.GitHubIssueComments[key]
		if j.GitHubIssueCommentsMtx != nil {
			j.GitHubIssueCommentsMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("issue comments found in cache: %+v\n", comments)
			}
			return
		}
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	opt := &github.IssueListCommentsOptions{}
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			comms    []*github.IssueComment
			e        error
		)
		comms, response, e = c.Issues.ListComments(j.Context, org, repo, number, opt)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, comms, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubIssueComments {
				if j.GitHubIssueCommentsMtx != nil {
					j.GitHubIssueCommentsMtx.Lock()
				}
				j.GitHubIssueComments[key] = []map[string]interface{}{}
				if j.GitHubIssueCommentsMtx != nil {
					j.GitHubIssueCommentsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubIssueComments: comments not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s issue comments: response: %+v, because: %+v, retrying rate\n", key, response, e)
			shared.Printf("githubIssueComments: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get issue comments %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get issue comments %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		for _, comment := range comms {
			com := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(comment)
			_ = jsoniter.Unmarshal(jm, &com)
			body, ok := shared.Dig(com, []string{"body"}, false, true)
			if ok {
				nBody := len(body.(string))
				if nBody > MaxCommentBodyLength {
					com["body"] = body.(string)[:MaxCommentBodyLength]
				}
			}
			com["body_analyzed"], _ = com["body"]
			userLogin, ok := shared.Dig(com, []string{"user", "login"}, false, true)
			if ok {
				com["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
				if err != nil {
					return
				}
			}
			iCnt, ok := shared.Dig(com, []string{"reactions", "total_count"}, false, true)
			if ok {
				com["reactions_data"] = []interface{}{}
				cnt := int(iCnt.(float64))
				if cnt > 0 {
					cid, ok := shared.Dig(com, []string{"id"}, false, true)
					if ok {
						com["reactions_data"], err = j.githubCommentReactions(ctx, org, repo, int64(cid.(float64)))
						if err != nil {
							return
						}
					}
				}
			}
			comments = append(comments, com)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			shared.Printf("processing next issue comments page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		shared.Printf("issue comments got from API: %+v\n", comments)
	}
	if CacheGitHubIssueComments {
		if j.GitHubIssueCommentsMtx != nil {
			j.GitHubIssueCommentsMtx.Lock()
		}
		j.GitHubIssueComments[key] = comments
		if j.GitHubIssueCommentsMtx != nil {
			j.GitHubIssueCommentsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubCommentReactions(ctx *shared.Ctx, org, repo string, cid int64) (reactions []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, cid)
	if ctx.Debug > 1 {
		shared.Printf("githubCommentReactions %s\n", key)
	}
	// Try memory cache 1st
	if CacheGitHubCommentReactions {
		if j.GitHubCommentReactionsMtx != nil {
			j.GitHubCommentReactionsMtx.RLock()
		}
		reactions, found = j.GitHubCommentReactions[key]
		if j.GitHubCommentReactionsMtx != nil {
			j.GitHubCommentReactionsMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("comment reactions found in cache: %+v\n", reactions)
			}
			return
		}
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	opt := &github.ListOptions{}
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			reacts   []*github.Reaction
			e        error
		)
		reacts, response, e = c.Reactions.ListIssueCommentReactions(j.Context, org, repo, cid, opt)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, cid, reacts, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubCommentReactions {
				if j.GitHubCommentReactionsMtx != nil {
					j.GitHubCommentReactionsMtx.Lock()
				}
				j.GitHubCommentReactions[key] = []map[string]interface{}{}
				if j.GitHubCommentReactionsMtx != nil {
					j.GitHubCommentReactionsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubCommentReactions: reactions not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s comment reactions: response: %+v, because: %+v, retrying rate\n", key, response, e)
			shared.Printf("githubCommentReactions: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get comment reactions %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get comment reactions %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		for _, reaction := range reacts {
			react := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(reaction)
			_ = jsoniter.Unmarshal(jm, &react)
			userLogin, ok := shared.Dig(react, []string{"user", "login"}, false, true)
			if ok {
				react["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
				if err != nil {
					return
				}
			}
			reactions = append(reactions, react)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			shared.Printf("processing next comment reactions page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		shared.Printf("comment reactions got from API: %+v\n", reactions)
	}
	if CacheGitHubCommentReactions {
		if j.GitHubCommentReactionsMtx != nil {
			j.GitHubCommentReactionsMtx.Lock()
		}
		j.GitHubCommentReactions[key] = reactions
		if j.GitHubCommentReactionsMtx != nil {
			j.GitHubCommentReactionsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubIssueReactions(ctx *shared.Ctx, org, repo string, number int) (reactions []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	if ctx.Debug > 1 {
		shared.Printf("githubIssueReactions %s\n", key)
	}
	// Try memory cache 1st
	if CacheGitHubIssueReactions {
		if j.GitHubIssueReactionsMtx != nil {
			j.GitHubIssueReactionsMtx.RLock()
		}
		reactions, found = j.GitHubIssueReactions[key]
		if j.GitHubIssueReactionsMtx != nil {
			j.GitHubIssueReactionsMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("issue reactions found in cache: %+v\n", reactions)
			}
			return
		}
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	opt := &github.ListOptions{}
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			reacts   []*github.Reaction
			e        error
		)
		reacts, response, e = c.Reactions.ListIssueReactions(j.Context, org, repo, number, opt)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, reacts, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubIssueReactions {
				if j.GitHubIssueReactionsMtx != nil {
					j.GitHubIssueReactionsMtx.Lock()
				}
				j.GitHubIssueReactions[key] = []map[string]interface{}{}
				if j.GitHubIssueReactionsMtx != nil {
					j.GitHubIssueReactionsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubIssueReactions: reactions not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s issue reactions: response: %+v, because: %+v, retrying rate\n", key, response, e)
			shared.Printf("githubIssueReactions: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get issue reactions %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get issue reactions %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		for _, reaction := range reacts {
			react := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(reaction)
			_ = jsoniter.Unmarshal(jm, &react)
			userLogin, ok := shared.Dig(react, []string{"user", "login"}, false, true)
			if ok {
				react["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
				if err != nil {
					return
				}
			}
			reactions = append(reactions, react)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			shared.Printf("processing next issue reactions page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		shared.Printf("issue reactions got from API: %+v\n", reactions)
	}
	if CacheGitHubIssueReactions {
		if j.GitHubIssueReactionsMtx != nil {
			j.GitHubIssueReactionsMtx.Lock()
		}
		j.GitHubIssueReactions[key] = reactions
		if j.GitHubIssueReactionsMtx != nil {
			j.GitHubIssueReactionsMtx.Unlock()
		}
	}
	return
}

// ItemID - return unique identifier for an item
func (j *DSGitHub) ItemID(item interface{}) string {
	if j.CurrentCategory == "repository" {
		id, ok := item.(map[string]interface{})["fetched_on"]
		if !ok {
			shared.Fatalf("github: ItemID() - cannot extract fetched_on from %+v", shared.DumpKeys(item))
		}
		return fmt.Sprintf("%v", id)
	}
	number, ok := item.(map[string]interface{})["number"]
	if !ok {
		shared.Fatalf("github: ItemID() - cannot extract number from %+v", shared.DumpKeys(item))
	}
	return fmt.Sprintf("%s/%s/%s/%d", j.Org, j.Repo, j.CurrentCategory, int(number.(float64)))
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSGitHub) ItemUpdatedOn(item interface{}) time.Time {
	if j.CurrentCategory == "repository" {
		epochNS, ok := item.(map[string]interface{})["fetched_on"].(float64)
		if ok {
			epochNS *= 1.0e9
			return time.Unix(0, int64(epochNS))
		}
		epochS, ok := item.(map[string]interface{})["fetched_on"].(string)
		if !ok {
			shared.Fatalf("github: ItemUpdatedOn() - cannot extract fetched_on from %+v", shared.DumpKeys(item))
		}
		epochNS, err := strconv.ParseFloat(epochS, 64)
		shared.FatalOnError(err)
		epochNS *= 1.0e9
		return time.Unix(0, int64(epochNS))
	}
	iWhen, _ := shared.Dig(item, []string{"updated_at"}, true, false)
	when, err := shared.TimeParseInterfaceString(iWhen)
	shared.FatalOnError(err)
	return when
}

// AddMetadata - add metadata to the item
func (j *DSGitHub) AddMetadata(ctx *shared.Ctx, item interface{}) (mItem map[string]interface{}) {
	mItem = make(map[string]interface{})
	origin := j.URL
	tags := ctx.Tags
	if len(tags) == 0 {
		tags = []string{origin}
	}
	itemID := j.ItemID(item)
	updatedOn := j.ItemUpdatedOn(item)
	uuid := shared.UUIDNonEmpty(ctx, origin, itemID)
	timestamp := time.Now()
	mItem["backend_name"] = "github"
	mItem["backend_version"] = GitHubBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e9)
	mItem["uuid"] = uuid
	mItem["origin"] = origin
	mItem["tags"] = tags
	mItem["offset"] = float64(updatedOn.Unix())
	mItem["category"] = j.CurrentCategory
	mItem["is_github_"+j.CurrentCategory] = 1
	mItem["search_fields"] = make(map[string]interface{})
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", "owner"}, j.Org, false))
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", "repo"}, j.Repo, false))
	mItem["metadata__updated_on"] = shared.ToESDate(updatedOn)
	mItem["metadata__timestamp"] = shared.ToESDate(timestamp)
	// mItem[ProjectSlug] = ctx.ProjectSlug
	return
}

// ProcessIssue - add issues sub items
func (j *DSGitHub) ProcessIssue(ctx *shared.Ctx, inIssue map[string]interface{}) (issue map[string]interface{}, err error) {
	issue = inIssue
	issue["user_data"] = map[string]interface{}{}
	issue["assignee_data"] = map[string]interface{}{}
	issue["assignees_data"] = []interface{}{}
	issue["comments_data"] = []interface{}{}
	issue["reactions_data"] = []interface{}{}
	// ["user", "assignee", "assignees", "comments", "reactions"]
	userLogin, ok := shared.Dig(issue, []string{"user", "login"}, false, true)
	if ok {
		issue["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
		if err != nil {
			return
		}
	}
	assigneeLogin, ok := shared.Dig(issue, []string{"assignee", "login"}, false, true)
	if ok {
		issue["assignee_data"], _, err = j.githubUser(ctx, assigneeLogin.(string))
		if err != nil {
			return
		}
	}
	iAssignees, ok := shared.Dig(issue, []string{"assignees"}, false, true)
	if ok {
		assignees, _ := iAssignees.([]interface{})
		assigneesAry := []map[string]interface{}{}
		for _, assignee := range assignees {
			aLogin, ok := shared.Dig(assignee, []string{"login"}, false, true)
			if ok {
				assigneeData, _, e := j.githubUser(ctx, aLogin.(string))
				if e != nil {
					err = e
					return
				}
				assigneesAry = append(assigneesAry, assigneeData)
			}
		}
		issue["assignees_data"] = assigneesAry
	}
	number, ok := shared.Dig(issue, []string{"number"}, false, true)
	if ok {
		issue["comments_data"], err = j.githubIssueComments(ctx, j.Org, j.Repo, int(number.(float64)))
		if err != nil {
			return
		}
		iCnt, ok := shared.Dig(issue, []string{"reactions", "total_count"}, false, true)
		if ok {
			issue["reactions_data"] = []interface{}{}
			cnt := int(iCnt.(float64))
			if cnt > 0 {
				issue["reactions_data"], err = j.githubIssueReactions(ctx, j.Org, j.Repo, int(number.(float64)))
				if err != nil {
					return
				}
			}
		}
	}
	return
}

// FetchItemsRepository - implement raw repository data for GitHub datasource
func (j *DSGitHub) FetchItemsRepository(ctx *shared.Ctx) (err error) {
	items := []interface{}{}
	docs := []interface{}{}
	item, err := j.githubRepo(ctx, j.Org, j.Repo)
	shared.FatalOnError(err)
	if item == nil {
		shared.Fatalf("there is no such repo %s/%s", j.Org, j.Repo)
		return
	}
	item["fetched_on"] = fmt.Sprintf("%.6f", float64(time.Now().UnixNano())/1.0e9)
	esItem := j.AddMetadata(ctx, item)
	if ctx.Project != "" {
		item["project"] = ctx.Project
	}
	esItem["data"] = item
	items = append(items, esItem)
	// NOTE: non-generic
	// err = SendToQueue(ctx, j, true, UUID, items)
	err = j.GitHubEnrichItems(ctx, items, &docs, true)
	if err != nil {
		shared.Printf("%s/%s: Error %v sending %d repo stats to queue\n", j.URL, j.CurrentCategory, err, len(items))
	}
	return
}

// FetchItemsIssue - implement raw issue data for GitHub datasource
func (j *DSGitHub) FetchItemsIssue(ctx *shared.Ctx) (err error) {
	// Process issues (possibly in threads)
	var (
		ch           chan error
		allDocs      []interface{}
		allIssues    []interface{}
		allIssuesMtx *sync.Mutex
		escha        []chan error
		eschaMtx     *sync.Mutex
	)
	if j.ThrN > 1 {
		ch = make(chan error)
		allIssuesMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads, nIss, issProcessed := 0, 0, 0
	processIssue := func(c chan error, issue map[string]interface{}) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		item, err := j.ProcessIssue(ctx, issue)
		shared.FatalOnError(err)
		esItem := j.AddMetadata(ctx, item)
		if ctx.Project != "" {
			item["project"] = ctx.Project
		}
		esItem["data"] = item
		if issProcessed%ItemsPerPage == 0 {
			shared.Printf("%s/%s: processed %d/%d issues\n", j.URL, j.CurrentCategory, issProcessed, nIss)
		}
		if allIssuesMtx != nil {
			allIssuesMtx.Lock()
		}
		allIssues = append(allIssues, esItem)
		nIssues := len(allIssues)
		if nIssues >= ctx.PackSize {
			sendToQueue := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = j.GitHubEnrichItems(ctx, allIssues, &allDocs, false)
				//ee = SendToQueue(ctx, j, true, UUID, allIssues)
				if ee != nil {
					shared.Printf("%s/%s: error %v sending %d issues to queue\n", j.URL, j.CurrentCategory, ee, len(allIssues))
				}
				allIssues = []interface{}{}
				if allIssuesMtx != nil {
					allIssuesMtx.Unlock()
				}
				return
			}
			if j.ThrN > 1 {
				wch = make(chan error)
				go func() {
					_ = sendToQueue(wch)
				}()
			} else {
				e = sendToQueue(nil)
				if e != nil {
					return
				}
			}
		} else {
			if allIssuesMtx != nil {
				allIssuesMtx.Unlock()
			}
		}
		return
	}
	issues, err := j.githubIssues(ctx, j.Org, j.Repo, ctx.DateFrom)
	shared.FatalOnError(err)
	runtime.GC()
	nIss = len(issues)
	shared.Printf("%s/%s: got %d issues\n", j.URL, j.CurrentCategory, nIss)
	if j.ThrN > 1 {
		for _, issue := range issues {
			go func(iss map[string]interface{}) {
				var (
					e    error
					esch chan error
				)
				esch, e = processIssue(ch, iss)
				if e != nil {
					shared.Printf("%s/%s: issues process error: %v\n", j.URL, j.CurrentCategory, e)
					return
				}
				if esch != nil {
					if eschaMtx != nil {
						eschaMtx.Lock()
					}
					escha = append(escha, esch)
					if eschaMtx != nil {
						eschaMtx.Unlock()
					}
				}
			}(issue)
			nThreads++
			if nThreads == j.ThrN {
				err = <-ch
				if err != nil {
					return
				}
				issProcessed++
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
			issProcessed++
		}
	} else {
		for _, issue := range issues {
			_, err = processIssue(nil, issue)
			if err != nil {
				return
			}
			issProcessed++
		}
	}
	if eschaMtx != nil {
		eschaMtx.Lock()
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			if eschaMtx != nil {
				eschaMtx.Unlock()
			}
			return
		}
	}
	if eschaMtx != nil {
		eschaMtx.Unlock()
	}
	nIssues := len(allIssues)
	if ctx.Debug > 0 {
		shared.Printf("%d remaining issues to send to queue\n", nIssues)
	}
	// err = SendToQueue(ctx, j, true, UUID, allIssues)
	err = j.GitHubEnrichItems(ctx, allIssues, &allDocs, true)
	if err != nil {
		shared.Printf("%s/%s: error %v sending %d issues to queue\n", j.URL, j.CurrentCategory, err, len(allIssues))
	}
	return
}

// GitHubRepositoryEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSGitHub) GitHubRepositoryEnrichItemsFunc(ctx *shared.Ctx, items []interface{}, docs *[]interface{}, final bool) (err error) {
	if ctx.Debug > 0 {
		shared.Printf("%s/%s: github enrich repository items %d/%d func\n", j.URL, j.CurrentCategory, len(items), len(*docs))
	}
	var (
		mtx *sync.RWMutex
		ch  chan error
	)
	thrN := j.ThrN
	if thrN > 1 {
		mtx = &sync.RWMutex{}
		ch = make(chan error)
	}
	nThreads := 0
	procItem := func(c chan error, idx int) (e error) {
		if thrN > 1 {
			mtx.RLock()
		}
		item := items[idx]
		if thrN > 1 {
			mtx.RUnlock()
		}
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		// NOTE: never refer to _source - we no longer use ES
		doc, ok := item.(map[string]interface{})
		if !ok {
			e = fmt.Errorf("Failed to parse document %+v", doc)
			return
		}
		var rich map[string]interface{}
		rich, e = j.EnrichItem(ctx, doc)
		if e != nil {
			return
		}
		if thrN > 1 {
			mtx.Lock()
		}
		*docs = append(*docs, rich)
		// NOTE: flush here
		if len(*docs) >= ctx.PackSize {
			j.OutputDocs(ctx, items, docs, final)
		}
		if thrN > 1 {
			mtx.Unlock()
		}
		return
	}
	if thrN > 1 {
		for i := range items {
			go func(i int) {
				_ = procItem(ch, i)
			}(i)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
		return
	}
	for i := range items {
		err = procItem(nil, i)
		if err != nil {
			return
		}
	}
	return
}

// EnrichRepositoryItem - return rich item from raw item for a given author type
func (j *DSGitHub) EnrichRepositoryItem(ctx *shared.Ctx, item map[string]interface{}) (rich map[string]interface{}, err error) {
	rich = make(map[string]interface{})
	repo, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", shared.DumpKeys(item))
		return
	}
	for _, field := range shared.RawFields {
		v, _ := item[field]
		rich[field] = v
	}
	if ctx.Project != "" {
		rich["project"] = ctx.Project
	}
	repoFields := []string{"id", "forks_count", "subscribers_count", "stargazers_count", "fetched_on"}
	for _, field := range repoFields {
		v, _ := repo[field]
		rich[field] = v
	}
	v, _ := repo["html_url"]
	rich["url"] = v
	rich["repo_name"] = j.URL
	updatedOn, _ := shared.Dig(item, []string{"metadata__updated_on"}, true, false)
	rich["metadata__updated_on"] = updatedOn
	rich["type"] = j.CurrentCategory
	rich["category"] = j.CurrentCategory
	// NOTE: From shared
	rich["metadata__enriched_on"] = time.Now()
	// rich[ProjectSlug] = ctx.ProjectSlug
	// rich["groups"] = ctx.Groups
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSGitHub) EnrichItem(ctx *shared.Ctx, item map[string]interface{}) (rich map[string]interface{}, err error) {
	switch j.CurrentCategory {
	case "repository":
		return j.EnrichRepositoryItem(ctx, item)
		// FIXME
		//case "issue":
		//	return j.EnrichIssueItem(ctx, item, author, affs, extra)
		//case "pull_request":
		//	return j.EnrichPullRequestItem(ctx, item, author, affs, extra)
	}
	return
}

// OutputDocs - send output documents to the consumer
func (j *DSGitHub) OutputDocs(ctx *shared.Ctx, items []interface{}, docs *[]interface{}, final bool) {
	if len(*docs) > 0 {
		// actual output
		shared.Printf("output processing(%d/%d/%v)\n", len(items), len(*docs), final)
		data := j.GetModelData(ctx, *docs)
		// FIXME: actual output to some consumer...
		jsonBytes, err := jsoniter.Marshal(data)
		if err != nil {
			shared.Printf("Error: %+v\n", err)
			return
		}
		shared.Printf("%s\n", string(jsonBytes))
		*docs = []interface{}{}
		gMaxUpstreamDtMtx.Lock()
		defer gMaxUpstreamDtMtx.Unlock()
		shared.SetLastUpdate(ctx, j.URL, gMaxUpstreamDt)
	}
}

// GitHubEnrichItems - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSGitHub) GitHubEnrichItems(ctx *shared.Ctx, items []interface{}, docs *[]interface{}, final bool) (err error) {
	shared.Printf("input processing(%d/%d/%v)\n", len(items), len(*docs), final)
	if final {
		defer func() {
			j.OutputDocs(ctx, items, docs, final)
		}()
	}
	// NOTE: non-generic code starts
	switch j.CurrentCategory {
	case "repository":
		return j.GitHubRepositoryEnrichItemsFunc(ctx, items, docs, final)
		// FIXME
		//case "issue":
		//	return j.GitHubIssueEnrichItemsFunc(ctx, items, docs)
		//case "pull_request":
		//	return j.GitHubPullRequestEnrichItemsFunc(ctx, items, docs)
	}
	return
}

// SyncCurrentCategory - sync GitHub data source for current category
func (j *DSGitHub) SyncCurrentCategory(ctx *shared.Ctx) (err error) {
	switch j.CurrentCategory {
	case "repository":
		return j.FetchItemsRepository(ctx)
	case "issue":
		return j.FetchItemsIssue(ctx)
		// FIXME
		//case "pull_request":
		//	return j.FetchItemsPullRequest(ctx)
	}
	return
}

// Sync - sync GitHub data source
func (j *DSGitHub) Sync(ctx *shared.Ctx, category string) (err error) {
	_, ok := GitHubCategories[category]
	if !ok {
		err = fmt.Errorf("Unknown category '%s', known categories: %v", category, GitHubCategories)
		return
	}
	j.CurrentCategory = category
	var zeroDt time.Time
	gMaxUpstreamDtMtx.Lock()
	gMaxUpstreamDt = zeroDt
	gMaxUpstreamDtMtx.Unlock()
	if ctx.DateFrom != nil {
		shared.Printf("%s fetching from %v (%d threads)\n", j.Endpoint(), ctx.DateFrom, j.ThrN)
	}
	if ctx.DateFrom == nil {
		ctx.DateFrom = shared.GetLastUpdate(ctx, j.Endpoint())
		if ctx.DateFrom != nil {
			shared.Printf("%s resuming from %v (%d threads)\n", j.Endpoint(), ctx.DateFrom, j.ThrN)
		}
	}
	if ctx.DateTo != nil {
		shared.Printf("%s fetching till %v (%d threads)\n", j.Endpoint(), ctx.DateTo, j.ThrN)
	}
	// NOTE: Non-generic starts here
	err = j.SyncCurrentCategory(ctx)
	// NOTE: Non-generic ends here
	gMaxUpstreamDtMtx.Lock()
	defer gMaxUpstreamDtMtx.Unlock()
	shared.SetLastUpdate(ctx, j.Endpoint(), gMaxUpstreamDt)
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
	// FIXME
	// source := data.DataSource.Slug
	switch j.CurrentCategory {
	case "repository":
		endpoint.Category = j.CurrentCategory
		for _, iDoc := range docs {
			doc, _ := iDoc.(map[string]interface{})
			//shared.Printf("%s: %+v\n", source, doc)
			updatedOn := j.ItemUpdatedOn(doc)
			id, _ := doc["id"].(float64)
			forks, _ := doc["forks_count"].(float64)
			subscribers, _ := doc["subscribers_count"].(float64)
			stargazers, _ := doc["stargazers_count"].(float64)
			// Event
			event := &models.Event{
				CodeChangeRequest: nil,
				Issue:             nil,
				Repository: &models.RepositoryStatus{
					ID:           int64(id),
					URL:          j.URL,
					CalculatedAt: strfmt.DateTime(updatedOn),
					Forks:        int64(forks),
					Subscribers:  int64(subscribers),
					Stargazers:   int64(stargazers),
				},
			}
			data.Events = append(data.Events, event)
			gMaxUpstreamDtMtx.Lock()
			if updatedOn.After(gMaxUpstreamDt) {
				gMaxUpstreamDt = updatedOn
			}
			gMaxUpstreamDtMtx.Unlock()
		}
		// FIXME
		//case "issue":
		//case "pull_request":
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
	for cat := range ctx.Categories {
		err = github.Sync(&ctx, cat)
		if err != nil {
			shared.Printf("Error: %+v\n", err)
			return
		}
	}
}
