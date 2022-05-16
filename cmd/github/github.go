package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LF-Engineering/insights-datasource-shared/cryptography"
	"github.com/LF-Engineering/lfx-event-schema/service"
	"github.com/LF-Engineering/lfx-event-schema/service/insights"
	"github.com/LF-Engineering/lfx-event-schema/service/repository"
	"github.com/LF-Engineering/lfx-event-schema/service/user"

	"github.com/LF-Engineering/lfx-event-schema/utils/datalake"

	igh "github.com/LF-Engineering/lfx-event-schema/service/insights/github"

	"github.com/LF-Engineering/dev-analytics-libraries/emoji"

	shared "github.com/LF-Engineering/insights-datasource-shared"
	elastic "github.com/LF-Engineering/insights-datasource-shared/elastic"
	logger "github.com/LF-Engineering/insights-datasource-shared/ingestjob"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/go-github/v43/github"

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
	// WantEnrichPullRequestCommits - do we want to create rich documents for pull request commits (it contains identity data too).
	WantEnrichPullRequestCommits = true
	// WantIssuePullRequestCommentsOnIssue - do we want to fetch pull request's issue part comments (pull requests are issues too) on the issue object?
	WantIssuePullRequestCommentsOnIssue = true
	// WantIssuePullRequestCommentsOnPullRequest - do we want to fetch pull request's issue part comments (pull requests are issues too) on the pull request object?
	WantIssuePullRequestCommentsOnPullRequest = true
	// GitHubDataSource - constant for github source
	GitHubDataSource = "github"
	// GitHubRepositoryDefaultStream - Stream To Publish repo stats
	GitHubRepositoryDefaultStream = "PUT-S3-github-repository-stats"
	// GitHubIssueDefaultStream - Stream To Publish issues
	GitHubIssueDefaultStream = "PUT-S3-github-issues"
	// GitHubPullRequestDefaultStream - Stream To Publish pull requests
	GitHubPullRequestDefaultStream = "PUT-S3-github-pull-requests"
	// GitHubConnector ...
	GitHubConnector = "github-connector"
)

var (
	// GitHubCategories - categories defined for GitHub
	GitHubCategories = map[string]struct{}{"issue": {}, "pull_request": {}, "repository": {}}
	// GitHubIssueRoles - roles to fetch affiliation data for github issue
	GitHubIssueRoles = []string{"user_data", "assignee_data", "closed_by_data"}
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
	// GitHubPullRequestCommitRoles - roles to fetch affiliation data for github pull request commit
	GitHubPullRequestCommitRoles = []string{"author_data", "committer_data"}
	gMaxUpstreamDt               time.Time
	gMaxUpstreamDtMtx            = &sync.Mutex{}
)

// Publisher - for streaming data to Kinesis
type Publisher interface {
	PushEvents(action, source, eventType, subEventType, env string, data []interface{}) error
}

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
	FlagStream    *string
	FlagSourceID  *string
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
	EmojisMtx                       *sync.RWMutex
	Emojis                          map[string]string
	// Publisher & stream
	Publisher
	Stream string // stream to publish the data
	Logger logger.Logger
	// SourceID: the optional external source identifier (such as the repo ID from github/gitlab, or gerrit project slug)
	// this field is required for github, gitlab and gerrit. For github and gitlab, this is typically a numeric value
	// converted to a string such as 194341141. For gerrit this is the project (repository) slug.
	SourceID string
}

// AddPublisher - sets Kinesis publisher
func (j *DSGitHub) AddPublisher(publisher Publisher) {
	j.Publisher = publisher
}

// PublisherPushEvents - this is a fake function to test publisher locally
// FIXME: don't use when done implementing
func (j *DSGitHub) PublisherPushEvents(ev, ori, src, cat, env string, v []interface{}) error {
	data, err := jsoniter.Marshal(v)
	shared.Printf("publish[ev=%s ori=%s src=%s cat=%s env=%s]: %d items: %+v -> %v\n", ev, ori, src, cat, env, len(v), string(data), err)
	return nil
}

// AddLogger - adds logger
func (j *DSGitHub) AddLogger(ctx *shared.Ctx) {
	client, err := elastic.NewClientProvider(&elastic.Params{
		URL:      os.Getenv("ELASTIC_LOG_URL"),
		Password: os.Getenv("ELASTIC_LOG_PASSWORD"),
		Username: os.Getenv("ELASTIC_LOG_USER"),
	})
	if err != nil {
		shared.Printf("AddLogger error: %+v", err)
		return
	}
	logProvider, err := logger.NewLogger(client, os.Getenv("STAGE"))
	if err != nil {
		shared.Printf("AddLogger error: %+v", err)
		return
	}
	j.Logger = *logProvider
}

// WriteLog - writes to log
func (j *DSGitHub) WriteLog(ctx *shared.Ctx, timestamp time.Time, status, message string) {
	_ = j.Logger.Write(&logger.Log{
		Connector: GitHubDataSource,
		Configuration: []map[string]string{
			{
				"GITHUB_ORG":  j.Org,
				"GITHUB_REPO": j.Repo,
				"REPO_URL":    j.URL,
				"ProjectSlug": ctx.Project,
			}},
		Status:    status,
		CreatedAt: timestamp,
		Message:   message,
	})
}

// AddFlags - add GitHub specific flags
func (j *DSGitHub) AddFlags() {
	j.FlagOrg = flag.String("github-org", "", "GitHub org, example cncf")
	j.FlagRepo = flag.String("github-repo", "", "GitHub repo, example devstats")
	j.FlagTokens = flag.String("github-tokens", "", "\",\" separated list of OAuth tokens")
	j.FlagCachePath = flag.String("github-cache-path", GitHubDefaultCachePath, "path to store github users cache, defaults to"+GitHubDefaultCachePath)
	j.FlagStream = flag.String("github-stream", GitHubIssueDefaultStream, "github kinesis stream name, for example PUT-S3-github-issues")
	j.FlagSourceID = flag.String("github-source-id", "", "repository id value from the github api")
}

// ParseArgs - parse GitHub specific environment variables
func (j *DSGitHub) ParseArgs(ctx *shared.Ctx) (err error) {
	decrypt, err := cryptography.NewEncryptionClient()
	if err != nil {
		return err
	}
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
		tokenDecrypted, err := decrypt.Decrypt(j.Tokens)
		if err != nil {
			return err
		}

		// decrypted tokens
		j.Tokens = tokenDecrypted
		shared.AddRedacted(j.Tokens, false)
	}

	// github cache path
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

	// github Kinesis stream
	j.Stream = GitHubIssueDefaultStream
	if len(j.Categories) == 1 {
		_, ok := ctx.Categories["pull_request"]
		if ok {
			j.Stream = GitHubPullRequestDefaultStream
		} else {
			_, ok := ctx.Categories["repository"]
			if ok {
				j.Stream = GitHubRepositoryDefaultStream
			}
		}
	}
	if shared.FlagPassed(ctx, "stream") {
		j.Stream = *j.FlagStream
	}
	if ctx.EnvSet("STREAM") {
		j.Stream = ctx.Env("STREAM")
	}

	// github repository sourceID
	if shared.FlagPassed(ctx, "source-id") {
		j.SourceID = strings.TrimSpace(*j.FlagSourceID)
	}

	// FIXME
	// gGitHubDataSource.Categories = j.Categories
	// gGitHubMetaData.Project = ctx.Project
	// gGitHubMetaData.Tags = ctx.Tags
	return
}

// Validate - is current DS configuration OK?
func (j *DSGitHub) Validate(ctx *shared.Ctx) (err error) {
	if strings.TrimSpace(j.SourceID) == "" {
		return fmt.Errorf("github-source-id must be set")
	}

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
	j.URL = strings.TrimSpace(GitHubURLRoot + j.Org + "/" + j.Repo)
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
	j.Emojis = make(map[string]string)
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
		j.EmojisMtx = &sync.RWMutex{}
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
		r := &repository.RepositoryObjectBase{}
		i := &igh.Issue{}
		p := &igh.PullRequest{}
		shared.Printf("GitHub: %+v\nshared context: %s\nModels: [%+v, %+v, %+v]\n", j, ctx.Info(), r, i, p)
	}
	if ctx.Debug > 0 {
		shared.Printf("stream: '%s'\n", j.Stream)
	}
	if j.Stream != "" {
		sess, err := session.NewSession()
		if err != nil {
			return err
		}
		s3Client := s3.New(sess)
		objectStore := datalake.NewS3ObjectStore(s3Client)
		datalakeClient := datalake.NewStoreClient(&objectStore)
		j.AddPublisher(&datalakeClient)
	}
	j.AddLogger(ctx)
	return
}

// Endpoint - return unique endpoint string representation
func (j *DSGitHub) Endpoint() string {
	if j.CurrentCategory == "" {
		return j.URL
	}
	return j.URL + " " + j.CurrentCategory
}

func (j *DSGitHub) emojiForContent(content string) string {
	if content == "" {
		return ""
	}
	if j.EmojisMtx != nil {
		j.EmojisMtx.RLock()
	}
	emojiContent, found := j.Emojis[content]
	if j.EmojisMtx != nil {
		j.EmojisMtx.RUnlock()
	}
	if found {
		return emojiContent
	}
	// +1, -1, laugh, confused, heart, hooray, rocket, eyes
	switch content {
	case "laugh":
		content = "laughing"
	case "hooray":
		content = "tada"
	}
	emojiContent = emoji.GetEmojiUnicode(":" + content + ":")
	if j.EmojisMtx != nil {
		j.EmojisMtx.Lock()
	}
	j.Emojis[content] = emojiContent
	if j.EmojisMtx != nil {
		j.EmojisMtx.Unlock()
	}
	return emojiContent
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

func (j *DSGitHub) githubIssues(ctx *shared.Ctx, org, repo string, since, until *time.Time) (issuesData []map[string]interface{}, err error) {
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
	// GitHub doesn't support date-to/until
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

func (j *DSGitHub) githubPull(ctx *shared.Ctx, org, repo string, number int) (pullData map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubPull {
		if j.GitHubPullMtx != nil {
			j.GitHubPullMtx.RLock()
		}
		pullData, found = j.GitHubPull[key]
		if j.GitHubPullMtx != nil {
			j.GitHubPullMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("pull found in cache: %+v\n", pullData)
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
			pull     *github.PullRequest
			e        error
		)
		pull, response, e = c.PullRequests.Get(j.Context, org, repo, number)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, pull, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPull {
				if j.GitHubPullMtx != nil {
					j.GitHubPullMtx.Lock()
				}
				j.GitHubPull[key] = nil
				if j.GitHubPullMtx != nil {
					j.GitHubPullMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubPull: pull not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s pull: response: %+v, because: %+v, retrying rate\n", key, response, e)
			shared.Printf("githubPull: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get pull %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get pull %s) waiting 1s before token switch\n", key)
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
		jm, _ := jsoniter.Marshal(pull)
		_ = jsoniter.Unmarshal(jm, &pullData)
		body, ok := shared.Dig(pullData, []string{"body"}, false, true)
		if ok {
			nBody := len(body.(string))
			if nBody > MaxPullBodyLength {
				pullData["body"] = body.(string)[:MaxPullBodyLength]
			}
		}
		pullData["body_analyzed"], _ = pullData["body"]
		if ctx.Debug > 2 {
			shared.Printf("pull got from API: %+v\n", pullData)
		}
		break
	}
	if CacheGitHubPull {
		if j.GitHubPullMtx != nil {
			j.GitHubPullMtx.Lock()
		}
		j.GitHubPull[key] = pullData
		if j.GitHubPullMtx != nil {
			j.GitHubPullMtx.Unlock()
		}
	}
	return
}

// githubPullsFromIssues - consider fetching this data in a stream-like mode to avoid a need of pulling all data and then of everything at once
func (j *DSGitHub) githubPullsFromIssues(ctx *shared.Ctx, org, repo string, since, until *time.Time) (pullsData []map[string]interface{}, err error) {
	var (
		issues []map[string]interface{}
		pull   map[string]interface{}
		ok     bool
	)
	issues, err = j.githubIssues(ctx, org, repo, since, until)
	if err != nil {
		return
	}
	i, pulls := 0, 0
	nIssues := len(issues)
	shared.Printf("%s/%s: processing %d issues (to filter for PRs)\n", j.URL, j.CurrentCategory, nIssues)
	if j.ThrN > 1 {
		nThreads := 0
		ch := make(chan interface{})
		for _, issue := range issues {
			i++
			if i%ItemsPerPage == 0 {
				runtime.GC()
				shared.Printf("%s/%s: processing %d/%d issues, %d pulls so far\n", j.URL, j.CurrentCategory, i, nIssues, pulls)
			}
			isPR, _ := issue["is_pull"]
			if !isPR.(bool) {
				continue
			}
			pulls++
			number, _ := issue["number"]
			go func(ch chan interface{}, num int) {
				pr, e := j.githubPull(ctx, org, repo, num)
				if e != nil {
					ch <- e
					return
				}
				ch <- pr
			}(ch, int(number.(float64)))
			nThreads++
			if nThreads == j.ThrN {
				obj := <-ch
				nThreads--
				err, ok = obj.(error)
				if ok {
					return
				}
				pullsData = append(pullsData, obj.(map[string]interface{}))
			}
		}
		for nThreads > 0 {
			obj := <-ch
			nThreads--
			err, ok = obj.(error)
			if ok {
				return
			}
			pullsData = append(pullsData, obj.(map[string]interface{}))
		}
	} else {
		for _, issue := range issues {
			i++
			if i%ItemsPerPage == 0 {
				runtime.GC()
				shared.Printf("%s/%s: processed %d/%d issues, %d pulls so far\n", j.URL, j.CurrentCategory, i, nIssues, pulls)
			}
			isPR, _ := issue["is_pull"]
			if !isPR.(bool) {
				continue
			}
			pulls++
			number, _ := issue["number"]
			pull, err = j.githubPull(ctx, org, repo, int(number.(float64)))
			if err != nil {
				return
			}
			pullsData = append(pullsData, pull)
		}
	}
	return
}

func (j *DSGitHub) githubPulls(ctx *shared.Ctx, org, repo string, since, until *time.Time) (pullsData []map[string]interface{}, err error) {
	// since & until params are ignored
	// WARNING: this is not returning all possible Pull sub fields, recommend to use githubPullsFromIssues instead.
	var found bool
	origin := org + "/" + repo
	// Try memory cache 1st
	if CacheGitHubPulls {
		if j.GitHubPullsMtx != nil {
			j.GitHubPullsMtx.RLock()
		}
		pullsData, found = j.GitHubPulls[origin]
		if j.GitHubPullsMtx != nil {
			j.GitHubPullsMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("pulls found in cache: %+v\n", pullsData)
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
	opt := &github.PullRequestListOptions{
		State:     "all",
		Sort:      "updated",
		Direction: "asc",
	}
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			pulls    []*github.PullRequest
			e        error
		)
		pulls, response, e = c.PullRequests.List(j.Context, org, repo, opt)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, pulls, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPulls {
				if j.GitHubPullsMtx != nil {
					j.GitHubPullsMtx.Lock()
				}
				j.GitHubPulls[origin] = []map[string]interface{}{}
				if j.GitHubPullsMtx != nil {
					j.GitHubPullsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubPulls: pulls not found %s: %v\n", origin, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s pulls: response: %+v, because: %+v, retrying rate\n", origin, response, e)
			shared.Printf("githubPulls: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get pulls %s), waiting for %ds\n", origin, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get pulls %s) waiting 1s before token switch\n", origin)
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
		for _, pull := range pulls {
			pr := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(pull)
			_ = jsoniter.Unmarshal(jm, &pr)
			body, ok := shared.Dig(pr, []string{"body"}, false, true)
			if ok {
				nBody := len(body.(string))
				if nBody > MaxPullBodyLength {
					pr["body"] = body.(string)[:MaxPullBodyLength]
				}
			}
			pr["body_analyzed"], _ = pr["body"]
			pullsData = append(pullsData, pr)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			shared.Printf("%s/%s: processing next pulls page: %d\n", j.URL, j.CurrentCategory, opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		shared.Printf("pulls got from API: %+v\n", pullsData)
	}
	if CacheGitHubPulls {
		if j.GitHubPullsMtx != nil {
			j.GitHubPullsMtx.Lock()
		}
		j.GitHubPulls[origin] = pullsData
		if j.GitHubPullsMtx != nil {
			j.GitHubPullsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubPullReviews(ctx *shared.Ctx, org, repo string, number int) (reviews []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubPullReviews {
		if j.GitHubPullReviewsMtx != nil {
			j.GitHubPullReviewsMtx.RLock()
		}
		reviews, found = j.GitHubPullReviews[key]
		if j.GitHubPullReviewsMtx != nil {
			j.GitHubPullReviewsMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("pull reviews found in cache: %+v\n", reviews)
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
			revs     []*github.PullRequestReview
			e        error
		)
		revs, response, e = c.PullRequests.ListReviews(j.Context, org, repo, number, opt)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, revs, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPullReviews {
				if j.GitHubPullReviewsMtx != nil {
					j.GitHubPullReviewsMtx.Lock()
				}
				j.GitHubPullReviews[key] = []map[string]interface{}{}
				if j.GitHubPullReviewsMtx != nil {
					j.GitHubPullReviewsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubPullReviews: reviews not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s pull reviews: response: %+v, because: %+v, retrying rate\n", key, response, e)
			shared.Printf("githubPullReviews: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get pull reviews %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get pull reviews %s) waiting 1s before token switch\n", key)
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
		for _, review := range revs {
			rev := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(review)
			_ = jsoniter.Unmarshal(jm, &rev)
			body, ok := shared.Dig(rev, []string{"body"}, false, true)
			if ok {
				nBody := len(body.(string))
				if nBody > MaxReviewBodyLength {
					rev["body"] = body.(string)[:MaxReviewBodyLength]
				}
			}
			rev["body_analyzed"], _ = rev["body"]
			userLogin, ok := shared.Dig(rev, []string{"user", "login"}, false, true)
			if ok {
				rev["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
				if err != nil {
					return
				}
			}
			reviews = append(reviews, rev)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			shared.Printf("processing next pull reviews page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		shared.Printf("pull reviews got from API: %+v\n", reviews)
	}
	if CacheGitHubPullReviews {
		if j.GitHubPullReviewsMtx != nil {
			j.GitHubPullReviewsMtx.Lock()
		}
		j.GitHubPullReviews[key] = reviews
		if j.GitHubPullReviewsMtx != nil {
			j.GitHubPullReviewsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubPullReviewComments(ctx *shared.Ctx, org, repo string, number int) (reviewComments []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubPullReviewComments {
		if j.GitHubPullReviewCommentsMtx != nil {
			j.GitHubPullReviewCommentsMtx.RLock()
		}
		reviewComments, found = j.GitHubPullReviewComments[key]
		if j.GitHubPullReviewCommentsMtx != nil {
			j.GitHubPullReviewCommentsMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("pull review comments found in cache: %+v\n", reviewComments)
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
	opt := &github.PullRequestListCommentsOptions{
		Sort:      "updated",
		Direction: "asc",
	}
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			revComms []*github.PullRequestComment
			e        error
		)
		revComms, response, e = c.PullRequests.ListComments(j.Context, org, repo, number, opt)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, revComms, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPullReviewComments {
				if j.GitHubPullReviewCommentsMtx != nil {
					j.GitHubPullReviewCommentsMtx.Lock()
				}
				j.GitHubPullReviewComments[key] = []map[string]interface{}{}
				if j.GitHubPullReviewCommentsMtx != nil {
					j.GitHubPullReviewCommentsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubPullReviewComments: review comments not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s pull review comments: response: %+v, because: %+v, retrying rate\n", key, response, e)
			shared.Printf("githubPullReviewComments: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get pull review comments %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get pull review comments %s) waiting 1s before token switch\n", key)
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
		for _, reviewComment := range revComms {
			revComm := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(reviewComment)
			_ = jsoniter.Unmarshal(jm, &revComm)
			body, ok := shared.Dig(revComm, []string{"body"}, false, true)
			if ok {
				nBody := len(body.(string))
				if nBody > MaxReviewCommentBodyLength {
					revComm["body"] = body.(string)[:MaxReviewCommentBodyLength]
				}
			}
			revComm["body_analyzed"], _ = revComm["body"]
			userLogin, ok := shared.Dig(revComm, []string{"user", "login"}, false, true)
			if ok {
				revComm["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
				if err != nil {
					return
				}
			}
			iCnt, ok := shared.Dig(revComm, []string{"reactions", "total_count"}, false, true)
			if ok {
				revComm["reactions_data"] = []interface{}{}
				cnt := int(iCnt.(float64))
				if cnt > 0 {
					cid, ok := shared.Dig(revComm, []string{"id"}, false, true)
					if ok {
						revComm["reactions_data"], err = j.githubReviewCommentReactions(ctx, org, repo, int64(cid.(float64)))
						if err != nil {
							return
						}
					}
				}
			}
			reviewComments = append(reviewComments, revComm)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			shared.Printf("processing next pull review comments page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		shared.Printf("pull review comments got from API: %+v\n", reviewComments)
	}
	if CacheGitHubPullReviewComments {
		if j.GitHubPullReviewCommentsMtx != nil {
			j.GitHubPullReviewCommentsMtx.Lock()
		}
		j.GitHubPullReviewComments[key] = reviewComments
		if j.GitHubPullReviewCommentsMtx != nil {
			j.GitHubPullReviewCommentsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubReviewCommentReactions(ctx *shared.Ctx, org, repo string, cid int64) (reactions []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, cid)
	if ctx.Debug > 1 {
		shared.Printf("githubReviewCommentReactions %s\n", key)
	}
	// Try memory cache 1st
	if CacheGitHubReviewCommentReactions {
		if j.GitHubReviewCommentReactionsMtx != nil {
			j.GitHubReviewCommentReactionsMtx.RLock()
		}
		reactions, found = j.GitHubReviewCommentReactions[key]
		if j.GitHubReviewCommentReactionsMtx != nil {
			j.GitHubReviewCommentReactionsMtx.RUnlock()
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
		reacts, response, e = c.Reactions.ListPullRequestCommentReactions(j.Context, org, repo, cid, opt)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, cid, reacts, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubReviewCommentReactions {
				if j.GitHubReviewCommentReactionsMtx != nil {
					j.GitHubReviewCommentReactionsMtx.Lock()
				}
				j.GitHubReviewCommentReactions[key] = []map[string]interface{}{}
				if j.GitHubReviewCommentReactionsMtx != nil {
					j.GitHubReviewCommentReactionsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubReviewCommentReactions: reactions not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s comment reactions: response: %+v, because: %+v, retrying rate\n", key, response, e)
			shared.Printf("githubReviewCommentReactions: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get pull comment reactions %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get pull comment reactions %s) waiting 1s before token switch\n", key)
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
			shared.Printf("processing next pull review comment reactions page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		shared.Printf("review comment reactions got from API: %+v\n", reactions)
	}
	if CacheGitHubReviewCommentReactions {
		if j.GitHubReviewCommentReactionsMtx != nil {
			j.GitHubReviewCommentReactionsMtx.Lock()
		}
		j.GitHubReviewCommentReactions[key] = reactions
		if j.GitHubReviewCommentReactionsMtx != nil {
			j.GitHubReviewCommentReactionsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubPullRequestedReviewers(ctx *shared.Ctx, org, repo string, number int) (reviewers []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubPullRequestedReviewers {
		if j.GitHubPullRequestedReviewersMtx != nil {
			j.GitHubPullRequestedReviewersMtx.RLock()
		}
		reviewers, found = j.GitHubPullRequestedReviewers[key]
		if j.GitHubPullRequestedReviewersMtx != nil {
			j.GitHubPullRequestedReviewersMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("pull requested reviewers found in cache: %+v\n", reviewers)
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
			revsObj  *github.Reviewers
			e        error
		)
		revsObj, response, e = c.PullRequests.ListReviewers(j.Context, org, repo, number, opt)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, revsObj, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPullRequestedReviewers {
				if j.GitHubPullRequestedReviewersMtx != nil {
					j.GitHubPullRequestedReviewersMtx.Lock()
				}
				j.GitHubPullRequestedReviewers[key] = []map[string]interface{}{}
				if j.GitHubPullRequestedReviewersMtx != nil {
					j.GitHubPullRequestedReviewersMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubPullRequestedReviewers: reviewers not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s pull requested reviewers: response: %+v, because: %+v, retrying rate\n", key, response, e)
			shared.Printf("githubPullRequestedReviewers: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get pull requested reviewers %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get pull requested reviewers %s) waiting 1s before token switch\n", key)
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
		users := revsObj.Users
		for _, reviewer := range users {
			if reviewer == nil || reviewer.Login == nil {
				continue
			}
			var userData map[string]interface{}
			userData, _, err = j.githubUser(ctx, *reviewer.Login)
			if err != nil {
				return
			}
			reviewers = append(reviewers, userData)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			shared.Printf("processing next pull requested reviewers page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		shared.Printf("pull requested reviewers got from API: %+v\n", reviewers)
	}
	if CacheGitHubPullRequestedReviewers {
		if j.GitHubPullRequestedReviewersMtx != nil {
			j.GitHubPullRequestedReviewersMtx.Lock()
		}
		j.GitHubPullRequestedReviewers[key] = reviewers
		if j.GitHubPullRequestedReviewersMtx != nil {
			j.GitHubPullRequestedReviewersMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubPullCommits(ctx *shared.Ctx, org, repo string, number int, deep bool) (commits []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubPullCommits {
		if j.GitHubPullCommitsMtx != nil {
			j.GitHubPullCommitsMtx.RLock()
		}
		commits, found = j.GitHubPullCommits[key]
		if j.GitHubPullCommitsMtx != nil {
			j.GitHubPullCommitsMtx.RUnlock()
		}
		if found {
			if ctx.Debug > 2 {
				shared.Printf("pull commits found in cache: %+v\n", commits)
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
			comms    []*github.RepositoryCommit
			e        error
		)
		comms, response, e = c.PullRequests.ListCommits(j.Context, org, repo, number, opt)
		if ctx.Debug > 2 {
			shared.Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, comms, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPullCommits {
				if j.GitHubPullCommitsMtx != nil {
					j.GitHubPullCommitsMtx.Lock()
				}
				j.GitHubPullCommits[key] = []map[string]interface{}{}
				if j.GitHubPullCommitsMtx != nil {
					j.GitHubPullCommitsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				shared.Printf("githubPullCommits: commits not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			shared.Printf("Unable to get %s pull commits: response: %+v, because: %+v, retrying rate\n", key, response, e)
			shared.Printf("githubPullCommits: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				shared.Printf("GitHub detected abuse (get pull commits %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				shared.Printf("Rate limit reached on a token (get pull commits %s) waiting 1s before token switch\n", key)
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
		for _, commit := range comms {
			com := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(commit)
			_ = jsoniter.Unmarshal(jm, &com)
			if deep {
				userLogin, ok := shared.Dig(com, []string{"author", "login"}, false, true)
				if ok {
					com["author_data"], _, err = j.githubUser(ctx, userLogin.(string))
					if err != nil {
						return
					}
				}
				userLogin, ok = shared.Dig(com, []string{"committer", "login"}, false, true)
				if ok {
					com["committer_data"], _, err = j.githubUser(ctx, userLogin.(string))
					if err != nil {
						return
					}
				}
			}
			commits = append(commits, com)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			shared.Printf("processing next pull commits page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		shared.Printf("pull commits got from API: %+v\n", commits)
	}
	if CacheGitHubPullCommits {
		if j.GitHubPullCommitsMtx != nil {
			j.GitHubPullCommitsMtx.Lock()
		}
		j.GitHubPullCommits[key] = commits
		if j.GitHubPullCommitsMtx != nil {
			j.GitHubPullCommitsMtx.Unlock()
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
	// shared.Printf("%s\n", shared.PrettyPrint(inIssue))
	issue = inIssue
	issue["user_data"] = map[string]interface{}{}
	issue["assignee_data"] = map[string]interface{}{}
	issue["closed_by_data"] = map[string]interface{}{}
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
	closedByLogin, ok := shared.Dig(issue, []string{"closed_by", "login"}, false, true)
	if ok {
		issue["closed_by_data"], _, err = j.githubUser(ctx, closedByLogin.(string))
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
	isPull := false
	iIsPull, ok := shared.Dig(issue, []string{"is_pull"}, false, true)
	if ok {
		isPull, _ = iIsPull.(bool)
	}
	// For issues that are PRs we only get comments if WantIssuePullRequestCommentsOnIssue is set
	if ok && (!isPull || (isPull && WantIssuePullRequestCommentsOnIssue)) {
		issue["comments_data"], err = j.githubIssueComments(ctx, j.Org, j.Repo, int(number.(float64)))
		if err != nil {
			return
		}
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
	return
}

// ProcessPull - add PRs sub items
func (j *DSGitHub) ProcessPull(ctx *shared.Ctx, inPull map[string]interface{}) (pull map[string]interface{}, err error) {
	// shared.Printf("%s\n", shared.PrettyPrint(inPull))
	pull = inPull
	pull["user_data"] = map[string]interface{}{}
	pull["assignee_data"] = map[string]interface{}{}
	pull["merged_by_data"] = map[string]interface{}{}
	pull["review_comments_data"] = []interface{}{}
	pull["assignees_data"] = []interface{}{}
	pull["reviews_data"] = []interface{}{}
	pull["requested_reviewers_data"] = []interface{}{}
	pull["commits_data"] = []interface{}{}
	pull["comments_data"] = []interface{}{}
	// ["user", "review_comments", "requested_reviewers", "merged_by", "commits", "assignee", "assignees"]
	number, ok := shared.Dig(pull, []string{"number"}, false, true)
	if ok {
		iNumber := int(number.(float64))
		pull["reviews_data"], err = j.githubPullReviews(ctx, j.Org, j.Repo, iNumber)
		if err != nil {
			return
		}
		pull["review_comments_data"], err = j.githubPullReviewComments(ctx, j.Org, j.Repo, iNumber)
		if err != nil {
			return
		}
		if WantIssuePullRequestCommentsOnPullRequest {
			pull["comments_data"], err = j.githubIssueComments(ctx, j.Org, j.Repo, iNumber)
			if err != nil {
				return
			}
		}
		pull["requested_reviewers_data"], err = j.githubPullRequestedReviewers(ctx, j.Org, j.Repo, iNumber)
		if err != nil {
			return
		}
		pull["commits_data"], err = j.githubPullCommits(ctx, j.Org, j.Repo, iNumber, true)
		if err != nil {
			return
		}
	}
	userLogin, ok := shared.Dig(pull, []string{"user", "login"}, false, true)
	if ok {
		pull["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
		if err != nil {
			return
		}
	}
	mergedByLogin, ok := shared.Dig(pull, []string{"merged_by", "login"}, false, true)
	if ok {
		pull["merged_by_data"], _, err = j.githubUser(ctx, mergedByLogin.(string))
		if err != nil {
			return
		}
	}
	assigneeLogin, ok := shared.Dig(pull, []string{"assignee", "login"}, false, true)
	if ok {
		pull["assignee_data"], _, err = j.githubUser(ctx, assigneeLogin.(string))
		if err != nil {
			return
		}
	}
	iAssignees, ok := shared.Dig(pull, []string{"assignees"}, false, true)
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
		pull["assignees_data"] = assigneesAry
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
		issProcMtx   *sync.Mutex
	)
	if j.ThrN > 1 {
		ch = make(chan error)
		allIssuesMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
		issProcMtx = &sync.Mutex{}
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
		if issProcMtx != nil {
			issProcMtx.Lock()
		}
		issProc := issProcessed
		if issProcMtx != nil {
			issProcMtx.Unlock()
		}
		esItem["data"] = item
		if issProc%ItemsPerPage == 0 {
			shared.Printf("%s/%s: processed %d/%d issues\n", j.URL, j.CurrentCategory, issProc, nIss)
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
	issues, err := j.githubIssues(ctx, j.Org, j.Repo, ctx.DateFrom, ctx.DateTo)
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
				if issProcMtx != nil {
					issProcMtx.Lock()
				}
				issProcessed++
				if issProcMtx != nil {
					issProcMtx.Unlock()
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
			if issProcMtx != nil {
				issProcMtx.Lock()
			}
			issProcessed++
			if issProcMtx != nil {
				issProcMtx.Unlock()
			}
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

// FetchItemsPullRequest - implement raw issue data for GitHub datasource
func (j *DSGitHub) FetchItemsPullRequest(ctx *shared.Ctx) (err error) {
	// Process pull requests (possibly in threads)
	var (
		ch           chan error
		allDocs      []interface{}
		allPulls     []interface{}
		allPullsMtx  *sync.Mutex
		escha        []chan error
		eschaMtx     *sync.Mutex
		pullsProcMtx *sync.Mutex
	)
	if j.ThrN > 1 {
		ch = make(chan error)
		allPullsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
		pullsProcMtx = &sync.Mutex{}
	}
	nThreads, pullsProcessed, nPRs := 0, 0, 0
	processPull := func(c chan error, pull map[string]interface{}) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		item, err := j.ProcessPull(ctx, pull)
		shared.FatalOnError(err)
		esItem := j.AddMetadata(ctx, item)
		if ctx.Project != "" {
			item["project"] = ctx.Project
		}
		if pullsProcMtx != nil {
			pullsProcMtx.Lock()
		}
		pullsProc := pullsProcessed
		if pullsProcMtx != nil {
			pullsProcMtx.Unlock()
		}
		esItem["data"] = item
		if pullsProc%ItemsPerPage == 0 {
			shared.Printf("%s/%s: processed %d/%d pulls\n", j.URL, j.CurrentCategory, pullsProc, nPRs)
		}
		if allPullsMtx != nil {
			allPullsMtx.Lock()
		}
		allPulls = append(allPulls, esItem)
		nPulls := len(allPulls)
		if nPulls >= ctx.PackSize {
			sendToQueue := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = j.GitHubEnrichItems(ctx, allPulls, &allDocs, false)
				//ee = SendToQueue(ctx, j, true, UUID, allPulls)
				if ee != nil {
					shared.Printf("%s/%s: error %v sending %d pulls to queue\n", j.URL, j.CurrentCategory, ee, len(allPulls))
				}
				allPulls = []interface{}{}
				if allPullsMtx != nil {
					allPullsMtx.Unlock()
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
			if allPullsMtx != nil {
				allPullsMtx.Unlock()
			}
		}
		return
	}
	var pulls []map[string]interface{}
	// PullRequests.List doesn't return merged_by data, we need to use PullRequests.Get on each pull
	// If it would we could use Pulls API to fetch all pulls when no date from is specified
	// If there is a date from Pulls API doesn't support Since parameter
	// if ctx.DateFrom != nil {
	if 1 == 1 {
		pulls, err = j.githubPullsFromIssues(ctx, j.Org, j.Repo, ctx.DateFrom, ctx.DateTo)
	} else {
		pulls, err = j.githubPulls(ctx, j.Org, j.Repo, ctx.DateFrom, ctx.DateTo)
	}
	shared.FatalOnError(err)
	runtime.GC()
	nPRs = len(pulls)
	shared.Printf("%s/%s: got %d pulls\n", j.URL, j.CurrentCategory, nPRs)
	if j.ThrN > 1 {
		for _, pull := range pulls {
			go func(pr map[string]interface{}) {
				var (
					e    error
					esch chan error
				)
				esch, e = processPull(ch, pr)
				if e != nil {
					shared.Printf("%s/%s: pulls process error: %v\n", j.URL, j.CurrentCategory, e)
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
			}(pull)
			nThreads++
			if nThreads == j.ThrN {
				err = <-ch
				if err != nil {
					return
				}
				if pullsProcMtx != nil {
					pullsProcMtx.Lock()
				}
				pullsProcessed++
				if pullsProcMtx != nil {
					pullsProcMtx.Unlock()
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
			if pullsProcMtx != nil {
				pullsProcMtx.Lock()
			}
			pullsProcessed++
			if pullsProcMtx != nil {
				pullsProcMtx.Unlock()
			}
		}
	} else {
		for _, pull := range pulls {
			_, err = processPull(nil, pull)
			if err != nil {
				return
			}
			pullsProcessed++
		}
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			return
		}
	}
	nPulls := len(allPulls)
	if ctx.Debug > 0 {
		shared.Printf("%d remaining pulls to send to queue\n", nPulls)
	}
	err = j.GitHubEnrichItems(ctx, allPulls, &allDocs, true)
	//err = SendToQueue(ctx, j, true, UUID, allPulls)
	if err != nil {
		shared.Printf("%s/%s: error %v sending %d pulls to queue\n", j.URL, j.CurrentCategory, err, len(allPulls))
	}
	return
}

// GetRoles - return identities for given roles
func (j *DSGitHub) GetRoles(ctx *shared.Ctx, item map[string]interface{}, roles []string, dt time.Time) (identities []map[string]interface{}) {
	for _, role := range roles {
		identity := j.GetRoleIdentity(ctx, item, role)
		if identity == nil || len(identity) == 0 {
			continue
		}
		identity["dt"] = dt
		identities = append(identities, identity)
	}
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSGitHub) GetRoleIdentity(ctx *shared.Ctx, item map[string]interface{}, role string) (identity map[string]interface{}) {
	usr, ok := item[role]
	if ok && usr != nil && len(usr.(map[string]interface{})) > 0 {
		ident := j.IdentityForObject(ctx, usr.(map[string]interface{}))
		identity = map[string]interface{}{
			"name":       ident[0],
			"username":   ident[1],
			"email":      ident[2],
			"avatar_url": ident[3],
			"role":       role,
			"site_admin": ident[4] != "",
		}
	}
	return
}

// IdentityForObject - construct identity from a given object
func (j *DSGitHub) IdentityForObject(ctx *shared.Ctx, item map[string]interface{}) (identity [5]string) {
	if ctx.Debug > 1 {
		defer func() {
			shared.Printf("%s/%s: IdentityForObject: %+v -> %+v\n", j.URL, j.CurrentCategory, item, identity)
		}()
	}
	for i, prop := range []string{"name", "login", "email", "avatar_url"} {
		iVal, ok := shared.Dig(item, []string{prop}, false, true)
		if ok {
			val, ok := iVal.(string)
			if ok {
				identity[i] = val
			}
		} else {
			identity[i] = ""
		}
	}
	bVal, ok := shared.Dig(item, []string{"site_admin"}, false, true)
	if ok {
		val, ok := bVal.(bool)
		if ok && val {
			identity[4] = "1"
		}
	}
	return
}

// GetFirstIssueAttention - get first non-author action date on the issue
func (j *DSGitHub) GetFirstIssueAttention(issue map[string]interface{}) (dt time.Time) {
	iUserLogin, _ := shared.Dig(issue, []string{"user", "login"}, false, true)
	userLogin, _ := iUserLogin.(string)
	dts := []time.Time{}
	udts := []time.Time{}
	iComments, ok := issue["comments_data"]
	if ok && iComments != nil {
		ary, _ := iComments.([]interface{})
		for _, iComment := range ary {
			comment, _ := iComment.(map[string]interface{})
			iCommentLogin, _ := shared.Dig(comment, []string{"user", "login"}, false, true)
			commentLogin, _ := iCommentLogin.(string)
			iCreatedAt, _ := comment["created_at"]
			createdAt, _ := shared.TimeParseInterfaceString(iCreatedAt)
			if userLogin == commentLogin {
				udts = append(udts, createdAt)
				continue
			}
			dts = append(dts, createdAt)
		}
	}
	// NOTE: p2o does it but reactions API doesn't have any datetimefield specifying when reaction was made
	/*
		iReactions, ok := issue["reactions_data"]
		if ok && iReactions != nil {
			ary, _ := iReactions.([]interface{})
			for _, iReaction := range ary {
				reaction, _ := iReaction.(map[string]interface{})
				iReactionLogin, _ := shared.Dig(reaction, []string{"user", "login"}, false, true)
				reactionLogin, _ := iReactionLogin.(string)
				if userLogin == reactionLogin {
					continue
				}
				iCreatedAt, _ := reaction["created_at"]
				createdAt, _ := TimeParseInterfaceString(iCreatedAt)
				dts = append(dts, createdAt)
			}
		}
	*/
	nDts := len(dts)
	if nDts == 0 {
		// If there was no action of anybody else that author's, then fallback to author's actions
		dts = udts
		nDts = len(dts)
	}
	switch nDts {
	case 0:
		dt = time.Now()
	case 1:
		dt = dts[0]
	default:
		sort.Slice(dts, func(i, j int) bool {
			return dts[i].Before(dts[j])
		})
		dt = dts[0]
	}
	return
}

// GetFirstPullRequestReviewDate - get first review date on a pull request
func (j *DSGitHub) GetFirstPullRequestReviewDate(pull map[string]interface{}, commsAndReviews bool) (dt time.Time) {
	iUserLogin, _ := shared.Dig(pull, []string{"user", "login"}, false, true)
	userLogin, _ := iUserLogin.(string)
	dts := []time.Time{}
	udts := []time.Time{}
	iReviews, ok := pull["review_comments_data"]
	if ok && iReviews != nil {
		ary, _ := iReviews.([]interface{})
		for _, iReview := range ary {
			review, _ := iReview.(map[string]interface{})
			iReviewLogin, _ := shared.Dig(review, []string{"user", "login"}, false, true)
			reviewLogin, _ := iReviewLogin.(string)
			iCreatedAt, _ := review["created_at"]
			createdAt, _ := shared.TimeParseInterfaceString(iCreatedAt)
			if userLogin == reviewLogin {
				udts = append(udts, createdAt)
				continue
			}
			dts = append(dts, createdAt)
		}
	}
	if commsAndReviews {
		iReviews, ok := pull["reviews_data"]
		if ok && iReviews != nil {
			ary, _ := iReviews.([]interface{})
			for _, iReview := range ary {
				review, _ := iReview.(map[string]interface{})
				iReviewLogin, _ := shared.Dig(review, []string{"user", "login"}, false, true)
				reviewLogin, _ := iReviewLogin.(string)
				iSubmittedAt, _ := review["submitted_at"]
				submittedAt, _ := shared.TimeParseInterfaceString(iSubmittedAt)
				if userLogin == reviewLogin {
					udts = append(udts, submittedAt)
					continue
				}
				dts = append(dts, submittedAt)
			}
		}
	}
	nDts := len(dts)
	if nDts == 0 {
		// If there was no review of anybody else that author's, then fallback to author's review
		dts = udts
		nDts = len(dts)
	}
	switch nDts {
	case 0:
		dt = time.Now()
	case 1:
		dt = dts[0]
	default:
		sort.Slice(dts, func(i, j int) bool {
			return dts[i].Before(dts[j])
		})
		dt = dts[0]
	}
	return
}

// EnrichPullRequestComments - return rich comments from raw pull request
func (j *DSGitHub) EnrichPullRequestComments(ctx *shared.Ctx, pull map[string]interface{}, comments []map[string]interface{}) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_comment=true
	// copy pull request: github_repo, repo_name, repository
	// copy comment: created_at, updated_at, body, body_analyzed, author_association, url, html_url
	// identify: id, id_in_repo, pull_request_comment_id, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// calc: n_reactions
	// identity: author_... -> commenter_...,
	// common: is_github_pull_request=1, is_github_pull_request_comment=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name"}
	copyCommentFields := []string{"created_at", "updated_at", "body", "body_analyzed", "author_association", "url", "html_url"}
	for _, comment := range comments {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		for _, field := range copyCommentFields {
			rich[field], _ = comment[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "pull_request_comment"
		rich["item_type"] = "pull request comment"
		rich["pull_request_comment"] = true
		rich["pull_request_created_at"], _ = pull["created_at"]
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		iCID, _ := comment["id"]
		cid := int64(iCID.(float64))
		rich["id_in_repo"] = cid
		rich["pull_request_comment_id"] = cid
		rich["id"] = id + "/comment/" + fmt.Sprintf("%d", cid)
		rich["url_id"] = fmt.Sprintf("%s/pulls/%d/comments/%d", githubRepo, iNumber, cid)
		reactions := 0
		iReactions, ok := shared.Dig(comment, []string{"reactions", "total_count"}, false, true)
		if ok {
			reactions = int(iReactions.(float64))
		}
		rich["n_reactions"] = reactions
		rich["commenter_association"], _ = comment["author_association"]
		rich["commenter_login"], _ = shared.Dig(comment, []string{"user", "login"}, false, true)
		iCommenterData, ok := comment["user_data"]
		if ok && iCommenterData != nil {
			user, _ := iCommenterData.(map[string]interface{})
			rich["author_login"], _ = user["login"]
			rich["author_name"], _ = user["name"]
			rich["author_avatar_url"], _ = user["avatar_url"]
			rich["commenter_avatar_url"] = rich["author_avatar_url"]
			rich["commenter_name"], _ = user["name"]
			rich["commenter_domain"] = nil
			iEmail, ok := user["email"]
			if ok {
				email, _ := iEmail.(string)
				ary := strings.Split(email, "@")
				if len(ary) > 1 {
					rich["commenter_domain"] = strings.TrimSpace(ary[1])
				}
			}
			rich["commenter_org"], _ = user["company"]
			rich["commenter_location"], _ = user["location"]
			rich["commenter_geolocation"] = nil
		} else {
			rich["author_login"] = nil
			rich["author_name"] = nil
			rich["author_avatar_url"] = nil
			rich["commenter_avatar_url"] = nil
			rich["commenter_name"] = nil
			rich["commenter_domain"] = nil
			rich["commenter_org"] = nil
			rich["commenter_location"] = nil
			rich["commenter_geolocation"] = nil
		}
		iCreatedAt, _ := comment["created_at"]
		createdAt, _ := shared.TimeParseInterfaceString(iCreatedAt)
		rich["metadata__updated_on"] = createdAt
		rich["roles"] = j.GetRoles(ctx, comment, GitHubPullRequestCommentRoles, createdAt)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
	}
	return
}

// EnrichPullRequestReviews - return rich reviews from raw pull request
func (j *DSGitHub) EnrichPullRequestReviews(ctx *shared.Ctx, pull map[string]interface{}, reviews []map[string]interface{}) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_review=true
	// copy pull request: github_repo, repo_name, repository
	// copy review: body, body_analyzed, submitted_at, commit_id, html_url, pull_request_url, state, author_association
	// identify: id, id_in_repo, pull_request_comment_id, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// calc: n_reactions
	// identity: author_... -> reviewer_...,
	// common: is_github_pull_request=1, is_github_pull_request_review=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	pullCreatedAt, _ := pull["created_at"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "url", "repo_short_name", "merged"}
	copyReviewFields := []string{"body", "body_analyzed", "submitted_at", "commit_id", "html_url", "pull_request_url", "state", "author_association", "is_first_review", "is_first_approval"}
	bApproved := false
	firstReview := time.Now()
	firstApproval := time.Now()
	firstReviewIdx := -1
	firstApprovalIdx := -1
	for i, review := range reviews {
		review["is_first_review"] = false
		review["is_first_approval"] = false
		iSubmittedAt, _ := review["submitted_at"]
		submittedAt, _ := shared.TimeParseInterfaceString(iSubmittedAt)
		if submittedAt.Before(firstReview) {
			firstReview = submittedAt
			firstReviewIdx = i
		}
		approved, ok := review["state"]
		if !ok {
			continue
		}
		if approved.(string) == "APPROVED" {
			bApproved = true
			if submittedAt.Before(firstApproval) {
				firstApproval = submittedAt
				firstApprovalIdx = i
			}
		}
	}
	if firstReviewIdx >= 0 {
		reviews[firstReviewIdx]["is_first_review"] = true
	}
	if firstApprovalIdx >= 0 {
		reviews[firstApprovalIdx]["is_first_approval"] = true
	}
	for _, review := range reviews {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		for _, field := range copyReviewFields {
			rich[field], _ = review[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "pull_request_review"
		rich["item_type"] = "pull request review"
		rich["pull_request_review"] = true
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		rich["is_approved"] = bApproved
		iRID, _ := review["id"]
		rid := int64(iRID.(float64))
		rich["id_in_repo"] = rid
		rich["pull_request_review_id"] = rid
		rich["pull_request_created_at"] = pullCreatedAt
		rich["id"] = id + "/review/" + fmt.Sprintf("%d", rid)
		rich["url_id"] = fmt.Sprintf("%s/pulls/%d/reviews/%d", githubRepo, iNumber, rid)
		rich["reviewer_association"], _ = review["author_association"]
		rich["reviewer_login"], _ = shared.Dig(review, []string{"user", "login"}, false, true)
		iReviewerData, ok := review["user_data"]
		if ok && iReviewerData != nil {
			user, _ := iReviewerData.(map[string]interface{})
			rich["author_login"], _ = user["login"]
			rich["author_name"], _ = user["name"]
			rich["author_avatar_url"], _ = user["avatar_url"]
			rich["reviewer_avatar_url"] = rich["author_avatar_url"]
			rich["reviewer_name"], _ = user["name"]
			rich["reviewer_domain"] = nil
			iEmail, ok := user["email"]
			if ok {
				email, _ := iEmail.(string)
				ary := strings.Split(email, "@")
				if len(ary) > 1 {
					rich["reviewer_domain"] = strings.TrimSpace(ary[1])
				}
			}
			rich["reviewer_org"], _ = user["company"]
			rich["reviewer_location"], _ = user["location"]
			rich["reviewer_geolocation"] = nil
		} else {
			rich["author_login"] = nil
			rich["author_name"] = nil
			rich["author_avatar_url"] = nil
			rich["reviewer_avatar_url"] = nil
			rich["reviewer_name"] = nil
			rich["reviewer_domain"] = nil
			rich["reviewer_org"] = nil
			rich["reviewer_location"] = nil
			rich["reviewer_geolocation"] = nil
		}
		iSubmittedAt, _ := review["submitted_at"]
		submittedAt, _ := shared.TimeParseInterfaceString(iSubmittedAt)
		rich["metadata__updated_on"] = submittedAt
		rich["roles"] = j.GetRoles(ctx, review, GitHubPullRequestReviewRoles, submittedAt)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
	}
	pull["is_approved"] = bApproved
	return
}

// EnrichPullRequestAssignees - return rich assignees from raw pull request
func (j *DSGitHub) EnrichPullRequestAssignees(ctx *shared.Ctx, pull map[string]interface{}, assignees []map[string]interface{}) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_assignee=true
	// copy pull request: github_repo, repo_name, repository
	// identify: id, id_in_repo, pull_request_assignee_login, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// identity: author_... -> assignee_...,
	// common: is_github_pull_request=1, is_github_pull_request_assignee=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name"}
	for _, assignee := range assignees {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "pull_request_assignee"
		rich["item_type"] = "pull request assignee"
		rich["pull_request_assignee"] = true
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		iLogin, _ := assignee["login"]
		login, _ := iLogin.(string)
		rich["id_in_repo"], _ = assignee["id"]
		rich["pull_request_assignee_login"] = login
		rich["id"] = id + "/assignee/" + login
		rich["url_id"] = fmt.Sprintf("%s/pulls/%d/assignees/%s", githubRepo, iNumber, login)
		rich["author_login"] = login
		rich["author_name"], _ = assignee["name"]
		rich["author_avatar_url"], _ = assignee["avatar_url"]
		rich["assignee_avatar_url"] = rich["author_avatar_url"]
		rich["assignee_login"] = login
		rich["assignee_name"], _ = assignee["name"]
		rich["assignee_domain"] = nil
		iEmail, ok := assignee["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["assignee_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["assignee_org"], _ = assignee["company"]
		rich["assignee_location"], _ = assignee["location"]
		rich["assignee_geolocation"] = nil
		// We consider assignee enrollment at pull request creation date
		iCreatedAt, _ := pull["created_at"]
		createdAt, _ := iCreatedAt.(time.Time)
		rich["metadata__updated_on"] = createdAt
		rich["roles"] = j.GetRoles(ctx, map[string]interface{}{"assignee": assignee}, GitHubPullRequestAssigneeRoles, createdAt)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
	}
	return
}

// EnrichPullRequestReactions - return rich reactions from raw pull request comment
func (j *DSGitHub) EnrichPullRequestReactions(ctx *shared.Ctx, pull map[string]interface{}, reactions []map[string]interface{}) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_comment_reaction=true
	// copy pull request: github_repo, repo_name, repository
	// copy reaction: content
	// identify: id, id_in_repo, pull_request_comment_reaction_id, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// identity: author_... -> actor_...,
	// common: is_github_pull_request=1, is_github_pull_request_comment_reaction=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name"}
	copyReactionFields := []string{"content"}
	reactionSuffix := "_comment_reaction"
	for _, reaction := range reactions {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		for _, field := range copyReactionFields {
			rich[field], _ = reaction[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		iRID, _ := reaction["id"]
		rid := int64(iRID.(float64))
		iComment, _ := reaction["parent"]
		comment, _ := iComment.(map[string]interface{})
		iCID, _ := comment["id"]
		cid := int64(iCID.(float64))
		rich["type"] = "pull_request" + reactionSuffix
		rich["item_type"] = "pull request comment reaction"
		rich["pull_request"+reactionSuffix] = true
		rich["pull_request_comment_id"] = cid
		rich["pull_request_comment_reaction_id"] = rid
		rich["id_in_repo"] = rid
		rich["id"] = id + "/comment/" + fmt.Sprintf("%d", cid) + "/reaction/" + fmt.Sprintf("%d", rid)
		rich["url_id"] = fmt.Sprintf("%s/pulls/%d/comments/%d/reactions/%d", githubRepo, iNumber, cid, rid)
		rich["pull_request_comment_html_url"], _ = comment["html_url"]
		rich["pull_request_comment_author_association"], _ = comment["author_association"]
		iCreatedAt, _ := comment["created_at"]
		iUserData, ok := reaction["user_data"]
		if ok && iUserData != nil {
			user, _ := iUserData.(map[string]interface{})
			rich["author_login"], _ = user["login"]
			rich["actor_login"], _ = user["login"]
			rich["author_name"], _ = user["name"]
			rich["author_avatar_url"], _ = user["avatar_url"]
			rich["actor_avatar_url"] = rich["author_avatar_url"]
			rich["actor_name"], _ = user["name"]
			rich["actor_domain"] = nil
			iEmail, ok := user["email"]
			if ok {
				email, _ := iEmail.(string)
				ary := strings.Split(email, "@")
				if len(ary) > 1 {
					rich["actor_domain"] = strings.TrimSpace(ary[1])
				}
			}
			rich["actor_org"], _ = user["company"]
			rich["actor_location"], _ = user["location"]
			rich["actor_geolocation"] = nil
		} else {
			rich["author_login"] = nil
			rich["author_name"] = nil
			rich["author_avatar_url"] = nil
			rich["actor_avatar_url"] = nil
			rich["actor_login"] = nil
			rich["actor_name"] = nil
			rich["actor_domain"] = nil
			rich["actor_org"] = nil
			rich["actor_location"] = nil
			rich["actor_geolocation"] = nil
		}
		// createdAt is pull request comment creation date
		// reaction itself doesn't have any date in GH API
		createdAt, _ := shared.TimeParseInterfaceString(iCreatedAt)
		rich["metadata__updated_on"] = createdAt
		rich["roles"] = j.GetRoles(ctx, reaction, GitHubPullRequestReactionRoles, createdAt)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
	}
	return
}

// EnrichPullRequestRequestedReviewers - return rich requested reviewers from raw pull request
func (j *DSGitHub) EnrichPullRequestRequestedReviewers(ctx *shared.Ctx, pull map[string]interface{}, requestedReviewers []map[string]interface{}) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_requested_reviewer=true
	// copy pull request: github_repo, repo_name, repository
	// identify: id, id_in_repo, pull_request_requested_reviewer_login, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// identity: author_... -> requested_reviewer_...,
	// common: is_github_pull_request=1, is_github_pull_request_requested_reviewer=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name"}
	for _, reviewer := range requestedReviewers {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "pull_request_requested_reviewer"
		rich["item_type"] = "pull request requested reviewer"
		rich["pull_request_requested_reviewer"] = true
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		iLogin, _ := reviewer["login"]
		login, _ := iLogin.(string)
		rich["id_in_repo"], _ = reviewer["id"]
		rich["pull_request_requested_reviewer_login"] = login
		rich["id"] = id + "/requested_reviewer/" + login
		rich["url_id"] = fmt.Sprintf("%s/pulls/%d/requested_reviewers/%s", githubRepo, iNumber, login)
		rich["author_login"] = login
		rich["author_name"], _ = reviewer["name"]
		rich["author_avatar_url"], _ = reviewer["avatar_url"]
		rich["requested_reviewer_avatar_url"] = rich["author_avatar_url"]
		rich["requested_reviewer_login"] = login
		rich["requested_reviewer_name"], _ = reviewer["name"]
		rich["requested_reviewer_domain"] = nil
		iEmail, ok := reviewer["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["requested_reviewer_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["requested_reviewer_org"], _ = reviewer["company"]
		rich["requested_reviewer_location"], _ = reviewer["location"]
		rich["requested_reviewer_geolocation"] = nil
		// We consider requested reviewer enrollment at pull request creation date
		iCreatedAt, _ := pull["created_at"]
		createdAt, _ := iCreatedAt.(time.Time)
		rich["metadata__updated_on"] = createdAt
		rich["roles"] = j.GetRoles(ctx, map[string]interface{}{"requested_reviewer": reviewer}, GitHubPullRequestRequestedReviewerRoles, createdAt)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
	}
	return
}

// EnrichPullRequestCommits - return rich commits from raw pull request
func (j *DSGitHub) EnrichPullRequestCommits(ctx *shared.Ctx, pull map[string]interface{}, commits []map[string]interface{}) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_commit=true
	// copy pull request: github_repo, repo_name, repository
	// identify: id, id_in_repo, pull_request_commit_author, pull_request_commit_committer, sha
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// common: is_github_pull_request=1, is_github_pull_request_commit=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name"}
	for _, commit := range commits {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "pull_request_commit"
		rich["item_type"] = "pull request commit"
		rich["pull_request_commit"] = true
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		sha, _ := commit["sha"].(string)
		rich["id_in_repo"] = sha
		rich["sha"] = sha
		rich["id"] = id + "/commit/" + sha
		rich["url_id"] = fmt.Sprintf("%s/pulls/%d/commits/%s", githubRepo, iNumber, sha)
		rich["url"], _ = commit["url"]
		// We consider commit enrollment at pull request creation date
		iCreatedAt, _ := pull["created_at"]
		createdAt, _ := iCreatedAt.(time.Time)
		rich["metadata__updated_on"] = createdAt
		rich["roles"] = j.GetRoles(ctx, commit, GitHubPullRequestCommitRoles, createdAt)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
	}
	return
}

// EnrichPullRequestItem - return rich item from raw item for a given author type
func (j *DSGitHub) EnrichPullRequestItem(ctx *shared.Ctx, item map[string]interface{}) (rich map[string]interface{}, err error) {
	rich = make(map[string]interface{})
	pull, ok := item["data"].(map[string]interface{})
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
	rich["repo_name"] = j.URL
	rich["repository"] = j.URL
	rich["id"] = j.ItemID(pull)
	rich["pull_request_id"], _ = pull["id"]
	iCreatedAt, _ := pull["created_at"]
	createdAt, _ := shared.TimeParseInterfaceString(iCreatedAt)
	updatedOn, _ := shared.Dig(item, []string{"metadata__updated_on"}, true, false)
	rich["type"] = j.CurrentCategory
	rich["category"] = j.CurrentCategory
	now := time.Now()
	rich["created_at"] = createdAt
	rich["updated_at"] = updatedOn
	iClosedAt, ok := pull["closed_at"]
	rich["closed_at"] = iClosedAt
	if ok && iClosedAt != nil {
		closedAt, e := shared.TimeParseInterfaceString(iClosedAt)
		if e == nil {
			rich["time_to_close_days"] = float64(closedAt.Sub(createdAt).Seconds()) / 86400.0
		} else {
			rich["time_to_close_days"] = nil
		}
	} else {
		rich["time_to_close_days"] = nil
	}
	state, ok := pull["state"]
	rich["state"] = state
	if ok && state != nil && state.(string) == "closed" {
		rich["time_open_days"] = rich["time_to_close_days"]
	} else {
		rich["time_open_days"] = float64(now.Sub(createdAt).Seconds()) / 86400.0
	}
	iNumber, _ := pull["number"]
	number := int(iNumber.(float64))
	rich["id_in_repo"] = number
	rich["title"], _ = pull["title"]
	rich["title_analyzed"], _ = pull["title"]
	rich["body"], _ = pull["body"]
	rich["body_analyzed"], _ = pull["body"]
	rich["url"], _ = pull["html_url"]
	iMergedAt, _ := pull["merged_at"]
	rich["merged_at"] = iMergedAt
	rich["merged"], _ = pull["merged"]
	rich["user_login"], _ = shared.Dig(pull, []string{"user", "login"}, false, true)
	iUserData, ok := pull["user_data"]
	if ok && iUserData != nil {
		user, _ := iUserData.(map[string]interface{})
		rich["author_login"], _ = user["login"]
		rich["author_name"], _ = user["name"]
		rich["author_avatar_url"], _ = user["avatar_url"]
		rich["user_avatar_url"] = rich["author_avatar_url"]
		rich["user_name"], _ = user["name"]
		rich["user_domain"] = nil
		iEmail, ok := user["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["user_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["user_org"], _ = user["company"]
		rich["user_location"], _ = user["location"]
		rich["user_geolocation"] = nil
	} else {
		rich["author_login"] = nil
		rich["author_name"] = nil
		rich["author_avatar_url"] = nil
		rich["user_avatar_url"] = nil
		rich["user_name"] = nil
		rich["user_domain"] = nil
		rich["user_org"] = nil
		rich["user_location"] = nil
		rich["user_geolocation"] = nil
	}
	iAssigneeData, ok := pull["assignee_data"]
	if ok && iAssigneeData != nil {
		assignee, _ := iAssigneeData.(map[string]interface{})
		rich["assignee_login"], _ = assignee["login"]
		rich["assignee_name"], _ = assignee["name"]
		rich["assignee_avatar_url"], _ = assignee["avatar_url"]
		rich["assignee_domain"] = nil
		iEmail, ok := assignee["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["assignee_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["assignee_org"], _ = assignee["company"]
		rich["assignee_location"], _ = assignee["location"]
		rich["assignee_geolocation"] = nil
	} else {
		rich["assignee_login"] = nil
		rich["assignee_name"] = nil
		rich["assignee_avatar_url"] = nil
		rich["assignee_domain"] = nil
		rich["assignee_org"] = nil
		rich["assignee_location"] = nil
		rich["assignee_geolocation"] = nil
	}
	iMergedByData, ok := pull["merged_by_data"]
	if ok && iMergedByData != nil {
		mergedBy, _ := iMergedByData.(map[string]interface{})
		rich["merge_author_login"], _ = mergedBy["login"]
		rich["merge_author_name"], _ = mergedBy["name"]
		rich["merge_author_avatar_url"], _ = mergedBy["avatar_url"]
		rich["merge_author_domain"] = nil
		iEmail, ok := mergedBy["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["merge_author_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["merge_author_org"], _ = mergedBy["company"]
		rich["merge_author_location"], _ = mergedBy["location"]
		rich["merge_author_geolocation"] = nil
	} else {
		rich["merge_author_login"] = nil
		rich["merge_author_name"] = nil
		rich["merge_author_avatar_url"] = nil
		rich["merge_author_domain"] = nil
		rich["merge_author_org"] = nil
		rich["merge_author_location"] = nil
		rich["merge_author_geolocation"] = nil
	}
	iLabels, ok := pull["labels"]
	if ok && iLabels != nil {
		ary, _ := iLabels.([]interface{})
		labels := []string{}
		for _, iLabel := range ary {
			label, _ := iLabel.(map[string]interface{})
			iLabelName, _ := label["name"]
			labelName, _ := iLabelName.(string)
			if labelName != "" {
				labels = append(labels, labelName)
			}
		}
		rich["labels"] = labels
	}
	nAssignees := 0
	iAssignees, ok := pull["assignees_data"]
	if ok && iAssignees != nil {
		ary, _ := iAssignees.([]interface{})
		nAssignees = len(ary)
		assignees := []interface{}{}
		for _, iAssignee := range ary {
			assignee, _ := iAssignee.(map[string]interface{})
			iAssigneeLogin, _ := assignee["login"]
			assigneeLogin, _ := iAssigneeLogin.(string)
			if assigneeLogin != "" {
				assignees = append(assignees, assigneeLogin)
			}
		}
		rich["assignees_data"] = assignees
	}
	rich["n_assignees"] = nAssignees
	nRequestedReviewers := 0
	iRequestedReviewers, ok := pull["requested_reviewers_data"]
	if ok && iRequestedReviewers != nil {
		ary, _ := iRequestedReviewers.([]interface{})
		nRequestedReviewers = len(ary)
		requestedReviewers := []interface{}{}
		for _, iRequestedReviewer := range ary {
			requestedReviewer, _ := iRequestedReviewer.(map[string]interface{})
			iRequestedReviewerLogin, _ := requestedReviewer["login"]
			requestedReviewerLogin, _ := iRequestedReviewerLogin.(string)
			if requestedReviewerLogin != "" {
				requestedReviewers = append(requestedReviewers, requestedReviewerLogin)
			}
		}
		rich["requested_reviewers_data"] = requestedReviewers
	}
	rich["n_requested_reviewers"] = nRequestedReviewers
	nCommits := 0
	iCommits, ok := pull["commits_data"]
	if ok && iCommits != nil {
		rich["commits_data"] = iCommits
		commits, _ := iCommits.([]map[string]interface{})
		nCommits = len(commits)
		shas := []string{}
		for _, commit := range commits {
			iSHA, _ := commit["sha"]
			sha, _ := iSHA.(string)
			if sha != "" {
				shas = append(shas, sha)
			}
			// we also have author & committer here, but we don't need it
		}
		rich["shas"] = shas
	}
	rich["n_commits"] = nCommits
	nCommenters := 0
	nComments := 0
	reactions := 0
	iComments, ok := pull["review_comments_data"]
	commenters := map[string]interface{}{}
	if ok && iComments != nil {
		ary, _ := iComments.([]interface{})
		nComments = len(ary)
		for _, iComment := range ary {
			comment, _ := iComment.(map[string]interface{})
			iCommenter, _ := shared.Dig(comment, []string{"user", "login"}, false, true)
			commenter, _ := iCommenter.(string)
			if commenter != "" {
				commenters[commenter] = struct{}{}
			}
			iReactions, ok := shared.Dig(comment, []string{"reactions", "total_count"}, false, true)
			if ok {
				reacts := int(iReactions.(float64))
				reactions += reacts
			}
		}
	}
	iComments, ok = pull["comments_data"]
	if ok && iComments != nil {
		ary, _ := iComments.([]interface{})
		nComments += len(ary)
		for _, iComment := range ary {
			comment, _ := iComment.(map[string]interface{})
			iCommenter, _ := shared.Dig(comment, []string{"user", "login"}, false, true)
			commenter, _ := iCommenter.(string)
			if commenter != "" {
				commenters[commenter] = struct{}{}
			}
			iReactions, ok := shared.Dig(comment, []string{"reactions", "total_count"}, false, true)
			if ok {
				reacts := int(iReactions.(float64))
				reactions += reacts
			}
		}
	}
	if len(commenters) > 0 {
		nCommenters = len(commenters)
		comms := []string{}
		for commenter := range commenters {
			comms = append(comms, commenter)
		}
		rich["commenters"] = comms
	}
	rich["n_commenters"] = nCommenters
	rich["n_comments"] = nComments
	nReviewCommenters := 0
	nReviewComments := 0
	iReviewComments, ok := pull["reviews_data"]
	if ok && iReviewComments != nil {
		ary, _ := iReviewComments.([]interface{})
		nReviewComments = len(ary)
		reviewCommenters := map[string]interface{}{}
		for _, iReviewComment := range ary {
			reviewComment, _ := iReviewComment.(map[string]interface{})
			iReviewCommenter, _ := shared.Dig(reviewComment, []string{"user", "login"}, false, true)
			reviewCommenter, _ := iReviewCommenter.(string)
			if reviewCommenter != "" {
				reviewCommenters[reviewCommenter] = struct{}{}
			}
		}
		nReviewCommenters = len(reviewCommenters)
		revComms := []string{}
		for reviewCommenter := range reviewCommenters {
			revComms = append(revComms, reviewCommenter)
		}
		rich["review_commenters"] = revComms
	}
	rich["n_review_commenters"] = nReviewCommenters
	rich["n_review_comments"] = nReviewComments
	rich["pull_request"] = true
	rich["item_type"] = "pull request"
	githubRepo := j.URL
	if strings.HasSuffix(githubRepo, ".git") {
		githubRepo = githubRepo[:len(githubRepo)-4]
	}
	if strings.Contains(githubRepo, GitHubURLRoot) {
		githubRepo = strings.Replace(githubRepo, GitHubURLRoot, "", -1)
	}
	var repoShortName string
	arr := strings.Split(githubRepo, "/")
	if len(arr) > 1 {
		repoShortName = arr[1]
	}
	rich["repo_short_name"] = repoShortName
	rich["github_repo"] = githubRepo
	rich["url_id"] = fmt.Sprintf("%s/pull/%d", githubRepo, number)
	rich["forks"], _ = shared.Dig(pull, []string{"base", "repo", "forks_count"}, false, true)
	rich["num_review_comments"], _ = pull["review_comments"]
	if iMergedAt != nil {
		mergedAt, e := shared.TimeParseInterfaceString(iMergedAt)
		if e == nil {
			rich["code_merge_duration"] = float64(mergedAt.Sub(createdAt).Seconds()) / 86400.0
		} else {
			rich["code_merge_duration"] = nil
		}
	} else {
		rich["code_merge_duration"] = nil
	}
	commentsVal := 0
	iCommentsVal, ok := pull["comments"]
	if ok {
		commentsVal = int(iCommentsVal.(float64))
	}
	rich["n_total_comments"] = commentsVal
	// There is probably no value for "reactions", "total_count" on the top level of "pull" object, but we can attempt to get this
	iReactions, ok := shared.Dig(pull, []string{"reactions", "total_count"}, false, true)
	if ok {
		reacts := int(iReactions.(float64))
		reactions += reacts
	}
	rich["n_reactions"] = reactions
	rich["time_to_merge_request_response"] = nil
	if nComments > 0 {
		firstReviewDate := j.GetFirstPullRequestReviewDate(pull, false)
		rich["time_to_merge_request_response"] = float64(firstReviewDate.Sub(createdAt).Seconds()) / 86400.0
	}
	if nReviewComments > 0 || nComments > 0 {
		firstAttentionDate := j.GetFirstPullRequestReviewDate(pull, true)
		rich["time_to_first_attention"] = float64(firstAttentionDate.Sub(createdAt).Seconds()) / 86400.0
	}
	rich["metadata__updated_on"] = createdAt
	rich["roles"] = j.GetRoles(ctx, pull, GitHubPullRequestRoles, createdAt)
	// NOTE: From shared
	rich["metadata__enriched_on"] = time.Now()
	// rich[ProjectSlug] = ctx.ProjectSlug
	// rich["groups"] = ctx.Groups
	return
}

// EnrichIssueItem - return rich item from raw item for a given author type
func (j *DSGitHub) EnrichIssueItem(ctx *shared.Ctx, item map[string]interface{}) (rich map[string]interface{}, err error) {
	rich = make(map[string]interface{})
	issue, ok := item["data"].(map[string]interface{})
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
	rich["repo_name"] = j.URL
	rich["repository"] = j.URL
	// I think we don't need original UUID in id
	/*
		uuid, ok := rich[UUID].(string)
		if !ok {
			err = fmt.Errorf("cannot read string uuid from %+v", DumpPreview(rich, 100))
			return
		}
		iid := uuid + "/" + j.ItemID(issue)
		rich["id"] = iid
	*/
	rich["id"] = j.ItemID(issue)
	rich["issue_id"], _ = issue["id"]
	iCreatedAt, _ := issue["created_at"]
	createdAt, _ := shared.TimeParseInterfaceString(iCreatedAt)
	updatedOn, _ := shared.Dig(item, []string{"metadata__updated_on"}, true, false)
	rich["type"] = j.CurrentCategory
	rich["category"] = j.CurrentCategory
	now := time.Now()
	rich["created_at"] = createdAt
	rich["updated_at"] = updatedOn
	iClosedAt, ok := issue["closed_at"]
	rich["closed_at"] = iClosedAt
	if ok && iClosedAt != nil {
		closedAt, e := shared.TimeParseInterfaceString(iClosedAt)
		if e == nil {
			rich["time_to_close_days"] = float64(closedAt.Sub(createdAt).Seconds()) / 86400.0
		} else {
			rich["time_to_close_days"] = nil
		}
	} else {
		rich["time_to_close_days"] = nil
	}
	state, ok := issue["state"]
	rich["state"] = state
	if ok && state != nil && state.(string) == "closed" {
		rich["time_open_days"] = rich["time_to_close_days"]
	} else {
		rich["time_open_days"] = float64(now.Sub(createdAt).Seconds()) / 86400.0
	}
	iNumber, _ := issue["number"]
	number := int(iNumber.(float64))
	rich["id_in_repo"] = number
	rich["title"], _ = issue["title"]
	rich["title_analyzed"], _ = issue["title"]
	rich["body"], _ = issue["body"]
	rich["body_analyzed"], _ = issue["body"]
	rich["url"], _ = issue["html_url"]
	rich["user_login"], _ = shared.Dig(issue, []string{"user", "login"}, false, true)
	iUserData, ok := issue["user_data"]
	if ok && iUserData != nil {
		user, _ := iUserData.(map[string]interface{})
		rich["author_login"], _ = user["login"]
		rich["author_name"], _ = user["name"]
		rich["author_avatar_url"], _ = user["avatar_url"]
		rich["user_avatar_url"] = rich["author_avatar_url"]
		rich["user_name"], _ = user["name"]
		rich["user_domain"] = nil
		iEmail, ok := user["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["user_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["user_org"], _ = user["company"]
		rich["user_location"], _ = user["location"]
		rich["user_geolocation"] = nil
	} else {
		rich["author_login"] = nil
		rich["author_name"] = nil
		rich["author_avatar_url"] = nil
		rich["user_avatar_url"] = nil
		rich["user_name"] = nil
		rich["user_domain"] = nil
		rich["user_org"] = nil
		rich["user_location"] = nil
		rich["user_geolocation"] = nil
	}
	iAssigneeData, ok := issue["assignee_data"]
	if ok && iAssigneeData != nil {
		assignee, _ := iAssigneeData.(map[string]interface{})
		rich["assignee_login"], _ = assignee["login"]
		rich["assignee_name"], _ = assignee["name"]
		rich["assignee_avatar_url"], _ = assignee["avatar_url"]
		rich["assignee_domain"] = nil
		iEmail, ok := assignee["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["assignee_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["assignee_org"], _ = assignee["company"]
		rich["assignee_location"], _ = assignee["location"]
		rich["assignee_geolocation"] = nil
	} else {
		rich["assignee_login"] = nil
		rich["assignee_name"] = nil
		rich["assignee_avatar_url"] = nil
		rich["assignee_domain"] = nil
		rich["assignee_org"] = nil
		rich["assignee_location"] = nil
		rich["assignee_geolocation"] = nil
	}
	iLabels, ok := issue["labels"]
	if ok && iLabels != nil {
		ary, _ := iLabels.([]interface{})
		labels := []string{}
		for _, iLabel := range ary {
			label, _ := iLabel.(map[string]interface{})
			iLabelName, _ := label["name"]
			labelName, _ := iLabelName.(string)
			if labelName != "" {
				labels = append(labels, labelName)
			}
		}
		rich["labels"] = labels
	}
	nAssignees := 0
	iAssignees, ok := issue["assignees_data"]
	if ok && iAssignees != nil {
		ary, _ := iAssignees.([]interface{})
		nAssignees = len(ary)
		assignees := []interface{}{}
		for _, iAssignee := range ary {
			assignee, _ := iAssignee.(map[string]interface{})
			iAssigneeLogin, _ := assignee["login"]
			assigneeLogin, _ := iAssigneeLogin.(string)
			if assigneeLogin != "" {
				assignees = append(assignees, assigneeLogin)
			}
		}
		rich["assignees_data"] = assignees
	}
	rich["n_assignees"] = nAssignees
	nCommenters := 0
	nComments := 0
	reactions := 0
	iComments, ok := issue["comments_data"]
	if ok && iComments != nil {
		ary, _ := iComments.([]interface{})
		nComments = len(ary)
		commenters := map[string]interface{}{}
		for _, iComment := range ary {
			comment, _ := iComment.(map[string]interface{})
			iCommenter, _ := shared.Dig(comment, []string{"user", "login"}, false, true)
			commenter, _ := iCommenter.(string)
			if commenter != "" {
				commenters[commenter] = struct{}{}
			}
			iReactions, ok := shared.Dig(comment, []string{"reactions", "total_count"}, false, true)
			if ok {
				reacts := int(iReactions.(float64))
				reactions += reacts
			}
		}
		nCommenters = len(commenters)
		comms := []string{}
		for commenter := range commenters {
			comms = append(comms, commenter)
		}
		rich["commenters"] = comms
	}
	rich["n_commenters"] = nCommenters
	rich["n_comments"] = nComments
	_, hasHead := issue["head"]
	_, hasPR := issue["pull_request"]
	if !hasHead && !hasPR {
		rich["pull_request"] = false
		rich["item_type"] = "issue"
	} else {
		rich["pull_request"] = true
		// "pull request" and "issue pull request" are different object
		// one is an issue object that is also a pull request, while the another is a pull request object
		// rich["item_type"] = "pull request"
		rich["item_type"] = "issue pull request"
	}
	githubRepo := j.URL
	if strings.HasSuffix(githubRepo, ".git") {
		githubRepo = githubRepo[:len(githubRepo)-4]
	}
	if strings.Contains(githubRepo, GitHubURLRoot) {
		githubRepo = strings.Replace(githubRepo, GitHubURLRoot, "", -1)
	}
	var repoShortName string
	arr := strings.Split(githubRepo, "/")
	if len(arr) > 1 {
		repoShortName = arr[1]
	}
	rich["repo_short_name"] = repoShortName
	rich["github_repo"] = githubRepo
	rich["url_id"] = fmt.Sprintf("%s/issues/%d", githubRepo, number)
	rich["time_to_first_attention"] = nil
	commentsVal := 0
	iCommentsVal, ok := issue["comments"]
	if ok {
		commentsVal = int(iCommentsVal.(float64))
	}
	rich["n_total_comments"] = commentsVal
	iReactions, ok := shared.Dig(issue, []string{"reactions", "total_count"}, false, true)
	if ok {
		reacts := int(iReactions.(float64))
		reactions += reacts
	}
	rich["n_reactions"] = reactions
	// if comments+reactions > 0 {
	if commentsVal > 0 || nComments > 0 {
		firstAttention := j.GetFirstIssueAttention(issue)
		rich["time_to_first_attention"] = float64(firstAttention.Sub(createdAt).Seconds()) / 86400.0
	}
	rich["metadata__updated_on"] = createdAt
	rich["roles"] = j.GetRoles(ctx, issue, GitHubIssueRoles, createdAt)
	// NOTE: From shared
	rich["metadata__enriched_on"] = time.Now()
	// rich[ProjectSlug] = ctx.ProjectSlug
	// rich["groups"] = ctx.Groups
	return
}

// EnrichIssueComments - return rich comments from raw issue
func (j *DSGitHub) EnrichIssueComments(ctx *shared.Ctx, issue map[string]interface{}, comments []map[string]interface{}) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), issue_comment=true
	// copy issue: github_repo, repo_name, repository
	// copy comment: created_at, updated_at, body, body_analyzed, author_association, url, html_url
	// identify: id, id_in_repo, issue_comment_id, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: issue_id, issue_number
	// calc: n_reactions
	// identity: author_... -> commenter_...,
	// common: is_github_issue=1, is_github_issue_comment=1
	iID, _ := issue["id"]
	id, _ := iID.(string)
	iIssueID, _ := issue["issue_id"]
	issueID := int(iIssueID.(float64))
	issueNumber, _ := issue["id_in_repo"]
	iNumber, _ := issueNumber.(int)
	iGithubRepo, _ := issue["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyIssueFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name", "pull_request"}
	copyCommentFields := []string{"created_at", "updated_at", "body", "body_analyzed", "author_association", "url", "html_url"}
	for _, comment := range comments {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := issue[field]
			rich[field] = v
		}
		for _, field := range copyIssueFields {
			rich[field], _ = issue[field]
		}
		for _, field := range copyCommentFields {
			rich[field], _ = comment[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "issue_comment"
		rich["item_type"] = "issue comment"
		rich["issue_comment"] = true
		rich["issue_created_at"], _ = issue["created_at"]
		rich["issue_id"] = issueID
		rich["issue_number"] = issueNumber
		iCID, _ := comment["id"]
		cid := int64(iCID.(float64))
		rich["id_in_repo"] = cid
		rich["issue_comment_id"] = cid
		rich["id"] = id + "/comment/" + fmt.Sprintf("%d", cid)
		rich["url_id"] = fmt.Sprintf("%s/issues/%d/comments/%d", githubRepo, iNumber, cid)
		reactions := 0
		iReactions, ok := shared.Dig(comment, []string{"reactions", "total_count"}, false, true)
		if ok {
			reactions = int(iReactions.(float64))
		}
		rich["n_reactions"] = reactions
		rich["commenter_association"], _ = comment["author_association"]
		rich["commenter_login"], _ = shared.Dig(comment, []string{"user", "login"}, false, true)
		iCommenterData, ok := comment["user_data"]
		if ok && iCommenterData != nil {
			user, _ := iCommenterData.(map[string]interface{})
			rich["author_login"], _ = user["login"]
			rich["author_name"], _ = user["name"]
			rich["author_avatar_url"], _ = user["avatar_url"]
			rich["commenter_avatar_url"] = rich["author_avatar_url"]
			rich["commenter_name"], _ = user["name"]
			rich["commenter_domain"] = nil
			iEmail, ok := user["email"]
			if ok {
				email, _ := iEmail.(string)
				ary := strings.Split(email, "@")
				if len(ary) > 1 {
					rich["commenter_domain"] = strings.TrimSpace(ary[1])
				}
			}
			rich["commenter_org"], _ = user["company"]
			rich["commenter_location"], _ = user["location"]
			rich["commenter_geolocation"] = nil
		} else {
			rich["author_login"] = nil
			rich["author_name"] = nil
			rich["author_avatar_url"] = nil
			rich["commenter_avatar_url"] = nil
			rich["commenter_name"] = nil
			rich["commenter_domain"] = nil
			rich["commenter_org"] = nil
			rich["commenter_location"] = nil
			rich["commenter_geolocation"] = nil
		}
		iCreatedAt, _ := comment["created_at"]
		createdAt, _ := shared.TimeParseInterfaceString(iCreatedAt)
		rich["metadata__updated_on"] = createdAt
		rich["roles"] = j.GetRoles(ctx, comment, GitHubIssueCommentRoles, createdAt)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
	}
	return
}

// EnrichIssuePullRequestComments - return rich comments from raw pull request (issue part)
func (j *DSGitHub) EnrichIssuePullRequestComments(ctx *shared.Ctx, pull map[string]interface{}, comments []map[string]interface{}) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_comment=true
	// copy pull request: github_repo, repo_name, repository
	// copy comment: created_at, updated_at, body, body_analyzed, author_association, url, html_url
	// identify: id, id_in_repo, pull_request_comment_id, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// calc: n_reactions
	// identity: author_... -> commenter_...,
	// common: is_github_pull_request=1, is_github_pull_request_comment=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name"}
	copyCommentFields := []string{"created_at", "updated_at", "body", "body_analyzed", "author_association", "url", "html_url"}
	for _, comment := range comments {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		for _, field := range copyCommentFields {
			rich[field], _ = comment[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "issue_pull_request_comment"
		rich["item_type"] = "issue pull request comment"
		rich["issue_pull_request_comment"] = true
		rich["pull_request_created_at"], _ = pull["created_at"]
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		iCID, _ := comment["id"]
		cid := int64(iCID.(float64))
		rich["id_in_repo"] = cid
		rich["pull_request_comment_id"] = cid
		rich["issue_comment_id"] = cid
		rich["id"] = id + "/comment/" + fmt.Sprintf("%d", cid)
		// rich["url_id"] = fmt.Sprintf("%s/pulls/%d/comments/%d", githubRepo, iNumber, cid)
		rich["url_id"] = fmt.Sprintf("%s/issues/%d/comments/%d", githubRepo, iNumber, cid)
		reactions := 0
		iReactions, ok := shared.Dig(comment, []string{"reactions", "total_count"}, false, true)
		if ok {
			reactions = int(iReactions.(float64))
		}
		rich["n_reactions"] = reactions
		rich["commenter_association"], _ = comment["author_association"]
		rich["commenter_login"], _ = shared.Dig(comment, []string{"user", "login"}, false, true)
		iCommenterData, ok := comment["user_data"]
		if ok && iCommenterData != nil {
			user, _ := iCommenterData.(map[string]interface{})
			rich["author_login"], _ = user["login"]
			rich["author_name"], _ = user["name"]
			rich["author_avatar_url"], _ = user["avatar_url"]
			rich["commenter_avatar_url"] = rich["author_avatar_url"]
			rich["commenter_name"], _ = user["name"]
			rich["commenter_domain"] = nil
			iEmail, ok := user["email"]
			if ok {
				email, _ := iEmail.(string)
				ary := strings.Split(email, "@")
				if len(ary) > 1 {
					rich["commenter_domain"] = strings.TrimSpace(ary[1])
				}
			}
			rich["commenter_org"], _ = user["company"]
			rich["commenter_location"], _ = user["location"]
			rich["commenter_geolocation"] = nil
		} else {
			rich["author_login"] = nil
			rich["author_name"] = nil
			rich["author_avatar_url"] = nil
			rich["commenter_avatar_url"] = nil
			rich["commenter_name"] = nil
			rich["commenter_domain"] = nil
			rich["commenter_org"] = nil
			rich["commenter_location"] = nil
			rich["commenter_geolocation"] = nil
		}
		iCreatedAt, _ := comment["created_at"]
		createdAt, _ := shared.TimeParseInterfaceString(iCreatedAt)
		rich["metadata__updated_on"] = createdAt
		rich["roles"] = j.GetRoles(ctx, comment, GitHubPullRequestCommentRoles, createdAt)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
	}
	return
}

// EnrichIssueAssignees - return rich assignees from raw issue
func (j *DSGitHub) EnrichIssueAssignees(ctx *shared.Ctx, issue map[string]interface{}, assignees []map[string]interface{}) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), issue_assignee=true
	// copy issue: github_repo, repo_name, repository
	// identify: id, id_in_repo, issue_assignee_login, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: issue_id, issue_number
	// identity: author_... -> assignee_...,
	// common: is_github_issue=1, is_github_issue_assignee=1
	iID, _ := issue["id"]
	id, _ := iID.(string)
	iIssueID, _ := issue["issue_id"]
	issueID := int(iIssueID.(float64))
	issueNumber, _ := issue["id_in_repo"]
	iNumber, _ := issueNumber.(int)
	iGithubRepo, _ := issue["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyIssueFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name", "pull_request"}
	for _, assignee := range assignees {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := issue[field]
			rich[field] = v
		}
		for _, field := range copyIssueFields {
			rich[field], _ = issue[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "issue_assignee"
		rich["item_type"] = "issue assignee"
		rich["issue_assignee"] = true
		rich["issue_id"] = issueID
		rich["issue_number"] = issueNumber
		iLogin, _ := assignee["login"]
		login, _ := iLogin.(string)
		rich["id_in_repo"], _ = assignee["id"]
		rich["issue_assignee_login"] = login
		rich["id"] = id + "/assignee/" + login
		rich["url_id"] = fmt.Sprintf("%s/issues/%d/assignees/%s", githubRepo, iNumber, login)
		rich["author_login"] = login
		rich["author_name"], _ = assignee["name"]
		rich["author_avatar_url"], _ = assignee["avatar_url"]
		rich["assignee_avatar_url"] = rich["author_avatar_url"]
		rich["assignee_login"] = login
		rich["assignee_name"], _ = assignee["name"]
		rich["assignee_domain"] = nil
		iEmail, ok := assignee["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["assignee_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["assignee_org"], _ = assignee["company"]
		rich["assignee_location"], _ = assignee["location"]
		rich["assignee_geolocation"] = nil
		// We consider assignee assignment at issue creation date
		iCreatedAt, _ := issue["created_at"]
		createdAt, _ := iCreatedAt.(time.Time)
		rich["metadata__updated_on"] = createdAt
		rich["roles"] = j.GetRoles(ctx, map[string]interface{}{"assignee": assignee}, GitHubIssueAssigneeRoles, createdAt)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
	}
	return
}

// EnrichIssueReactions - return rich reactions from raw issue and/or issue comment
func (j *DSGitHub) EnrichIssueReactions(ctx *shared.Ctx, issue map[string]interface{}, reactions []map[string]interface{}) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), issue_reaction=true | issue_comment_reaction=true
	// copy issue: github_repo, repo_name, repository
	// copy reaction: content
	// identify: id, id_in_repo, issue_reaction_id | issue_comment_reaction_id, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: issue_id, issue_number
	// identity: author_... -> actor_...,
	// common: is_github_issue=1, is_github_issue_reaction=1 | is_github_issue_comment_reaction=1
	iID, _ := issue["id"]
	id, _ := iID.(string)
	iIssueID, _ := issue["issue_id"]
	issueID := int(iIssueID.(float64))
	issueNumber, _ := issue["id_in_repo"]
	iNumber, _ := issueNumber.(int)
	iGithubRepo, _ := issue["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyIssueFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name", "pull_request"}
	copyReactionFields := []string{"content"}
	for _, reaction := range reactions {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := issue[field]
			rich[field] = v
		}
		for _, field := range copyIssueFields {
			rich[field], _ = issue[field]
		}
		for _, field := range copyReactionFields {
			rich[field], _ = reaction[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["issue_id"] = issueID
		rich["issue_number"] = issueNumber
		iRID, _ := reaction["id"]
		rid := int64(iRID.(float64))
		var (
			comment        map[string]interface{}
			createdAt      time.Time
			reactionSuffix string
		)
		iComment, ok := reaction["parent"]
		if ok {
			comment, _ = iComment.(map[string]interface{})
		}
		if comment != nil {
			reactionSuffix = "_comment_reaction"
			iCID, _ := comment["id"]
			cid := int64(iCID.(float64))
			rich["type"] = "issue" + reactionSuffix
			rich["item_type"] = "issue comment reaction"
			rich["issue"+reactionSuffix] = true
			rich["issue_comment_id"] = cid
			rich["issue_comment_reaction_id"] = rid
			rich["id_in_repo"] = rid
			rich["id"] = id + "/comment/" + fmt.Sprintf("%d", cid) + "/reaction/" + fmt.Sprintf("%d", rid)
			rich["url_id"] = fmt.Sprintf("%s/issues/%d/comments/%d/reactions/%d", githubRepo, iNumber, cid, rid)
			iCreatedAt, _ := comment["created_at"]
			// createdAt is comment creation date for comment reactions
			// reaction itself doesn't have any date in GH API
			createdAt, _ = shared.TimeParseInterfaceString(iCreatedAt)
			rich["issue_comment_html_url"], _ = comment["html_url"]
			rich["issue_comment_author_association"], _ = comment["author_association"]
		} else {
			reactionSuffix = "_reaction"
			rich["type"] = "issue" + reactionSuffix
			rich["item_type"] = "issue reaction"
			rich["issue"+reactionSuffix] = true
			rich["issue_reaction_id"] = rid
			rich["id_in_repo"] = rid
			rich["id"] = id + "/reaction/" + fmt.Sprintf("%d", rid)
			rich["url_id"] = fmt.Sprintf("%s/issues/%d/reactions/%d", githubRepo, iNumber, rid)
			iCreatedAt, _ := issue["created_at"]
			// createdAt is issue creation date for issue reactions
			// reaction itself doesn't have any date in GH API
			createdAt, _ = iCreatedAt.(time.Time)
		}
		iUserData, ok := reaction["user_data"]
		if ok && iUserData != nil {
			user, _ := iUserData.(map[string]interface{})
			rich["author_login"], _ = user["login"]
			rich["actor_login"], _ = user["login"]
			rich["author_name"], _ = user["name"]
			rich["author_avatar_url"], _ = user["avatar_url"]
			rich["actor_avatar_url"] = rich["author_avatar_url"]
			rich["actor_name"], _ = user["name"]
			rich["actor_domain"] = nil
			iEmail, ok := user["email"]
			if ok {
				email, _ := iEmail.(string)
				ary := strings.Split(email, "@")
				if len(ary) > 1 {
					rich["actor_domain"] = strings.TrimSpace(ary[1])
				}
			}
			rich["actor_org"], _ = user["company"]
			rich["actor_location"], _ = user["location"]
			rich["actor_geolocation"] = nil
		} else {
			rich["author_login"] = nil
			rich["author_name"] = nil
			rich["author_avatar_url"] = nil
			rich["actor_avatar_url"] = nil
			rich["actor_login"] = nil
			rich["actor_name"] = nil
			rich["actor_domain"] = nil
			rich["actor_org"] = nil
			rich["actor_location"] = nil
			rich["actor_geolocation"] = nil
		}
		rich["metadata__updated_on"] = createdAt
		rich["roles"] = j.GetRoles(ctx, reaction, GitHubIssueReactionRoles, createdAt)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
	}
	return
}

// GitHubIssueEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSGitHub) GitHubIssueEnrichItemsFunc(ctx *shared.Ctx, items []interface{}, docs *[]interface{}, final bool) (err error) {
	if ctx.Debug > 0 {
		shared.Printf("%s/%s: github enrich issue items %d/%d func\n", j.URL, j.CurrentCategory, len(items), len(*docs))
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
	getRichItem := func(doc map[string]interface{}) (rich map[string]interface{}, e error) {
		rich, e = j.EnrichItem(ctx, doc)
		if e != nil {
			return
		}
		data, _ := shared.Dig(doc, []string{"data"}, true, false)
		// issue: assignees_data[]
		// issue: comments_data[].user_data
		// issue: comments_data[].reactions_data[].user_data
		// issue: reactions_data[].user_data
		iComments, ok := shared.Dig(data, []string{"comments_data"}, false, true)
		if ok && iComments != nil {
			comments, ok := iComments.([]map[string]interface{})
			if ok && len(comments) > 0 {
				var riches []interface{}
				riches, e = j.EnrichIssueComments(ctx, rich, comments)
				if e != nil {
					return
				}
				rich["comments_array"] = riches
				if WantEnrichIssueCommentReactions {
					var reacts []map[string]interface{}
					for _, comment := range comments {
						iReactions, ok := shared.Dig(comment, []string{"reactions_data"}, false, true)
						if ok && iReactions != nil {
							reactions, ok := iReactions.([]map[string]interface{})
							if ok {
								for _, reaction := range reactions {
									// Store parent comment (not present in issue)
									reaction["parent"] = comment
									reacts = append(reacts, reaction)
								}
							}
						}
					}
					if len(reacts) > 0 {
						var riches []interface{}
						riches, e = j.EnrichIssueReactions(ctx, rich, reacts)
						if e != nil {
							return
						}
						rich["comments_reactions_array"] = riches
					}
				}
			}
		}
		if WantEnrichIssueAssignees {
			iAssignees, ok := shared.Dig(data, []string{"assignees_data"}, false, true)
			if ok && iAssignees != nil {
				assignees, ok := iAssignees.([]map[string]interface{})
				if ok && len(assignees) > 0 {
					var riches []interface{}
					riches, e = j.EnrichIssueAssignees(ctx, rich, assignees)
					if e != nil {
						return
					}
					rich["assignees_array"] = riches
				}
			}
		}
		if WantEnrichIssueReactions {
			iReactions, ok := shared.Dig(data, []string{"reactions_data"}, false, true)
			if ok && iReactions != nil {
				reactions, ok := iReactions.([]map[string]interface{})
				if ok && len(reactions) > 0 {
					var riches []interface{}
					riches, e = j.EnrichIssueReactions(ctx, rich, reactions)
					if e != nil {
						return
					}
					rich["reactions_array"] = riches
				}
			}
		}
		return
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
		rich, e := getRichItem(doc)
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

// GitHubPullRequestEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSGitHub) GitHubPullRequestEnrichItemsFunc(ctx *shared.Ctx, items []interface{}, docs *[]interface{}, final bool) (err error) {
	if ctx.Debug > 0 {
		shared.Printf("%s/%s: github enrich pull request items %d/%d func\n", j.URL, j.CurrentCategory, len(items), len(*docs))
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
	getRichItem := func(doc map[string]interface{}) (rich map[string]interface{}, e error) {
		rich, e = j.EnrichItem(ctx, doc)
		if e != nil {
			return
		}
		data, _ := shared.Dig(doc, []string{"data"}, true, false)
		// pr:    assignees_data[]
		// pr:    reviews_data[].user_data
		// pr:    review_comments_data[].user_data
		// pr:    review_comments_data[].reactions_data[].user_data
		// pr:    comments_data[].user_data
		// pr:    comments_data[].reactions_data[].user_data
		// pr:    requested_reviewers_data[].user_data
		// pr:    commits_data[].author_data
		// pr:    commits_data[].committer_data
		if WantEnrichPullRequestAssignees {
			iAssignees, ok := shared.Dig(data, []string{"assignees_data"}, false, true)
			if ok && iAssignees != nil {
				assignees, ok := iAssignees.([]map[string]interface{})
				if ok && len(assignees) > 0 {
					var riches []interface{}
					riches, e = j.EnrichPullRequestAssignees(ctx, rich, assignees)
					if e != nil {
						return
					}
					rich["assignees_array"] = riches
				}
			}
		}
		iReviews, ok := shared.Dig(data, []string{"reviews_data"}, false, true)
		if ok && iReviews != nil {
			reviews, ok := iReviews.([]map[string]interface{})
			if ok && len(reviews) > 0 {
				var riches []interface{}
				riches, e = j.EnrichPullRequestReviews(ctx, rich, reviews)
				if e != nil {
					return
				}
				rich["reviews_array"] = riches
			}
		}
		iComments, ok := shared.Dig(data, []string{"review_comments_data"}, false, true)
		if ok && iComments != nil {
			comments, ok := iComments.([]map[string]interface{})
			if ok && len(comments) > 0 {
				var riches []interface{}
				riches, e = j.EnrichPullRequestComments(ctx, rich, comments)
				if e != nil {
					return
				}
				rich["review_comments_array"] = riches
				if WantEnrichPullRequestCommentReactions {
					var reacts []map[string]interface{}
					for _, comment := range comments {
						iReactions, ok := shared.Dig(comment, []string{"reactions_data"}, false, true)
						if ok && iReactions != nil {
							reactions, ok := iReactions.([]map[string]interface{})
							if ok {
								for _, reaction := range reactions {
									reaction["parent"] = comment
									reacts = append(reacts, reaction)
								}
							}
						}
					}
					if len(reacts) > 0 {
						var riches []interface{}
						riches, e = j.EnrichPullRequestReactions(ctx, rich, reacts)
						if e != nil {
							return
						}
						rich["review_comments_reactions_array"] = riches
					}
				}
			}
		}
		iComments, ok = shared.Dig(data, []string{"comments_data"}, false, true)
		if ok && iComments != nil {
			comments, ok := iComments.([]map[string]interface{})
			if ok && len(comments) > 0 {
				var riches []interface{}
				riches, e = j.EnrichIssuePullRequestComments(ctx, rich, comments)
				if e != nil {
					return
				}
				rich["comments_array"] = riches
				if WantEnrichIssueCommentReactions {
					var reacts []map[string]interface{}
					for _, comment := range comments {
						iReactions, ok := shared.Dig(comment, []string{"reactions_data"}, false, true)
						if ok && iReactions != nil {
							reactions, ok := iReactions.([]map[string]interface{})
							if ok {
								for _, reaction := range reactions {
									// Store parent comment (not present in issue)
									reaction["parent"] = comment
									reacts = append(reacts, reaction)
								}
							}
						}
					}
					if len(reacts) > 0 {
						var riches []interface{}
						riches, e = j.EnrichPullRequestReactions(ctx, rich, reacts)
						if e != nil {
							return
						}
						rich["comments_reactions_array"] = riches
					}
				}
			}
		}
		if WantEnrichPullRequestRequestedReviewers {
			iReviewers, ok := shared.Dig(data, []string{"requested_reviewers_data"}, false, true)
			if ok && iReviewers != nil {
				reviewers, ok := iReviewers.([]map[string]interface{})
				if ok && len(reviewers) > 0 {
					var riches []interface{}
					riches, e = j.EnrichPullRequestRequestedReviewers(ctx, rich, reviewers)
					if e != nil {
						return
					}
					rich["requested_reviewers_array"] = riches
				}
			}
		}
		if WantEnrichPullRequestCommits {
			iCommits, ok := shared.Dig(data, []string{"commits_data"}, false, true)
			if ok && iCommits != nil {
				commits, ok := iCommits.([]map[string]interface{})
				if ok && len(commits) > 0 {
					var riches []interface{}
					riches, e = j.EnrichPullRequestCommits(ctx, rich, commits)
					if e != nil {
						return
					}
					rich["commits_array"] = riches
				}
			}
		}
		return
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
		rich, e := getRichItem(doc)
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
	repoFields := []string{"id", "forks_count", "subscribers_count", "stargazers_count", "fetched_on", "description", "created_at", "id"}
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
	case "issue":
		return j.EnrichIssueItem(ctx, item)
	case "pull_request":
		return j.EnrichPullRequestItem(ctx, item)
	}
	return
}

// OutputDocs - send output documents to the consumer
func (j *DSGitHub) OutputDocs(ctx *shared.Ctx, items []interface{}, docs *[]interface{}, final bool) {
	if len(*docs) > 0 {
		// actual output
		shared.Printf("output processing(%d/%d/%v)\n", len(items), len(*docs), final)
		var (
			repos      []repository.RepositoryUpdatedEvent
			issuesData map[string][]interface{}
			pullsData  map[string][]interface{}
			jsonBytes  []byte
			err        error
		)
		switch j.CurrentCategory {
		case "repository":
			repos, err = j.GetModelDataRepository(ctx, *docs)
			if err == nil {
				if j.Publisher != nil {
					formattedData := make([]interface{}, 0)
					for _, d := range repos {
						formattedData = append(formattedData, d)
					}
					if len(repos) > 0 {
						err = j.Publisher.PushEvents(repos[0].Event(), "insights", GitHubDataSource, "repository", os.Getenv("STAGE"), formattedData)
					}
				} else {
					jsonBytes, err = jsoniter.Marshal(repos)
				}
			}
		case "issue":
			issuesData, err = j.GetModelDataIssue(ctx, *docs)
			if err == nil {
				if j.Publisher != nil {
					insightsStr := "insights"
					issuesStr := "issues"
					envStr := os.Getenv("STAGE")
					for k, v := range issuesData {
						switch k {
						case "created":
							ev, _ := v[0].(igh.IssueCreatedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						case "updated":
							ev, _ := v[0].(igh.IssueUpdatedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						case "closed":
							ev, _ := v[0].(igh.IssueClosedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						case "assignee_added":
							ev, _ := v[0].(igh.IssueAssigneeAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						case "assignee_removed":
							ev, _ := v[0].(igh.IssueAssigneeRemovedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						case "comment_added":
							ev, _ := v[0].(igh.IssueCommentAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						case "comment_edited":
							ev, _ := v[0].(igh.IssueCommentEditedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						case "comment_deleted":
							ev, _ := v[0].(igh.IssueCommentDeletedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						case "reaction_added":
							ev, _ := v[0].(igh.IssueReactionAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						case "reaction_removed":
							ev, _ := v[0].(igh.IssueReactionRemovedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						case "comment_reaction_added":
							ev, _ := v[0].(igh.IssueCommentReactionAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						case "comment_reaction_removed":
							ev, _ := v[0].(igh.IssueCommentReactionRemovedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, issuesStr, envStr, v)
						default:
							err = fmt.Errorf("unknown issue event type '%s'", k)
						}
						if err != nil {
							break
						}
					}
				} else {
					jsonBytes, err = jsoniter.Marshal(issuesData)
				}
			}
		case "pull_request":
			pullsData, err = j.GetModelDataPullRequest(ctx, *docs)
			if err == nil {
				if j.Publisher != nil {
					insightsStr := "insights"
					pullsStr := "pull_requests"
					envStr := os.Getenv("STAGE")
					for k, v := range pullsData {
						// shared.Printf("(k,len(v)) = ('%s',%d)\n", k, len(v))
						switch k {
						case "created":
							ev, _ := v[0].(igh.PullRequestCreatedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "updated":
							ev, _ := v[0].(igh.PullRequestUpdatedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "closed":
							ev, _ := v[0].(igh.PullRequestClosedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "merged":
							ev, _ := v[0].(igh.PullRequestMergedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "assignee_added":
							ev, _ := v[0].(igh.PullRequestAssigneeAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "assignee_removed":
							ev, _ := v[0].(igh.PullRequestAssigneeRemovedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "comment_added":
							ev, _ := v[0].(igh.PullRequestCommentAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "comment_edited":
							ev, _ := v[0].(igh.PullRequestCommentEditedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "comment_deleted":
							ev, _ := v[0].(igh.PullRequestCommentDeletedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "comment_reaction_added":
							ev, _ := v[0].(igh.PullRequestCommentReactionAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "comment_reaction_removed":
							ev, _ := v[0].(igh.PullRequestCommentReactionRemovedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						/* there are no such events
						case "reaction_added":
							ev, _ := v[0].(igh.PullRequestReactionAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "reaction_removed":
							ev, _ := v[0].(igh.PullRequestReactionRemovedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						*/
						case "review_added":
							ev, _ := v[0].(igh.PullRequestReviewAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "reviewer_added":
							ev, _ := v[0].(igh.PullRequestReviewerAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						case "reviewer_removed":
							ev, _ := v[0].(igh.PullRequestReviewerRemovedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GitHubDataSource, pullsStr, envStr, v)
						default:
							err = fmt.Errorf("unknown pull request event type '%s'", k)
						}
						if err != nil {
							break
						}
					}
				} else {
					jsonBytes, err = jsoniter.Marshal(pullsData)
				}
			}
		default:
			err = fmt.Errorf("unknown category: '%s'", j.CurrentCategory)
		}
		if err != nil {
			shared.Printf("Error: %+v\n", err)
			return
		}
		if j.Publisher == nil {
			shared.Printf("%s\n", string(jsonBytes))
		}
		*docs = []interface{}{}
		gMaxUpstreamDtMtx.Lock()
		defer gMaxUpstreamDtMtx.Unlock()
		shared.SetLastUpdate(ctx, j.Endpoint(), gMaxUpstreamDt)
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
	case "issue":
		return j.GitHubIssueEnrichItemsFunc(ctx, items, docs, final)
	case "pull_request":
		return j.GitHubPullRequestEnrichItemsFunc(ctx, items, docs, final)
	}
	return
}

// SyncCurrentCategory - sync GitHub data source for current category
func (j *DSGitHub) SyncCurrentCategory(ctx *shared.Ctx) (err error) {
	if j.CurrentCategory != "repository" {
		repo, e := j.githubRepo(ctx, j.Org, j.Repo)
		shared.FatalOnError(e)
		if repo == nil {
			shared.Fatalf("there is no such repo %s/%s", j.Org, j.Repo)
			return
		}
	}
	switch j.CurrentCategory {
	case "repository":
		return j.FetchItemsRepository(ctx)
	case "issue":
		return j.FetchItemsIssue(ctx)
	case "pull_request":
		return j.FetchItemsPullRequest(ctx)
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

// ItemNullableDate - return date value for a given field name, can be null
func (j *DSGitHub) ItemNullableDate(item interface{}, field string) *time.Time {
	iWhen, ok := shared.Dig(item, []string{field}, false, true)
	if !ok || iWhen == nil {
		return nil
	}
	sWhen, ok := iWhen.(string)
	if !ok {
		// shared.Printf("ItemNullableDate: incorrect date (non string): %v,%T\n", iWhen, iWhen)
		return nil
	}
	when, err := shared.TimeParseES(sWhen)
	if err != nil {
		// shared.Printf("ItemNullableDate: incorrect date (cannot parse): %s,%v\n", sWhen, err)
		return nil
	}
	return &when
}

// GetModelDataPullRequest - return pull requests data in lfx-event-schema format
func (j *DSGitHub) GetModelDataPullRequest(ctx *shared.Ctx, docs []interface{}) (data map[string][]interface{}, err error) {
	data = make(map[string][]interface{})
	defer func() {
		if err != nil {
			return
		}
		pullRequestBaseEvent := igh.PullRequestBaseEvent{
			Connector:        insights.GithubConnector,
			ConnectorVersion: GitHubBackendVersion,
			Source:           insights.GithubSource,
		}
		pullRequestAssigneeBaseEvent := igh.PullRequestAssigneeBaseEvent{
			Connector:        insights.GithubConnector,
			ConnectorVersion: GitHubBackendVersion,
			Source:           insights.GithubSource,
		}
		pullRequestCommentBaseEvent := igh.PullRequestCommentBaseEvent{
			Connector:        insights.GithubConnector,
			ConnectorVersion: GitHubBackendVersion,
			Source:           insights.GithubSource,
		}
		pullRequestCommentReactionBaseEvent := igh.PullRequestCommentReactionBaseEvent{
			Connector:        insights.GithubConnector,
			ConnectorVersion: GitHubBackendVersion,
			Source:           insights.GithubSource,
		}
		pullRequestReviewBaseEvent := igh.PullRequestReviewBaseEvent{
			Connector:        insights.GithubConnector,
			ConnectorVersion: GitHubBackendVersion,
			Source:           insights.GithubSource,
		}
		pullRequestReviewerBaseEvent := igh.PullRequestReviewerBaseEvent{
			Connector:        insights.GithubConnector,
			ConnectorVersion: GitHubBackendVersion,
			Source:           insights.GithubSource,
		}
		for k, v := range data {
			switch k {
			case "created":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.PullRequestCreatedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, pullRequest := range v {
					ary = append(ary, igh.PullRequestCreatedEvent{
						PullRequestBaseEvent: pullRequestBaseEvent,
						BaseEvent:            baseEvent,
						Payload:              pullRequest.(igh.PullRequest),
					})
				}
				data[k] = ary
			case "updated":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.PullRequestUpdatedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, pullRequest := range v {
					ary = append(ary, igh.PullRequestUpdatedEvent{
						PullRequestBaseEvent: pullRequestBaseEvent,
						BaseEvent:            baseEvent,
						Payload:              pullRequest.(igh.PullRequest),
					})
				}
				data[k] = ary
			case "closed":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.PullRequestClosedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, pullRequest := range v {
					ary = append(ary, igh.PullRequestClosedEvent{
						PullRequestBaseEvent: pullRequestBaseEvent,
						BaseEvent:            baseEvent,
						Payload:              pullRequest.(igh.PullRequest),
					})
				}
				data[k] = ary
			case "merged":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.PullRequestMergedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, pullRequest := range v {
					ary = append(ary, igh.PullRequestMergedEvent{
						PullRequestBaseEvent: pullRequestBaseEvent,
						BaseEvent:            baseEvent,
						Payload:              pullRequest.(igh.PullRequest),
					})
				}
				data[k] = ary
			case "assignee_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.PullRequestAssigneeAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, pullRequestAssignee := range v {
					ary = append(ary, igh.PullRequestAssigneeAddedEvent{
						PullRequestAssigneeBaseEvent: pullRequestAssigneeBaseEvent,
						BaseEvent:                    baseEvent,
						Payload:                      pullRequestAssignee.(igh.PullRequestAssignee),
					})
				}
				data[k] = ary
			case "comment_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.PullRequestCommentAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, pullRequestComment := range v {
					ary = append(ary, igh.PullRequestCommentAddedEvent{
						PullRequestCommentBaseEvent: pullRequestCommentBaseEvent,
						BaseEvent:                   baseEvent,
						Payload:                     pullRequestComment.(igh.PullRequestComment),
					})
				}
				data[k] = ary
			case "comment_reaction_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.PullRequestCommentReactionAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, pullRequestCommentReaction := range v {
					ary = append(ary, igh.PullRequestCommentReactionAddedEvent{
						PullRequestCommentReactionBaseEvent: pullRequestCommentReactionBaseEvent,
						BaseEvent:                           baseEvent,
						Payload:                             pullRequestCommentReaction.(igh.PullRequestCommentReaction),
					})
				}
				data[k] = ary
			case "review_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.PullRequestReviewAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, pullRequestReview := range v {
					ary = append(ary, igh.PullRequestReviewAddedEvent{
						PullRequestReviewBaseEvent: pullRequestReviewBaseEvent,
						BaseEvent:                  baseEvent,
						Payload:                    pullRequestReview.(igh.PullRequestReview),
					})
				}
				data[k] = ary
			case "reviewer_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.PullRequestReviewerAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, pullRequestReviewer := range v {
					ary = append(ary, igh.PullRequestReviewerAddedEvent{
						PullRequestReviewerBaseEvent: pullRequestReviewerBaseEvent,
						BaseEvent:                    baseEvent,
						Payload:                      pullRequestReviewer.(igh.PullRequestReviewer),
					})
				}
				data[k] = ary
			default:
				err = fmt.Errorf("unknown pull request '%s' event", k)
				return
			}
		}
	}()
	// pullRequestID, repoID, userID, pullRequestAssigneeID, pullRequestReactionID, pullRequestCommentID, pullRequestCommentReactionID := "", "", "", "", "", "", ""
	pullRequestID, repoID, userID, pullRequestAssigneeID, pullRequestCommentID, pullRequestCommentReactionID, pullRequestReviewID, pullRequestReviewerID := "", "", "", "", "", "", "", ""
	source := GitHubDataSource
	resolution := int64(5)
	for _, iDoc := range docs {
		nReactions := 0
		nComments := 0
		nReviews := 0
		doc, _ := iDoc.(map[string]interface{})
		createdOn, _ := doc["created_at"].(time.Time)
		updatedOn := j.ItemUpdatedOn(doc)
		githubRepoName, _ := doc["github_repo"].(string)
		repoShortName, _ := doc["repo_short_name"].(string)
		repoID, err = repository.GenerateRepositoryID(j.SourceID, j.URL, source)
		// shared.Printf("repository.GenerateRepositoryID(%s, %s, %s) -> %s,%v\n", j.SourceID, j.URL, source, repoID, err)
		if err != nil {
			shared.Printf("GenerateRepositoryID(%s,%s,%s): %+v for %+v\n", j.SourceID, j.URL, source, err, doc)
			return
		}
		fIID, _ := doc["pull_request_id"].(float64)
		sIID := fmt.Sprintf("%.0f", fIID)
		pullRequestID, err = igh.GenerateGithubPullRequestID(repoID, sIID)
		// shared.Printf("igh.GenerateGithubPullRequestID(%s, %s) -> %s,%v\n", repoID, sIID, pullRequestID, err)
		if err != nil {
			shared.Printf("GenerateGithubPullRequestID(%s,%s): %+v for %+v\n", repoID, sIID, err, doc)
			return
		}
		splitted := strings.Split(githubRepoName, "/")
		org := splitted[0]
		labels, _ := doc["labels"].([]string)
		title, _ := doc["title"].(string)
		body, _ := doc["body"].(string)
		url, _ := doc["url"].(string)
		state, _ := doc["state"].(string)
		closedOn := j.ItemNullableDate(doc, "closed_at")
		mergedOn := j.ItemNullableDate(doc, "merged_at")
		isClosed := closedOn != nil
		isMerged := mergedOn != nil
		var (
			mergedBy *insights.Contributor
			closedBy *insights.Contributor
		)
		mergedBy, closedBy = nil, nil
		pullRequestContributors := []insights.Contributor{}
		possiblyAddOwnerContributor := func(role map[string]interface{}, contributor insights.Contributor) {
			siteAdmin, _ := role["site_admin"].(bool)
			if siteAdmin {
				contributor.Role = insights.OwnerRole
				pullRequestContributors = append(pullRequestContributors, contributor)
				// fmt.Printf("added owner: %s\n", shared.PrettyPrint(contributor))
			}
		}
		// Primary assignee start
		primaryAssignee := ""
		roles, okRoles := doc["roles"].([]map[string]interface{})
		if okRoles {
			for _, role := range roles {
				roleType, _ := role["role"].(string)
				if roleType != "assignee_data" && roleType != "user_data" && roleType != "merged_by_data" {
					continue
				}
				username, _ := role["username"].(string)
				primaryAssignee = username
				name, _ := role["name"].(string)
				email, _ := role["email"].(string)
				avatarURL, _ := role["avatar_url"].(string)
				// No identity data postprocessing in V2
				// name, username = shared.PostprocessNameUsername(name, username, email)
				userID, err = user.GenerateIdentity(&source, &email, &name, &username)
				if err != nil {
					shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
					return
				}
				roleValue := insights.AuthorRole
				if roleType == "assignee_data" {
					roleValue = insights.AssigneeRole
				} else if roleType == "merged_by_data" {
					roleValue = insights.MergeAuthorRole
				}
				contributor := insights.Contributor{
					Role:   roleValue,
					Weight: 1.0,
					Identity: user.UserIdentityObjectBase{
						ID:         userID,
						Avatar:     avatarURL,
						Email:      email,
						IsVerified: false,
						Name:       name,
						Username:   username,
						Source:     source,
					},
				}
				if roleType == "merged_by_data" {
					mergedBy = &contributor
				}
				pullRequestContributors = append(pullRequestContributors, contributor)
				possiblyAddOwnerContributor(role, contributor)
				if roleType != "assignee_data" {
					continue
				}
				assigneeSID := sIID + ":" + username
				pullRequestAssigneeID, err = igh.GenerateGithubAssigneeID(repoID, pullRequestID, assigneeSID)
				if err != nil {
					shared.Printf("GenerateGithubAssigneeID(%s,%s,%s): %+v for %+v\n", repoID, pullRequestID, assigneeSID, err, doc)
					return
				}
				pullRequestAssignee := igh.PullRequestAssignee{
					ID:            pullRequestAssigneeID,
					PullRequestID: pullRequestID,
					Assignee: insights.Assignee{
						AssigneeID:      username,
						Contributor:     contributor,
						SourceTimestamp: createdOn,
						SyncTimestamp:   time.Now(),
					},
				}
				key := "assignee_added"
				ary, ok := data[key]
				if !ok {
					ary = []interface{}{pullRequestAssignee}
				} else {
					ary = append(ary, pullRequestAssignee)
				}
				data[key] = ary
			}
		}
		// Primary assignee end
		// Other assignees start
		assigneesAry, okAssignees := doc["assignees_array"].([]interface{})
		if okAssignees {
			for _, iAssignee := range assigneesAry {
				assignee, okAssignee := iAssignee.(map[string]interface{})
				if !okAssignee || assignee == nil {
					continue
				}
				roles, okRoles := assignee["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "assignee" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					if username == primaryAssignee {
						continue
					}
					email, _ := role["email"].(string)
					avatarURL, _ := role["avatar_url"].(string)
					// No identity data postprocessing in V2
					// name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.AssigneeRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					pullRequestContributors = append(pullRequestContributors, contributor)
					possiblyAddOwnerContributor(role, contributor)
					assigneeSID := sIID + ":" + username
					pullRequestAssigneeID, err = igh.GenerateGithubAssigneeID(repoID, pullRequestID, assigneeSID)
					if err != nil {
						shared.Printf("GenerateGithubAssigneeID(%s,%s,%s): %+v for %+v\n", repoID, pullRequestID, assigneeSID, err, doc)
						return
					}
					pullRequestAssignee := igh.PullRequestAssignee{
						ID:            pullRequestAssigneeID,
						PullRequestID: pullRequestID,
						Assignee: insights.Assignee{
							AssigneeID:      username,
							Contributor:     contributor,
							SyncTimestamp:   time.Now(),
							SourceTimestamp: createdOn,
						},
					}
					key := "assignee_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{pullRequestAssignee}
					} else {
						ary = append(ary, pullRequestAssignee)
					}
					data[key] = ary
				}
			}
		}
		// Other assignees end
		// Reviews of state=COMMENTED come without body from reviews API
		// PR Comments are actually review comments (state=COMMENTED) - they have body but miss the review part
		// We need to correlate them and make both reviews data and comments data contain both body and review ID
		// correlation: (author login, review comment unix timestamp) -> (reviewID, review comment body, author, unix timestamp)
		correlation := make(map[string][4]string)
		correlation2 := make(map[string][4]string)
		commentsAry, okComments := doc["review_comments_array"].([]interface{})
		reviewsAry, okReviews := doc["reviews_array"].([]interface{})
		if okReviews {
			for _, iReview := range reviewsAry {
				review, okReview := iReview.(map[string]interface{})
				if !okReview || review == nil {
					continue
				}
				roles, okRoles := review["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				sReviewState, _ := review["state"].(string)
				if sReviewState != "COMMENTED" {
					continue
				}
				sAuthorLogin, _ := review["author_login"].(string)
				reviewCreatedOn, _ := review["metadata__updated_on"].(time.Time)
				reviewID, _ := review["pull_request_review_id"].(int64)
				sReviewID := fmt.Sprintf("%d", reviewID)
				ts := reviewCreatedOn.Unix()
				ts2 := ts / resolution
				key := fmt.Sprintf("%s:%d", sAuthorLogin, ts)
				key2 := fmt.Sprintf("%s:%d", sAuthorLogin, ts2)
				pullRequestReviewID, err = igh.GenerateGithubReviewID(repoID, sReviewID)
				if err != nil {
					shared.Printf("GenerateGithubReviewID(%s,%s): %+v for %+v\n", repoID, sReviewID, err, doc)
					return
				}
				_, ok := correlation[key]
				if ok && ctx.Debug > 0 {
					shared.Printf("WARNING: non-unique key '%s'(%v) within a PR %s(%s)\ncomments=%s\nreviews=%s\n", key, reviewCreatedOn, url, sReviewID, shared.PrettyPrint(commentsAry), shared.PrettyPrint(reviewsAry))
				}
				correlation[key] = [4]string{pullRequestReviewID, "", sAuthorLogin, fmt.Sprintf("%d", ts)}
				correlation2[key2] = [4]string{pullRequestReviewID, "", sAuthorLogin, fmt.Sprintf("%d", ts2)}
			}
		}
		if okComments {
			for _, iComment := range commentsAry {
				comment, okComment := iComment.(map[string]interface{})
				if !okComment || comment == nil {
					continue
				}
				roles, okRoles := comment["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				sAuthorLogin, _ := comment["author_login"].(string)
				sBody, _ := comment["body"].(string)
				commentCreatedOn, _ := comment["metadata__updated_on"].(time.Time)
				ts := commentCreatedOn.Unix()
				ts2 := ts / resolution
				key := fmt.Sprintf("%s:%d", sAuthorLogin, ts)
				key2 := fmt.Sprintf("%s:%d", sAuthorLogin, ts2)
				ary, ok := correlation[key]
				if !ok {
					ary, ok = correlation2[key2]
					if !ok {
						if ctx.Debug > 0 {
							shared.Printf("WARNING: cannot find review data for key '%s'(%v) within a PR %s(%s)\ncomments=%s\nreviews=%s\n", key, commentCreatedOn, url, sBody, shared.PrettyPrint(commentsAry), shared.PrettyPrint(reviewsAry))
						}
						correlation[key] = [4]string{"", sBody, sAuthorLogin, fmt.Sprintf("%d", ts)}
						correlation2[key2] = [4]string{"", sBody, sAuthorLogin, fmt.Sprintf("%d", ts2)}
						continue
					}
				}
				ary[1] = sBody
				correlation[key] = ary
				ary[3] = fmt.Sprintf("%d", ts2)
				correlation2[key2] = ary
			}
		}
		for key, ary := range correlation {
			reviewID := ary[0]
			body := ary[1]
			if reviewID == "" || body == "" {
				sts := ary[3]
				ts, e := strconv.ParseInt(sts, 10, 64)
				if e != nil {
					continue
				}
				author := ary[2]
				ts2 := ts / resolution
				key2 := fmt.Sprintf("%s:%d", author, ts2)
				ary2, ok := correlation2[key2]
				if !ok && ctx.Debug > 0 {
					shared.Printf("WARNING: cannot find review data for key '%s'(%v) within a PR %s\ncomments=%s\nreviews=%s\n", key, ary, url, shared.PrettyPrint(commentsAry), shared.PrettyPrint(reviewsAry))
				}
				if reviewID == "" {
					reviewID = ary2[0]
					ary[0] = ary2[0]
				}
				if body == "" {
					body = ary2[1]
					ary[1] = ary2[1]
				}
				correlation[key] = ary
			}
		}
		// fmt.Printf("correlation=%+v\n", correlation)
		// fmt.Printf("correlation2=%+v\n", correlation2)
		// Correlation calculate ends
		// Comments start
		commentsAry, okComments = doc["review_comments_array"].([]interface{})
		if okComments {
			for _, iComment := range commentsAry {
				comment, okComment := iComment.(map[string]interface{})
				if !okComment || comment == nil {
					continue
				}
				roles, okRoles := comment["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				sCommentBody, _ := comment["body"].(string)
				sCommentURL, _ := comment["html_url"].(string)
				commentCreatedOn, _ := comment["metadata__updated_on"].(time.Time)
				commentID, _ := comment["pull_request_comment_id"].(int64)
				sCommentID := fmt.Sprintf("%d", commentID)
				if commentCreatedOn.After(updatedOn) {
					updatedOn = commentCreatedOn
				}
				sAuthorLogin, _ := comment["author_login"].(string)
				ts := commentCreatedOn.Unix()
				key := fmt.Sprintf("%s:%d", sAuthorLogin, ts)
				sReviewID := ""
				ary, ok := correlation[key]
				if ok {
					sReviewID = ary[0]
				}
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "user_data" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					email, _ := role["email"].(string)
					avatarURL, _ := role["avatar_url"].(string)
					// No identity data postprocessing in V2
					// name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.CommenterRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					pullRequestContributors = append(pullRequestContributors, contributor)
					possiblyAddOwnerContributor(role, contributor)
					commentSID := sCommentID
					pullRequestCommentID, err = igh.GenerateGithubCommentID(repoID, commentSID)
					if err != nil {
						shared.Printf("GenerateGithubCommentID(%s,%s): %+v for %+v\n", repoID, commentSID, err, doc)
						return
					}
					pullRequestComment := igh.PullRequestComment{
						ID:              pullRequestCommentID,
						PullRequestID:   pullRequestID,
						IsReviewComment: true,
						ReviewID:        sReviewID,
						Comment: insights.Comment{
							Body:            sCommentBody,
							CommentURL:      sCommentURL,
							SourceTimestamp: commentCreatedOn,
							SyncTimestamp:   time.Now(),
							CommentID:       commentSID,
							Contributor:     contributor,
							Orphaned:        false,
						},
					}
					key := "comment_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{pullRequestComment}
					} else {
						ary = append(ary, pullRequestComment)
					}
					data[key] = ary
					nComments++
				}
			}
		}
		// Comments end
		// Comments reactions start
		reactionsAry, okReactions := doc["review_comments_reactions_array"].([]interface{})
		if okReactions {
			for _, iReaction := range reactionsAry {
				reaction, okReaction := iReaction.(map[string]interface{})
				if !okReaction || reaction == nil {
					continue
				}
				commentID, _ := reaction["pull_request_comment_id"].(int64)
				sCommentID := fmt.Sprintf("%d", commentID)
				reactionCreatedOn, _ := reaction["metadata__updated_on"].(time.Time)
				roles, okRoles := reaction["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				content, _ := reaction["content"].(string)
				emojiContent := j.emojiForContent(content)
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "user_data" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					email, _ := role["email"].(string)
					avatarURL, _ := role["avatar_url"].(string)
					// No identity data postprocessing in V2
					// name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.ReactionAuthorRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					pullRequestContributors = append(pullRequestContributors, contributor)
					possiblyAddOwnerContributor(role, contributor)
					reactionSID := sCommentID + ":" + content
					pullRequestCommentReactionID, err = igh.GenerateGithubReactionID(repoID, reactionSID)
					if err != nil {
						shared.Printf("GenerateGithubReactionID(%s,%s): %+v for %+v\n", repoID, reactionSID, err, doc)
						return
					}
					pullRequestCommentReaction := igh.PullRequestCommentReaction{
						ID:        pullRequestCommentReactionID,
						CommentID: sCommentID,
						Reaction: insights.Reaction{
							Emoji: service.Emoji{
								ID:      content,
								Unicode: emojiContent,
							},
							ReactionID:      reactionSID,
							SourceTimestamp: reactionCreatedOn,
							SyncTimestamp:   time.Now(),
							Contributor:     contributor,
						},
					}
					key := "comment_reaction_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{pullRequestCommentReaction}
					} else {
						ary = append(ary, pullRequestCommentReaction)
					}
					data[key] = ary
					nReactions++
				}
			}
		}
		// Comments reactions end
		// Comments start (comments stored on the issue part of PR)
		commentsAry, okComments = doc["comments_array"].([]interface{})
		if okComments {
			for _, iComment := range commentsAry {
				comment, okComment := iComment.(map[string]interface{})
				if !okComment || comment == nil {
					continue
				}
				roles, okRoles := comment["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				sCommentBody, _ := comment["body"].(string)
				sCommentURL, _ := comment["html_url"].(string)
				commentCreatedOn, _ := comment["metadata__updated_on"].(time.Time)
				commentID, _ := comment["pull_request_comment_id"].(int64)
				sCommentID := fmt.Sprintf("%d", commentID)
				if commentCreatedOn.After(updatedOn) {
					updatedOn = commentCreatedOn
				}
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "user_data" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					email, _ := role["email"].(string)
					avatarURL, _ := role["avatar_url"].(string)
					// No identity data postprocessing in V2
					// name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.CommenterRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					pullRequestContributors = append(pullRequestContributors, contributor)
					possiblyAddOwnerContributor(role, contributor)
					commentSID := sCommentID
					pullRequestCommentID, err = igh.GenerateGithubCommentID(repoID, commentSID)
					if err != nil {
						shared.Printf("GenerateGithubCommentID(%s,%s): %+v for %+v\n", repoID, commentSID, err, doc)
						return
					}
					pullRequestComment := igh.PullRequestComment{
						ID:              pullRequestCommentID,
						PullRequestID:   pullRequestID,
						IsReviewComment: false,
						ReviewID:        "",
						Comment: insights.Comment{
							Body:            sCommentBody,
							CommentURL:      sCommentURL,
							SourceTimestamp: commentCreatedOn,
							SyncTimestamp:   time.Now(),
							CommentID:       commentSID,
							Contributor:     contributor,
							Orphaned:        false,
						},
					}
					key := "comment_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{pullRequestComment}
					} else {
						ary = append(ary, pullRequestComment)
					}
					data[key] = ary
					nComments++
				}
			}
		}
		// Comments end (comments stored on the issue part of PR)
		// Comment reactions start (reactions to comments stored on the issue part of PR)
		reactionsAry, okReactions = doc["comments_reactions_array"].([]interface{})
		if okReactions {
			for _, iReaction := range reactionsAry {
				reaction, okReaction := iReaction.(map[string]interface{})
				if !okReaction || reaction == nil {
					continue
				}
				commentID, _ := reaction["pull_request_comment_id"].(int64)
				sCommentID := fmt.Sprintf("%d", commentID)
				reactionCreatedOn, _ := reaction["metadata__updated_on"].(time.Time)
				roles, okRoles := reaction["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				content, _ := reaction["content"].(string)
				emojiContent := j.emojiForContent(content)
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "user_data" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					email, _ := role["email"].(string)
					avatarURL, _ := role["avatar_url"].(string)
					// No identity data postprocessing in V2
					// name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.ReactionAuthorRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					pullRequestContributors = append(pullRequestContributors, contributor)
					possiblyAddOwnerContributor(role, contributor)
					reactionSID := sCommentID + ":" + content
					pullRequestCommentReactionID, err = igh.GenerateGithubReactionID(repoID, reactionSID)
					if err != nil {
						shared.Printf("GenerateGithubReactionID(%s,%s): %+v for %+v\n", repoID, reactionSID, err, doc)
						return
					}
					pullRequestCommentReaction := igh.PullRequestCommentReaction{
						ID:        pullRequestCommentReactionID,
						CommentID: sCommentID,
						Reaction: insights.Reaction{
							Emoji: service.Emoji{
								ID:      content,
								Unicode: emojiContent,
							},
							ReactionID:      reactionSID,
							SourceTimestamp: reactionCreatedOn,
							SyncTimestamp:   time.Now(),
							Contributor:     contributor,
						},
					}
					key := "comment_reaction_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{pullRequestCommentReaction}
					} else {
						ary = append(ary, pullRequestCommentReaction)
					}
					data[key] = ary
					nReactions++
				}
			}
		}
		// Comment reactions end (reactions to comments stored on the issue part of PR)
		// Reviews start
		reviewsAry, okReviews = doc["reviews_array"].([]interface{})
		if okReviews {
			for _, iReview := range reviewsAry {
				review, okReview := iReview.(map[string]interface{})
				if !okReview || review == nil {
					continue
				}
				roles, okRoles := review["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				sReviewState, _ := review["state"].(string)
				sReviewBody, _ := review["body"].(string)
				reviewCreatedOn, _ := review["metadata__updated_on"].(time.Time)
				reviewID, _ := review["pull_request_review_id"].(int64)
				sReviewID := fmt.Sprintf("%d", reviewID)
				if reviewCreatedOn.After(updatedOn) {
					updatedOn = reviewCreatedOn
				}
				var state igh.ReviewState
				switch sReviewState {
				case "APPROVED":
					state = igh.ApprovedReviewState
				case "CHANGES_REQUESTED":
					state = igh.ChangeRequestedReviewState
				case "DISMISSED":
					state = igh.DismissedReviewState
				case "COMMENTED":
					state = igh.CommentedReviewState
					// NOTE:
					// Those "reviews" come without comment body and are duplicted in comments data, this is why they need to be skipped.
					if sReviewBody == "" {
						sAuthorLogin, _ := review["author_login"].(string)
						ts := reviewCreatedOn.Unix()
						key := fmt.Sprintf("%s:%d", sAuthorLogin, ts)
						ary, ok := correlation[key]
						if ok {
							sReviewBody = ary[1]
						}
					}
				default:
					shared.Printf("WARNING: unknown review state '%s', skipping\n", sReviewState)
					continue
				}
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "user_data" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					email, _ := role["email"].(string)
					avatarURL, _ := role["avatar_url"].(string)
					// No identity data postprocessing in V2
					// name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.ReviewerRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					pullRequestContributors = append(pullRequestContributors, contributor)
					possiblyAddOwnerContributor(role, contributor)
					reviewSID := sReviewID
					pullRequestReviewID, err = igh.GenerateGithubReviewID(repoID, reviewSID)
					if err != nil {
						shared.Printf("GenerateGithubReviewID(%s,%s): %+v for %+v\n", repoID, reviewSID, err, doc)
						return
					}
					// If we want to have different reviewer ID for the same user in different repos
					// reviewerSID := sIID + ":" + username
					reviewerSID := username
					pullRequestReviewerID, err = igh.GenerateGithubReviewerID(repoID, pullRequestID, reviewerSID)
					if err != nil {
						shared.Printf("GenerateGithubReviewerID(%s,%s,%s): %+v for %+v\n", repoID, pullRequestID, reviewerSID, err, doc)
						return
					}
					pullRequestReview := igh.PullRequestReview{
						ID:            pullRequestReviewID,
						PullRequestID: pullRequestID,
						ReviewerID:    pullRequestReviewerID,
						Review: igh.Review{
							ReviewID: reviewSID,
							State:    state,
							Body:     sReviewBody,
							Reviewer: insights.Reviewer{
								ReviewerID:      username,
								Contributor:     contributor,
								SyncTimestamp:   time.Now(),
								SourceTimestamp: reviewCreatedOn,
							},
						},
					}
					key := "review_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{pullRequestReview}
					} else {
						ary = append(ary, pullRequestReview)
					}
					data[key] = ary
					nReviews++
				}
			}
		}
		// Reviews end
		// Requested reviewers start
		requestedReviewersAry, okRequestedReviewers := doc["requested_reviewers_array"].([]interface{})
		if okRequestedReviewers {
			for _, iRequestedReviewer := range requestedReviewersAry {
				requestedReviewer, okRequestedReviewer := iRequestedReviewer.(map[string]interface{})
				if !okRequestedReviewer || requestedReviewer == nil {
					continue
				}
				roles, okRoles := requestedReviewer["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "requested_reviewer" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					email, _ := role["email"].(string)
					avatarURL, _ := role["avatar_url"].(string)
					// No identity data postprocessing in V2
					// name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.RequestedReviewerRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					pullRequestContributors = append(pullRequestContributors, contributor)
					possiblyAddOwnerContributor(role, contributor)
					// If we want to have different reviewer ID for the same user in different repos
					// reviewerSID := sIID + ":" + username
					reviewerSID := username
					pullRequestReviewerID, err = igh.GenerateGithubReviewerID(repoID, pullRequestID, reviewerSID)
					if err != nil {
						shared.Printf("GenerateGithubReviewerID(%s,%s,%s): %+v for %+v\n", repoID, pullRequestID, reviewerSID, err, doc)
						return
					}
					pullRequestReviewer := igh.PullRequestReviewer{
						ID:            pullRequestReviewerID,
						PullRequestID: pullRequestID,
						Reviewer: insights.Reviewer{
							ReviewerID:      username,
							Contributor:     contributor,
							SyncTimestamp:   time.Now(),
							SourceTimestamp: createdOn,
						},
					}
					key := "reviewer_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{pullRequestReviewer}
					} else {
						ary = append(ary, pullRequestReviewer)
					}
					data[key] = ary
				}
			}
		}
		// Requested reviewers end
		// Final PullRequest object
		shas := []string{}
		commitsAry, okCommits := doc["commits_array"].([]interface{})
		if okCommits {
			for _, iCommit := range commitsAry {
				commit, okCommit := iCommit.(map[string]interface{})
				if !okCommit || commit == nil {
					continue
				}
				sha, _ := commit["sha"].(string)
				if sha == "" {
					continue
				}
				shas = append(shas, sha)
			}
		}
		pullRequest := igh.PullRequest{
			ID:            pullRequestID,
			RepositoryID:  repoID,
			RepositoryURL: j.URL,
			Repository:    repoShortName,
			Organization:  org,
			Labels:        labels,
			Commits:       shas,
			Contributors:  shared.DedupContributors(pullRequestContributors),
			MergedBy:      mergedBy,
			ClosedBy:      closedBy,
			ChangeRequest: insights.ChangeRequest{
				Title:            title,
				Body:             body,
				ChangeRequestID:  sIID,
				ChangeRequestURL: url,
				State:            insights.ChangeRequestState(state),
				SyncTimestamp:    time.Now(),
				SourceTimestamp:  createdOn,
				Orphaned:         false,
			},
		}
		isNew := false
		if !updatedOn.After(createdOn) || (nComments == 0 && nReactions == 0 && nReviews == 0) {
			isNew = true
		}
		key := "updated"
		if isNew {
			key = "created"
		}
		ary, ok := data[key]
		if !ok {
			ary = []interface{}{pullRequest}
		} else {
			ary = append(ary, pullRequest)
		}
		data[key] = ary
		// Fake merge "event"
		if isMerged {
			// pullRequest.Contributors = []insights.Contributor{}
			pullRequest.SyncTimestamp = time.Now()
			pullRequest.SourceTimestamp = *mergedOn
			key := "merged"
			ary, ok := data[key]
			if !ok {
				ary = []interface{}{pullRequest}
			} else {
				ary = append(ary, pullRequest)
			}
			data[key] = ary
		}
		// Fake "close" event (not merged and closed)
		if isClosed && !isMerged {
			// pullRequest.Contributors = []insights.Contributor{}
			pullRequest.SyncTimestamp = time.Now()
			pullRequest.SourceTimestamp = *closedOn
			key := "closed"
			ary, ok := data[key]
			if !ok {
				ary = []interface{}{pullRequest}
			} else {
				ary = append(ary, pullRequest)
			}
			data[key] = ary
		}
		gMaxUpstreamDtMtx.Lock()
		if updatedOn.After(gMaxUpstreamDt) {
			gMaxUpstreamDt = updatedOn
		}
		gMaxUpstreamDtMtx.Unlock()
	}
	return
}

// GetModelDataRepository - return repository data in lfx-event-schema format
func (j *DSGitHub) GetModelDataRepository(ctx *shared.Ctx, docs []interface{}) (data []repository.RepositoryUpdatedEvent, err error) {
	repositoryBaseEvent := repository.RepositoryBaseEvent{
		// FIXME: there is no connector data in 'RepositoryBaseEvent'
		// Connector:        insights.GithubConnector,
		// ConnectorVersion: GitHubBackendVersion,
		// Source:           insights.GithubSource,
	}
	ev := repository.RepositoryUpdatedEvent{}
	baseEvent := service.BaseEvent{
		Type: service.EventType(ev.Event()),
		CRUDInfo: service.CRUDInfo{
			CreatedBy: GitHubConnector,
			UpdatedBy: GitHubConnector,
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
		},
	}
	repoID := ""
	for _, iDoc := range docs {
		doc, _ := iDoc.(map[string]interface{})
		// shared.Printf("%+v\n", doc)
		updatedOn := j.ItemUpdatedOn(doc)
		forks, _ := doc["forks_count"].(float64)
		subscribers, _ := doc["subscribers_count"].(float64)
		stargazers, _ := doc["stargazers_count"].(float64)
		description, _ := doc["description"].(string)
		sCreatedAt, _ := doc["created_at"].(string)
		createdAt, _ := shared.TimeParseES(sCreatedAt)
		// id is github repository id
		id, _ := doc["id"].(float64)
		sid := fmt.Sprintf("%.0f", id)
		// repoName, _ := doc["repo_name"].(string)
		repoID, err = repository.GenerateRepositoryID(j.SourceID, j.URL, GitHubDataSource)
		// shared.Printf("repository.GenerateRepositoryID(%s, %s, %s) -> %s,%v (%s)\n", j.SourceID, j.URL, GitHubDataSource, repoID, err, sid)
		if err != nil {
			shared.Printf("GenerateRepositoryID(%s,%s,%s): %+v for %+v\n", j.SourceID, j.URL, GitHubDataSource, err, doc)
			return
		}
		// Event
		repo := repository.RepositoryObjectBase{
			ID:              repoID,
			SourceID:        sid,
			URL:             j.URL,
			Description:     description,
			ReportingSource: repository.InsightsService,
			EnabledServices: []string{string(repository.InsightsService)},
			Source:          GitHubDataSource,
			Stats: []repository.RepositoryStats{
				{
					CalculatedAt: updatedOn,
					Forks:        int(forks),
					Stargazers:   int(stargazers),
					Subscribers:  int(subscribers),
				},
			},
			CreatedAt: createdAt,
			UpdatedAt: time.Now(),
		}
		data = append(data, repository.RepositoryUpdatedEvent{
			RepositoryBaseEvent: repositoryBaseEvent,
			BaseEvent:           baseEvent,
			Payload:             repo,
		})
		gMaxUpstreamDtMtx.Lock()
		if updatedOn.After(gMaxUpstreamDt) {
			gMaxUpstreamDt = updatedOn
		}
		gMaxUpstreamDtMtx.Unlock()
	}
	return
}

// GetModelDataIssue - return issues data in lfx-event-schema format
func (j *DSGitHub) GetModelDataIssue(ctx *shared.Ctx, docs []interface{}) (data map[string][]interface{}, err error) {
	data = make(map[string][]interface{})
	defer func() {
		if err != nil {
			return
		}
		issueBaseEvent := igh.IssueBaseEvent{
			Connector:        insights.GithubConnector,
			ConnectorVersion: GitHubBackendVersion,
			Source:           insights.GithubSource,
		}
		issueAssigneeBaseEvent := igh.IssueAssigneeBaseEvent{
			Connector:        insights.GithubConnector,
			ConnectorVersion: GitHubBackendVersion,
			Source:           insights.GithubSource,
		}
		issueCommentBaseEvent := igh.IssueCommentBaseEvent{
			Connector:        insights.GithubConnector,
			ConnectorVersion: GitHubBackendVersion,
			Source:           insights.GithubSource,
		}
		issueReactionBaseEvent := igh.IssueReactionBaseEvent{
			Connector:        insights.GithubConnector,
			ConnectorVersion: GitHubBackendVersion,
			Source:           insights.GithubSource,
		}
		issueCommentReactionBaseEvent := igh.IssueCommentReactionBaseEvent{
			Connector:        insights.GithubConnector,
			ConnectorVersion: GitHubBackendVersion,
			Source:           insights.GithubSource,
		}
		for k, v := range data {
			switch k {
			case "created":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.IssueCreatedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issue := range v {
					ary = append(ary, igh.IssueCreatedEvent{
						IssueBaseEvent: issueBaseEvent,
						BaseEvent:      baseEvent,
						Payload:        issue.(igh.Issue),
					})
				}
				data[k] = ary
			case "updated":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.IssueUpdatedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issue := range v {
					ary = append(ary, igh.IssueUpdatedEvent{
						IssueBaseEvent: issueBaseEvent,
						BaseEvent:      baseEvent,
						Payload:        issue.(igh.Issue),
					})
				}
				data[k] = ary
			case "closed":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.IssueClosedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issue := range v {
					ary = append(ary, igh.IssueClosedEvent{
						IssueBaseEvent: issueBaseEvent,
						BaseEvent:      baseEvent,
						Payload:        issue.(igh.Issue),
					})
				}
				data[k] = ary
			case "assignee_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.IssueAssigneeAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issueAssignee := range v {
					ary = append(ary, igh.IssueAssigneeAddedEvent{
						IssueAssigneeBaseEvent: issueAssigneeBaseEvent,
						BaseEvent:              baseEvent,
						Payload:                issueAssignee.(igh.IssueAssignee),
					})
				}
				data[k] = ary
			case "comment_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.IssueCommentAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issueComment := range v {
					ary = append(ary, igh.IssueCommentAddedEvent{
						IssueCommentBaseEvent: issueCommentBaseEvent,
						BaseEvent:             baseEvent,
						Payload:               issueComment.(igh.IssueComment),
					})
				}
				data[k] = ary
			case "reaction_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.IssueReactionAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issueReaction := range v {
					ary = append(ary, igh.IssueReactionAddedEvent{
						IssueReactionBaseEvent: issueReactionBaseEvent,
						BaseEvent:              baseEvent,
						Payload:                issueReaction.(igh.IssueReaction),
					})
				}
				data[k] = ary
			case "comment_reaction_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(igh.IssueCommentReactionAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GitHubConnector,
						UpdatedBy: GitHubConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issueCommentReaction := range v {
					ary = append(ary, igh.IssueCommentReactionAddedEvent{
						IssueCommentReactionBaseEvent: issueCommentReactionBaseEvent,
						BaseEvent:                     baseEvent,
						Payload:                       issueCommentReaction.(igh.IssueCommentReaction),
					})
				}
				data[k] = ary
			default:
				err = fmt.Errorf("unknown issue '%s' event", k)
				return
			}
		}
	}()
	issueID, repoID, userID, issueAssigneeID, issueReactionID, issueCommentID, issueCommentReactionID := "", "", "", "", "", "", ""
	source := GitHubDataSource
	for _, iDoc := range docs {
		nReactions := 0
		nComments := 0
		doc, _ := iDoc.(map[string]interface{})
		createdOn, _ := doc["created_at"].(time.Time)
		updatedOn := j.ItemUpdatedOn(doc)
		closedOn := j.ItemNullableDate(doc, "closed_at")
		isClosed := closedOn != nil
		var closedBy *insights.Contributor
		closedBy = nil
		githubRepoName, _ := doc["github_repo"].(string)
		repoShortName, _ := doc["repo_short_name"].(string)
		repoID, err = repository.GenerateRepositoryID(j.SourceID, j.URL, source)
		// shared.Printf("repository.GenerateRepositoryID(%s, %s, %s) -> %s,%v\n", j.SourceID, j.URL, source, repoID, err)
		if err != nil {
			shared.Printf("GenerateRepositoryID(%s,%s,%s): %+v for %+v\n", j.SourceID, j.URL, source, err, doc)
			return
		}
		fIID, _ := doc["issue_id"].(float64)
		sIID := fmt.Sprintf("%.0f", fIID)
		issueID, err = igh.GenerateGithubIssueID(repoID, sIID)
		// shared.Printf("igh.GenerateGithubIssueID(%s, %s) -> %s,%v\n", repoID, sIID, issueID, err)
		if err != nil {
			shared.Printf("GenerateGithubIssueID(%s,%s): %+v for %+v\n", repoID, sIID, err, doc)
			return
		}
		splitted := strings.Split(githubRepoName, "/")
		org := splitted[0]
		labels, _ := doc["labels"].([]string)
		title, _ := doc["title"].(string)
		body, _ := doc["body"].(string)
		url, _ := doc["url"].(string)
		state, _ := doc["state"].(string)
		// We need an information that issue is a PR (GitHub specific)
		isPullRequest, _ := doc["pull_request"].(bool)
		issueContributors := []insights.Contributor{}
		possiblyAddOwnerContributor := func(role map[string]interface{}, contributor insights.Contributor) {
			siteAdmin, _ := role["site_admin"].(bool)
			if siteAdmin {
				contributor.Role = insights.OwnerRole
				issueContributors = append(issueContributors, contributor)
				// fmt.Printf("added owner: %s\n", shared.PrettyPrint(contributor))
			}
		}
		// Primary assignee start
		primaryAssignee := ""
		roles, okRoles := doc["roles"].([]map[string]interface{})
		if okRoles {
			for _, role := range roles {
				roleType, _ := role["role"].(string)
				if roleType != "assignee_data" && roleType != "user_data" && roleType != "closed_by_data" {
					continue
				}
				name, _ := role["name"].(string)
				username, _ := role["username"].(string)
				primaryAssignee = username
				email, _ := role["email"].(string)
				avatarURL, _ := role["avatar_url"].(string)
				// No identity data postprocessing in V2
				// name, username = shared.PostprocessNameUsername(name, username, email)
				userID, err = user.GenerateIdentity(&source, &email, &name, &username)
				if err != nil {
					shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
					return
				}
				roleValue := insights.AuthorRole
				if roleType == "assignee_data" {
					roleValue = insights.AssigneeRole
				} else if roleType == "closed_by_data" {
					roleValue = insights.CloseAuthorRole
				}
				contributor := insights.Contributor{
					Role:   roleValue,
					Weight: 1.0,
					Identity: user.UserIdentityObjectBase{
						ID:         userID,
						Avatar:     avatarURL,
						Email:      email,
						IsVerified: false,
						Name:       name,
						Username:   username,
						Source:     source,
					},
				}
				if roleType == "closed_by_data" {
					closedBy = &contributor
				}
				issueContributors = append(issueContributors, contributor)
				possiblyAddOwnerContributor(role, contributor)
				if roleType != "assignee_data" {
					continue
				}
				assigneeSID := sIID + ":" + username
				issueAssigneeID, err = igh.GenerateGithubAssigneeID(repoID, issueID, assigneeSID)
				if err != nil {
					shared.Printf("GenerateGithubAssigneeID(%s,%s,%s): %+v for %+v\n", repoID, issueID, assigneeSID, err, doc)
					return
				}
				issueAssignee := igh.IssueAssignee{
					ID:      issueAssigneeID,
					IssueID: issueID,
					Assignee: insights.Assignee{
						AssigneeID:      username,
						Contributor:     contributor,
						SyncTimestamp:   time.Now(),
						SourceTimestamp: createdOn,
					},
				}
				key := "assignee_added"
				ary, ok := data[key]
				if !ok {
					ary = []interface{}{issueAssignee}
				} else {
					ary = append(ary, issueAssignee)
				}
				data[key] = ary
			}
		}
		// Primary assignee end
		// Other assignees start
		assigneesAry, okAssignees := doc["assignees_array"].([]interface{})
		if okAssignees {
			for _, iAssignee := range assigneesAry {
				assignee, okAssignee := iAssignee.(map[string]interface{})
				if !okAssignee || assignee == nil {
					continue
				}
				roles, okRoles := assignee["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "assignee" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					if username == primaryAssignee {
						continue
					}
					email, _ := role["email"].(string)
					avatarURL, _ := role["avatar_url"].(string)
					// No identity data postprocessing in V2
					// name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.AssigneeRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					issueContributors = append(issueContributors, contributor)
					possiblyAddOwnerContributor(role, contributor)
					assigneeSID := sIID + ":" + username
					issueAssigneeID, err = igh.GenerateGithubAssigneeID(repoID, issueID, assigneeSID)
					if err != nil {
						shared.Printf("GenerateGithubAssigneeID(%s,%s, %s): %+v for %+v\n", repoID, issueID, assigneeSID, err, doc)
						return
					}
					issueAssignee := igh.IssueAssignee{
						ID:      issueAssigneeID,
						IssueID: issueID,
						Assignee: insights.Assignee{
							AssigneeID:      username,
							Contributor:     contributor,
							SyncTimestamp:   time.Now(),
							SourceTimestamp: createdOn,
						},
					}
					key := "assignee_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{issueAssignee}
					} else {
						ary = append(ary, issueAssignee)
					}
					data[key] = ary
				}
			}
		}
		// Other assignees end
		// Issue reactions start
		reactionsAry, okReactions := doc["reactions_array"].([]interface{})
		if okReactions {
			for _, iReaction := range reactionsAry {
				reaction, okReaction := iReaction.(map[string]interface{})
				if !okReaction || reaction == nil {
					continue
				}
				roles, okRoles := reaction["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				content, _ := reaction["content"].(string)
				emojiContent := j.emojiForContent(content)
				reactionCreatedOn, _ := reaction["metadata__updated_on"].(time.Time)
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "user_data" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					email, _ := role["email"].(string)
					avatarURL, _ := role["avatar_url"].(string)
					// No identity data postprocessing in V2
					// name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.ReactionAuthorRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					issueContributors = append(issueContributors, contributor)
					possiblyAddOwnerContributor(role, contributor)
					reactionSID := sIID + ":" + content
					issueReactionID, err = igh.GenerateGithubReactionID(repoID, reactionSID)
					if err != nil {
						shared.Printf("GenerateGithubReactionID(%s,%s): %+v for %+v\n", repoID, reactionSID, err, doc)
						return
					}
					issueReaction := igh.IssueReaction{
						ID:      issueReactionID,
						IssueID: issueID,
						Reaction: insights.Reaction{
							Emoji: service.Emoji{
								ID:      content,
								Unicode: emojiContent,
							},
							ReactionID:      reactionSID,
							SourceTimestamp: reactionCreatedOn,
							SyncTimestamp:   time.Now(),
							Contributor:     contributor,
						},
					}
					key := "reaction_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{issueReaction}
					} else {
						ary = append(ary, issueReaction)
					}
					data[key] = ary
					nReactions++
				}
			}
		}
		// Issue reactions end
		// Comments start
		commentsAry, okComments := doc["comments_array"].([]interface{})
		if okComments {
			for _, iComment := range commentsAry {
				comment, okComment := iComment.(map[string]interface{})
				if !okComment || comment == nil {
					continue
				}
				roles, okRoles := comment["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				sCommentBody, _ := comment["body"].(string)
				sCommentURL, _ := comment["html_url"].(string)
				commentCreatedOn, _ := comment["metadata__updated_on"].(time.Time)
				commentID, _ := comment["issue_comment_id"].(int64)
				sCommentID := fmt.Sprintf("%d", commentID)
				if commentCreatedOn.After(updatedOn) {
					updatedOn = commentCreatedOn
				}
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "user_data" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					email, _ := role["email"].(string)
					avatarURL, _ := role["avatar_url"].(string)
					// No identity data postprocessing in V2
					// name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.CommenterRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					issueContributors = append(issueContributors, contributor)
					possiblyAddOwnerContributor(role, contributor)
					commentSID := sCommentID
					issueCommentID, err = igh.GenerateGithubCommentID(repoID, commentSID)
					if err != nil {
						shared.Printf("GenerateGithubCommentID(%s,%s): %+v for %+v\n", repoID, commentSID, err, doc)
						return
					}
					issueComment := igh.IssueComment{
						ID:      issueCommentID,
						IssueID: issueID,
						Comment: insights.Comment{
							Body:            sCommentBody,
							CommentURL:      sCommentURL,
							SourceTimestamp: commentCreatedOn,
							SyncTimestamp:   time.Now(),
							CommentID:       commentSID,
							Contributor:     contributor,
							Orphaned:        false,
						},
					}
					key := "comment_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{issueComment}
					} else {
						ary = append(ary, issueComment)
					}
					data[key] = ary
					nComments++
				}
			}
		}
		// Comments end
		// Comment reactions start
		reactionsAry, okReactions = doc["comments_reactions_array"].([]interface{})
		if okReactions {
			for _, iReaction := range reactionsAry {
				reaction, okReaction := iReaction.(map[string]interface{})
				if !okReaction || reaction == nil {
					continue
				}
				commentID, _ := reaction["issue_comment_id"].(int64)
				sCommentID := fmt.Sprintf("%d", commentID)
				reactionCreatedOn, _ := reaction["metadata__updated_on"].(time.Time)
				roles, okRoles := reaction["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				content, _ := reaction["content"].(string)
				emojiContent := j.emojiForContent(content)
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "user_data" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					email, _ := role["email"].(string)
					avatarURL, _ := role["avatar_url"].(string)
					// No identity data postprocessing in V2
					// name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.ReactionAuthorRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					issueContributors = append(issueContributors, contributor)
					possiblyAddOwnerContributor(role, contributor)
					reactionSID := sCommentID + ":" + content
					issueCommentReactionID, err = igh.GenerateGithubReactionID(repoID, reactionSID)
					if err != nil {
						shared.Printf("GenerateGithubReactionID(%s,%s): %+v for %+v\n", repoID, reactionSID, err, doc)
						return
					}
					issueCommentReaction := igh.IssueCommentReaction{
						ID:        issueCommentReactionID,
						CommentID: sCommentID,
						Reaction: insights.Reaction{
							Emoji: service.Emoji{
								ID:      content,
								Unicode: emojiContent,
							},
							ReactionID:      reactionSID,
							SourceTimestamp: reactionCreatedOn,
							SyncTimestamp:   time.Now(),
							Contributor:     contributor,
						},
					}
					key := "comment_reaction_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{issueCommentReaction}
					} else {
						ary = append(ary, issueCommentReaction)
					}
					data[key] = ary
					nReactions++
				}
			}
		}
		// Comment reactions end
		// Final Issue object
		issue := igh.Issue{
			ID:            issueID,
			RepositoryID:  repoID,
			RepositoryURL: j.URL,
			Repository:    repoShortName,
			Organization:  org,
			IsPullRequest: isPullRequest,
			Labels:        labels,
			ClosedBy:      closedBy,
			Contributors:  shared.DedupContributors(issueContributors),
			Issue: insights.Issue{
				Title:           title,
				Body:            body,
				IssueID:         sIID,
				IssueURL:        url,
				State:           insights.IssueState(state),
				SyncTimestamp:   time.Now(),
				SourceTimestamp: createdOn,
				Orphaned:        false,
			},
		}
		isNew := false
		if !updatedOn.After(createdOn) || (nComments == 0 && nReactions == 0) {
			isNew = true
		}
		key := "updated"
		if isNew {
			key = "created"
		}
		ary, ok := data[key]
		if !ok {
			ary = []interface{}{issue}
		} else {
			ary = append(ary, issue)
		}
		data[key] = ary
		// Fake close "event"
		if isClosed {
			// issue.Contributors = []insights.Contributor{}
			issue.SyncTimestamp = time.Now()
			issue.SourceTimestamp = *closedOn
			key := "closed"
			ary, ok := data[key]
			if !ok {
				ary = []interface{}{issue}
			} else {
				ary = append(ary, issue)
			}
			data[key] = ary
		}
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
	timestamp := time.Now()
	shared.SetSyncMode(true, false)
	shared.SetLogLoggerError(false)
	shared.AddLogger(&github.Logger, GitHubDataSource, logger.Internal, []map[string]string{{"GITHUB_ORG": github.Org, "GITHUB_REPO": github.Repo, "REPO_URL": github.URL, "ProjectSlug": ctx.Project}})
	for cat := range ctx.Categories {
		github.WriteLog(&ctx, timestamp, logger.InProgress, cat)
		err = github.Sync(&ctx, cat)
		if err != nil {
			shared.Printf("Error: %+v\n", err)
			github.WriteLog(&ctx, timestamp, logger.Failed, cat+": "+err.Error())
			return
		}
		github.WriteLog(&ctx, timestamp, logger.Done, cat)
	}
}
