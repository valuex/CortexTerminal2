package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Auth        AuthConfig        `yaml:"auth"`
	PhoneAuth   PhoneAuthConfig   `yaml:"phone_auth"`
	AppleOAuth  AppleOAuthConfig  `yaml:"apple_oauth"`
	HuaweiOAuth HuaweiOAuthConfig `yaml:"huawei_oauth"`
	Tts         TtsConfig         `yaml:"tts"`
	Scrollback  ScrollbackConfig  `yaml:"scrollback"`
	Tunnels     TunnelsConfig     `yaml:"tunnels"`
	Storage     StorageConfig     `yaml:"storage"`
	Support     SupportConfig     `yaml:"support"`
	Captcha     CaptchaConfig     `yaml:"captcha"`
	GitHub      GitHubConfig      `yaml:"github"`
	Database    DatabaseConfig    `yaml:"database"`
}

type ServerConfig struct {
	Listen           string `yaml:"listen"`
	ForwardedHeaders bool   `yaml:"forwarded_headers"`
}

type AuthConfig struct {
	SigningKey               string `yaml:"signing_key"`
	WorkerTokenLifetimeMins  int    `yaml:"worker_token_lifetime_minutes"`
	VerificationURI          string `yaml:"verification_uri"`
	GitHub                   OAuthProviderConfig `yaml:"github"`
	Google                   OAuthProviderConfig `yaml:"google"`
}

type OAuthProviderConfig struct {
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
}

type PhoneAuthConfig struct {
	AccessKeyID     string `yaml:"access_key_id"`
	AccessKeySecret string `yaml:"access_key_secret"`
	SignName        string `yaml:"sign_name"`
	TemplateCode    string `yaml:"template_code"`
	RegionID        string `yaml:"region_id"`
}

type AppleOAuthConfig struct {
	ClientID   string `yaml:"client_id"`
	TeamID     string `yaml:"team_id"`
	KeyID      string `yaml:"key_id"`
	PrivateKey string `yaml:"private_key"`
}

type HuaweiOAuthConfig struct {
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
}

type TtsConfig struct {
	Cloudflare CloudflareTtsConfig `yaml:"cloudflare"`
	Aliyun     AliyunTtsConfig     `yaml:"aliyun"`
}

type CloudflareTtsConfig struct {
	AccountID string `yaml:"account_id"`
	APIToken  string `yaml:"api_token"`
}

type AliyunTtsConfig struct {
	APIKey string `yaml:"api_key"`
	Model  string `yaml:"model"`
	Voice  string `yaml:"voice"`
}

type ScrollbackConfig struct {
	MaxMegabytes    int `yaml:"max_megabytes"`
	MaxBytesOverride int `yaml:"max_bytes_override"`
}

type TunnelsConfig struct {
	RoutePrefix         string        `yaml:"route_prefix"`
	RootDomain          string        `yaml:"root_domain"`
	DefaultTTL          time.Duration `yaml:"default_ttl"`
	MaxTunnelsPerSession int          `yaml:"max_tunnels_per_session"`
	ProbeTimeout        time.Duration `yaml:"probe_timeout"`
	ForwardTimeout      time.Duration `yaml:"forward_timeout"`
	Enabled             bool          `yaml:"enabled"`
	MaxQpsPerTunnel     int           `yaml:"max_qps_per_tunnel"`
}

type StorageConfig struct {
	Endpoint               string        `yaml:"endpoint"`
	Bucket                 string        `yaml:"bucket"`
	Region                 string        `yaml:"region"`
	AccessKey              string        `yaml:"access_key"`
	SecretKey              string        `yaml:"secret_key"`
	ForcePathStyle         bool          `yaml:"force_path_style"`
	PresignedURLTTL        time.Duration `yaml:"presigned_url_ttl"`
	MaxArtifactSizeBytes   int64         `yaml:"max_artifact_size_bytes"`
	MaxArtifactAgeDays     int           `yaml:"max_artifact_age_days"`
	GracePeriodHours       int           `yaml:"grace_period_hours"`
	MaxArtifactsPerSession int           `yaml:"max_artifacts_per_session"`
}

type SupportConfig struct {
	QQGroup       SupportGroupConfig `yaml:"qq_group"`
	TelegramGroup SupportGroupConfig `yaml:"telegram_group"`
	Email         string             `yaml:"email"`
}

type SupportGroupConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Name     string `yaml:"name"`
	Number   string `yaml:"number"`
	URL      string `yaml:"url"`
	QRCodeURL string `yaml:"qr_code_url"`
}

type CaptchaConfig struct {
	FailedThreshold int `yaml:"failed_threshold"`
	WindowMinutes   int `yaml:"window_minutes"`
	TolerancePixels int `yaml:"tolerance_pixels"`
}

type GitHubConfig struct {
	Repo  string `yaml:"repo"`
	Proxy string `yaml:"proxy"`
}

type DatabaseConfig struct {
	SQLitePath string `yaml:"sqlite_path"`
}

// Load reads YAML from path, then applies post-bind env overrides that the
// C# Gateway does via PostConfigure<T>() and Environment.GetEnvironmentVariable.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyEnvOverrides()
	c.setDefaults()
	return &c, nil
}

func (c *Config) applyEnvOverrides() {
	// C# Program.cs line 285-289: CORTERM_SCROLLBACK_BYTES overrides scrollback max bytes.
	if v := os.Getenv("CORTERM_SCROLLBACK_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Scrollback.MaxBytesOverride = n
		}
	}
	// C# Program.cs line 295-299: TUNNELS_ENABLED overrides tunnels.enabled.
	if v := os.Getenv("TUNNELS_ENABLED"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			c.Tunnels.Enabled = b
		}
	}
	// C# Program.cs line 311-313: GATEWAY_SQLITE_CONNECTION_STRING wins.
	if v := os.Getenv("GATEWAY_SQLITE_CONNECTION_STRING"); v != "" {
		c.Database.SQLitePath = v
	}
}

func (c *Config) setDefaults() {
	if c.Server.Listen == "" {
		c.Server.Listen = ":5045"
	}
	if c.Auth.SigningKey == "" {
		c.Auth.SigningKey = "gateway-auth-signing-key-minimum-32b"
	}
	if c.Auth.WorkerTokenLifetimeMins == 0 {
		c.Auth.WorkerTokenLifetimeMins = 30 * 24 * 60
	}
	if c.Auth.VerificationURI == "" {
		c.Auth.VerificationURI = "https://corterm.rwecho.top/activate"
	}
	if c.Tunnels.RoutePrefix == "" {
		c.Tunnels.RoutePrefix = "/t/"
	}
	if c.GitHub.Repo == "" {
		c.GitHub.Repo = "monster-echo/CortexTerminal2"
	}
	if c.GitHub.Proxy == "" {
		c.GitHub.Proxy = "https://proxy.0x2a.top"
	}
	if c.Storage.PresignedURLTTL == 0 {
		c.Storage.PresignedURLTTL = 15 * time.Minute
	}
	if c.Tunnels.DefaultTTL == 0 {
		c.Tunnels.DefaultTTL = 24 * time.Hour
	}
	if c.Tunnels.ProbeTimeout == 0 {
		c.Tunnels.ProbeTimeout = 2 * time.Second
	}
	if c.Tunnels.ForwardTimeout == 0 {
		c.Tunnels.ForwardTimeout = 30 * time.Second
	}
	if c.Tunnels.MaxTunnelsPerSession == 0 {
		c.Tunnels.MaxTunnelsPerSession = 3
	}
	if c.Tunnels.MaxQpsPerTunnel == 0 {
		c.Tunnels.MaxQpsPerTunnel = 50
	}
	if c.Scrollback.MaxMegabytes == 0 {
		c.Scrollback.MaxMegabytes = 5
	}
	if c.Scrollback.MaxBytesOverride == 0 {
		c.Scrollback.MaxBytesOverride = 524288
	}
	if c.Captcha.FailedThreshold == 0 {
		c.Captcha.FailedThreshold = 3
	}
	if c.Captcha.WindowMinutes == 0 {
		c.Captcha.WindowMinutes = 15
	}
	if c.Captcha.TolerancePixels == 0 {
		c.Captcha.TolerancePixels = 5
	}
	if c.Database.SQLitePath == "" {
		c.Database.SQLitePath = "corterm_gateway.db"
	}
}
