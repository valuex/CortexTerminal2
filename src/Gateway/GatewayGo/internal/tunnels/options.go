// Package tunnels is the Go port of CortexTerminal.Gateway.Tunnels —
// visitor HTTP ingress that forwards requests to a worker port via a
// short-lived tunnel record. Mirrors the C# state machine in
// TunnelMiddleware.cs + TunnelRegistry.cs.
package tunnels

import "time"

// Options mirrors CortexTerminal.Gateway.Tunnels.TunnelOptions. The
// YAML loader under config.TunnelsConfig populates these fields; the
// zero values are filled by WithDefaults.
type Options struct {
	RoutePrefix         string
	RootDomain          string // empty = path mode only
	DefaultTTL          time.Duration
	MaxTunnelsPerSession int
	Enabled             bool
	MaxQpsPerTunnel     int
	ProbeTimeout        time.Duration
	ForwardTimeout      time.Duration
}

// WithDefaults returns a copy with zero-value fields populated with the
// same defaults the C# options class uses.
func (o Options) WithDefaults() Options {
	if o.RoutePrefix == "" {
		o.RoutePrefix = "/t/"
	}
	if o.DefaultTTL == 0 {
		o.DefaultTTL = 24 * time.Hour
	}
	if o.MaxTunnelsPerSession == 0 {
		o.MaxTunnelsPerSession = 3
	}
	if o.MaxQpsPerTunnel == 0 {
		o.MaxQpsPerTunnel = 50
	}
	if o.ProbeTimeout == 0 {
		o.ProbeTimeout = 2 * time.Second
	}
	if o.ForwardTimeout == 0 {
		o.ForwardTimeout = 30 * time.Second
	}
	// Enabled defaults to true — matches C#.
	// (a bool zero-value is false, but the C# class defaults Enabled=true.)
	return o
}