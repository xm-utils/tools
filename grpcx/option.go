package grpcx

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/stats"
)

// ClientOption is client create option
type ClientOption func(*clientOptions)

type clientOptions struct {
	prefix      string
	host        string
	grpcOptions []grpc.DialOption
}

func WidthHost(host string) ClientOption {
	return func(opts *clientOptions) {
		opts.host = host
	}
}

// WithDirectAddresses returns a direct addresses option
func WithDirectAddresses(addrs ...string) ClientOption {
	return func(opts *clientOptions) {
		if len(addrs) <= 0 {
			return
		}
		opts.prefix = addrs[0]
		//opts.resolver = resolver.Builder(addrs...)
	}
}

// WithTimeout returns a timeout option
func WithTimeout(timeout time.Duration) ClientOption {
	return func(opts *clientOptions) {
		opts.grpcOptions = append(opts.grpcOptions, grpc.WithTimeout(timeout))
	}
}

// WithResolver returns a resolver option
func WithResolver(resolver resolver.Builder) ClientOption {
	return func(opts *clientOptions) {
		opts.grpcOptions = append(opts.grpcOptions, grpc.WithResolvers(resolver))
	}
}

func WithStatsHandler(h stats.Handler) ClientOption {
	return func(options *clientOptions) {
		options.grpcOptions = append(options.grpcOptions, grpc.WithStatsHandler(h))
	}
}
