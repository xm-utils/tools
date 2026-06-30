package grpcx

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

type IServer interface {
	Start() error
	stop()
	Serve(l net.Listener) error
	getAddr() string
	typeCode() string
	Match() cmux.Matcher
}

// ServiceRegister registry grpc services
type ServiceRegister func(*grpc.Server)

// Server is a grpc server
type Server struct {
	ImplementShutdown
	servers []IServer
	log     *logrus.Entry
}

func NewServer(servers []IServer, log *logrus.Logger) *Server {
	s := &Server{
		servers: servers,
		log:     log.WithField("model", "GatewayGrpcServer"),
	}
	return s
}

func (s *Server) Start() error {
	defer func() {
		if err := recover(); err != nil {
			s.log.Errorf("rpc: grpc server crash, errors:\n %+v", err)
		}
	}()

	for _, ser := range s.servers {
		go func() {
			if err := ser.Start(); err != nil {
				s.log.Errorf("rpc: start a %s server failed, errors:\n%+v", ser.typeCode(), err)
			}
		}()

	}

	return nil
}

func (s *Server) StartWithListener(l net.Listener) error {
	defer func() {
		if err := recover(); err != nil {
			s.log.Errorf("rpc: grpc server crash, errors:\n %+v", err)
		}
	}()

	m := cmux.New(l)

	for _, server := range s.servers {
		s.log.Infof("rpc: start a %s server at %s \n", server.typeCode(), server.getAddr())
		ser := server
		go func() {
			sl := m.Match(ser.Match())
			err := ser.Serve(sl)
			if err != nil {
				s.log.Fatalf("rpc: start a %s server failed, errors:\n%+v", ser.typeCode(), err)
			}
		}()

	}

	return m.Serve()
}

func (s *Server) BeforeShutdown() {
	s.Stop()
}

// Stop stop servers
func (s *Server) Stop() {
	for _, server := range s.servers {
		s.log.Infof("rpc: stop a %s server at %s \n", server.typeCode(), server.getAddr())
		server.stop()
	}
}

func adjustAddr(addr string) string {
	if addr[0] == ':' {
		ips, err := intranetIP()
		if err != nil {
			logrus.Fatalf("get intranet ip failed, errors:\n%+v", err)
		}

		return fmt.Sprintf("%s%s", ips[0], addr)
	}

	return addr
}

type HTTPServer struct {
	ImplementShutdown
	addr   string
	server *http.Server
}

func NewHTTPServer(name, addr string, priority int, httpSetup func(engine *gin.Engine)) *HTTPServer {
	handler := gin.Default()
	httpSetup(handler)
	srv := &http.Server{
		Addr:    addr,
		Handler: handler,
	}
	s := &HTTPServer{
		ImplementShutdown: ImplementShutdown{
			Priority:  priority,
			EventName: name,
		},
		addr:   addr,
		server: srv,
	}
	return s
}

func WithHTTPServer(name, addr string, priority int, httpSetup func(engine *gin.Engine)) IServer {
	return NewHTTPServer(name, addr, priority, httpSetup)
}

func (s *HTTPServer) Start() error {
	logrus.Infof("rpc: start a %s server at %s \n", s.typeCode(), s.addr)
	return s.server.ListenAndServe()
}
func (s *HTTPServer) stop() {
	if err := s.server.Shutdown(context.Background()); nil != err && http.ErrServerClosed != err {
		log.Printf("server shutdown: %s\n", err)
	}
	log.Println("server exiting")
}
func (s *HTTPServer) typeCode() string {
	return "http"
}
func (s *HTTPServer) getAddr() string {
	return s.addr
}
func (s *HTTPServer) Serve(l net.Listener) error {
	return s.server.Serve(l)
}
func (s *HTTPServer) Match() cmux.Matcher {
	return cmux.Any()
}
func (s *HTTPServer) BeforeShutdown() {
	s.stop()
}

type GrpcServer struct {
	ImplementShutdown
	addr   string
	server *grpc.Server
}

func NewGrpcServer(name, addr string, priority int, register ServiceRegister, opts ...grpc.ServerOption) *GrpcServer {

	s := &GrpcServer{
		server: grpc.NewServer(opts...),
	}

	s.addr = addr
	s.ImplementShutdown = ImplementShutdown{
		Priority:  priority,
		EventName: name,
	}

	register(s.server)

	return s
}

func WithGrpcServer(name, addr string, priority int, register ServiceRegister, opts ...grpc.ServerOption) IServer {
	return NewGrpcServer(name, addr, priority, register, opts...)
}

func (s *GrpcServer) Start() error {
	logrus.Infof("rpc: start a %s server at %s \n", s.typeCode(), s.addr)
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	return s.server.Serve(lis)
}
func (s *GrpcServer) stop() {
	s.server.GracefulStop()
}
func (s *GrpcServer) Serve(l net.Listener) error {
	return s.server.Serve(l)
}

func (s *GrpcServer) typeCode() string {
	return "grpc"
}
func (s *GrpcServer) getAddr() string {
	return s.addr
}
func (s *GrpcServer) Match() cmux.Matcher {
	return cmux.HTTP2HeaderField("content-type", "application/grpc")
}
func (s *GrpcServer) BeforeShutdown() {
	s.stop()
}
