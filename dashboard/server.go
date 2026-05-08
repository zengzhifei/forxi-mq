package dashboard

import (
	"context"
	"embed"
	"io/fs"
	"net/http"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/log"
)

//go:embed ui/dist/*
var staticFiles embed.FS

// Server is the dashboard HTTP server.
type Server struct {
	rdb    *redis.Client
	group  string
	topics []string
	logger log.Logger
	addr   string
	srv    *http.Server
}

// New creates a new dashboard Server.
func New(rdb *redis.Client, group string, topics []string, addr string, logger log.Logger) *Server {
	return &Server{
		rdb:    rdb,
		group:  group,
		topics: topics,
		logger: logger,
		addr:   addr,
	}
}

// Start launches the HTTP server in a goroutine.
func (s *Server) Start(ctx context.Context) {
	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("GET /api/overview", s.handleOverview)
	mux.HandleFunc("GET /api/topics", s.handleTopics)
	mux.HandleFunc("GET /api/topics/{topic}", s.handleTopicDetail)
	mux.HandleFunc("GET /api/topics/{topic}/dead", s.handleDeadLetters)
	mux.HandleFunc("POST /api/topics/{topic}/dead/requeue", s.handleRequeue)
	mux.HandleFunc("GET /api/topics/{topic}/delay", s.handleDelayQueue)

	// Static files (Vue dist)
	dist, _ := fs.Sub(staticFiles, "ui/dist")
	fileServer := http.FileServer(http.FS(dist))
	mux.Handle("/", fileServer)

	s.srv = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	go func() {
		s.logger.Info("dashboard started", "addr", s.addr)
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("dashboard server error", "error", err)
		}
	}()

	go func() {
		<-ctx.Done()
		s.srv.Close()
	}()
}
