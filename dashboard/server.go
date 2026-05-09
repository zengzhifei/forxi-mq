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
	logger log.Logger
	addr   string
	srv    *http.Server
}

// New creates a new dashboard Server.
func New(rdb *redis.Client, addr string, logger log.Logger) *Server {
	return &Server{
		rdb:    rdb,
		logger: logger,
		addr:   addr,
	}
}

// Start launches the HTTP server in a goroutine.
// The returned channel is closed when the server has fully stopped.
func (s *Server) Start(ctx context.Context) <-chan struct{} {
	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("GET /api/overview", s.handleOverview)
	mux.HandleFunc("GET /api/topics", s.handleTopics)
	mux.HandleFunc("GET /api/topics/{topic}", s.handleTopicDetail)
	mux.HandleFunc("GET /api/topics/{topic}/messages", s.handleMessages)
	mux.HandleFunc("GET /api/topics/{topic}/lag", s.handleLag)
	mux.HandleFunc("GET /api/topics/{topic}/pending", s.handlePending)
	mux.HandleFunc("GET /api/topics/{topic}/dead", s.handleDeadLetters)
	mux.HandleFunc("POST /api/topics/{topic}/dead/requeue", s.handleRequeue)
	mux.HandleFunc("POST /api/topics/{topic}/resend", s.handleResend)
	mux.HandleFunc("GET /api/topics/{topic}/delay", s.handleDelayQueue)
	mux.HandleFunc("GET /api/topics/{topic}/search", s.handleSearchMessage)
	mux.HandleFunc("POST /api/topics/{topic}/publish", s.handlePublish)
	mux.HandleFunc("DELETE /api/topics/{topic}/dead/{id}", s.handleDeleteDead)
	mux.HandleFunc("POST /api/topics/{topic}/delay/delete", s.handleDeleteDelay)
	mux.HandleFunc("POST /api/topics/{topic}/groups/{group}/reset", s.handleResetGroup)

	// Static files (Vue dist)
	dist, _ := fs.Sub(staticFiles, "ui/dist")
	fileServer := http.FileServer(http.FS(dist))
	mux.Handle("/", fileServer)

	s.srv = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	done := make(chan struct{})
	go func() {
		s.logger.Info("dashboard started", "addr", s.addr)
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("dashboard server error", "error", err)
		}
	}()

	go func() {
		<-ctx.Done()
		s.srv.Close()
		close(done)
	}()

	return done
}
