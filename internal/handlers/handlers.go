package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"sentinel/internal/db"
	"sentinel/internal/models"
)

type Server struct {
	Mongo db.Database
	Redis db.QueueCache
}

func NewServer(mongoClient db.Database, redisClient db.QueueCache) *Server {
	return &Server{
		Mongo: mongoClient,
		Redis: redisClient,
	}
}

// ServeHTTP implements custom routing using standard Go net/http
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Enable CORS for frontend/curl integration
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Routing logic
	path := r.URL.Path
	if path == "/" || path == "/index.html" {
		if r.Method == http.MethodGet {
			// Set HTML header since http.ServeFile will serve the file
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			http.ServeFile(w, r, "web/index.html")
		} else {
			s.respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if path == "/urls" || path == "/urls/" {
		switch r.Method {
		case http.MethodPost:
			s.handleCreateURL(w, r)
		case http.MethodGet:
			s.handleListURLs(w, r)
		default:
			s.respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}

	if strings.HasPrefix(path, "/urls/") {
		parts := strings.Split(strings.TrimPrefix(path, "/urls/"), "/")
		if len(parts) == 1 && parts[0] != "" {
			// e.g. /urls/:id
			id := parts[0]
			switch r.Method {
			case http.MethodGet:
				s.handleGetURL(w, r, id)
			case http.MethodDelete:
				s.handleDeleteURL(w, r, id)
			default:
				s.respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
			}
			return
		} else if len(parts) == 2 && parts[1] == "history" {
			// e.g. /urls/:id/history
			id := parts[0]
			if r.Method == http.MethodGet {
				s.handleGetURLHistory(w, r, id)
			} else {
				s.respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
			}
			return
		}
	}

	s.respondWithError(w, http.StatusNotFound, "Not found")
}

type CreateURLRequest struct {
	URL             string `json:"url"`
	IntervalSeconds int    `json:"interval_seconds"`
}

func (s *Server) handleCreateURL(w http.ResponseWriter, r *http.Request) {
	var req CreateURLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	// Validation
	if req.URL == "" {
		s.respondWithError(w, http.StatusBadRequest, "URL is required")
		return
	}
	parsedURL, err := url.ParseRequestURI(req.URL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		s.respondWithError(w, http.StatusBadRequest, "Invalid URL format (must include http or https)")
		return
	}
	if req.IntervalSeconds < 5 {
		s.respondWithError(w, http.StatusBadRequest, "Interval must be at least 5 seconds")
		return
	}

	monitor := models.Monitor{
		ID:              primitive.NewObjectID(),
		URL:             req.URL,
		IntervalSeconds: req.IntervalSeconds,
		CreatedAt:       time.Now(),
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	err = s.Mongo.CreateMonitor(ctx, monitor)
	if err != nil {
		log.Printf("Error creating monitor: %v", err)
		s.respondWithError(w, http.StatusInternalServerError, "Failed to save monitor")
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(monitor)
}

func (s *Server) handleListURLs(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	monitors, err := s.Mongo.GetMonitors(ctx)
	if err != nil {
		log.Printf("Error fetching monitors: %v", err)
		s.respondWithError(w, http.StatusInternalServerError, "Failed to fetch monitors")
		return
	}

	details := make([]models.MonitorDetail, 0, len(monitors))
	for _, m := range monitors {
		detail := models.MonitorDetail{
			Monitor: m,
		}

		// Try Redis cache first
		check, err := s.Redis.GetLatestStatus(ctx, m.ID.Hex())
		if err != nil {
			log.Printf("Redis cache error: %v", err)
		}

		if check == nil {
			// Cache miss, fallback to MongoDB
			lastCheck, err := s.Mongo.GetLatestCheck(ctx, m.ID.Hex())
			if err == nil && lastCheck != nil {
				detail.LatestCheck = lastCheck
				// Write back to cache
				_ = s.Redis.CacheLatestStatus(ctx, lastCheck)
			}
		} else {
			detail.LatestCheck = check
		}

		details = append(details, detail)
	}

	json.NewEncoder(w).Encode(details)
}

func (s *Server) handleGetURL(w http.ResponseWriter, r *http.Request, idStr string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	m, err := s.Mongo.GetMonitor(ctx, idStr)
	if err != nil {
		s.respondWithError(w, http.StatusNotFound, "Monitor not found")
		return
	}

	detail := models.MonitorDetail{
		Monitor: *m,
	}

	// Try Redis cache first
	check, err := s.Redis.GetLatestStatus(ctx, m.ID.Hex())
	if err != nil {
		log.Printf("Redis cache error: %v", err)
	}

	if check == nil {
		// Cache miss, fallback to MongoDB
		lastCheck, err := s.Mongo.GetLatestCheck(ctx, m.ID.Hex())
		if err == nil && lastCheck != nil {
			detail.LatestCheck = lastCheck
			// Write back to cache
			_ = s.Redis.CacheLatestStatus(ctx, lastCheck)
		}
	} else {
		detail.LatestCheck = check
	}

	json.NewEncoder(w).Encode(detail)
}

func (s *Server) handleGetURLHistory(w http.ResponseWriter, r *http.Request, idStr string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Verify monitor exists
	_, err := s.Mongo.GetMonitor(ctx, idStr)
	if err != nil {
		s.respondWithError(w, http.StatusNotFound, "Monitor not found")
		return
	}

	events, err := s.Mongo.GetStatusEvents(ctx, idStr)
	if err != nil {
		log.Printf("Error fetching status events: %v", err)
		s.respondWithError(w, http.StatusInternalServerError, "Failed to fetch history")
		return
	}

	json.NewEncoder(w).Encode(events)
}

func (s *Server) handleDeleteURL(w http.ResponseWriter, r *http.Request, idStr string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Delete from monitors
	err := s.Mongo.DeleteMonitor(ctx, idStr)
	if err != nil {
		s.respondWithError(w, http.StatusNotFound, "Monitor not found")
		return
	}

	// Clear cache and lock
	err = s.Redis.DeleteStatusCache(ctx, idStr)
	if err != nil {
		log.Printf("Error clearing cache: %v", err)
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) respondWithError(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
