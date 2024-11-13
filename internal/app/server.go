package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	log "log/slog"
	"net/http"
	"regexp"

	"github.com/dariusigna/object-storage/internal/gateway"
	"github.com/gorilla/mux"
)

// Storage is an interface for the object storage
type Storage interface {
	GetObject(ctx context.Context, bucket, id string) ([]byte, error)
	PutObject(ctx context.Context, bucket, id string, object []byte) error
}

// NewServer creates a new HTTP server for the object storage gateway
func NewServer(
	storage Storage,
) http.Handler {
	r := mux.NewRouter()
	addRoutes(
		r,
		storage,
	)
	var handler http.Handler = r
	return handler
}

func addRoutes(
	mux *mux.Router,
	storage Storage,
) {
	mux.Handle("/{bucket}/{id}", handleGetObject(storage)).Methods(http.MethodGet)
	mux.Handle("/{bucket}/{id}", handlePutObject(storage)).Methods(http.MethodPut)
}

func handleGetObject(storage Storage) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			id := mux.Vars(r)["id"]
			if err := validateID(id); err != nil {
				log.Error("validation error", err)
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}

			bucket := mux.Vars(r)["bucket"]
			object, err := storage.GetObject(r.Context(), bucket, id)
			if err != nil {
				log.Error("get error", err)
				if errors.Is(err, gateway.NotFoundError{}) {
					w.WriteHeader(http.StatusNotFound)
					return
				}

				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write(object)
		},
	)
}

func handlePutObject(storage Storage) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			id := mux.Vars(r)["id"]
			if err := validateID(id); err != nil {
				log.Error("validation error", err)
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}

			bucket := mux.Vars(r)["bucket"]
			log.Debug("put object", "bucket", bucket, "id", id)
			object, err := io.ReadAll(r.Body)
			if err != nil {
				log.Error("read error", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			err = storage.PutObject(r.Context(), bucket, id, object)
			if err != nil {
				log.Error("put error", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
		},
	)
}

func validateID(id string) error {
	if len(id) > 32 {
		return fmt.Errorf("id is too long")
	}

	isAlphanumeric := regexp.MustCompile(`^[a-zA-Z0-9]+$`).MatchString
	if !isAlphanumeric(id) {
		return fmt.Errorf("id contains invalid characters")
	}

	return nil
}
