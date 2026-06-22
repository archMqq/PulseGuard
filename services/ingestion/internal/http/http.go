package http

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"pulseguard/services/ingestion/internal/queue"
	"pulseguard/services/ingestion/internal/service"
	"pulseguard/services/pkg/contracts"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type HttpHandler struct {
	injectionService service.InjectionService
	queue            queue.QueueSaver
	router           *gin.Engine
	wg               *sync.WaitGroup
}

func NewHttpHandler(injectService service.InjectionService, queue queue.QueueSaver, wg *sync.WaitGroup) *HttpHandler {
	newRouter := gin.New()
	handler := &HttpHandler{
		injectionService: injectService,
		queue:            queue,
		router:           newRouter,
		wg:               wg,
	}

	newRouter.POST("/error", handler.postError)

	return handler
}

func (hh *HttpHandler) postError(c *gin.Context) {
	pk := c.Request.Header.Get("X-Project-Key")
	if pk == "" {
		c.IndentedJSON(http.StatusBadRequest, gin.H{
			"message": "no needed \"X-Project-Key\" header",
		})
		return
	}

	var event contracts.ErrorEvent
	if err := c.BindJSON(&event); err != nil {
		c.IndentedJSON(http.StatusBadRequest, gin.H{
			"message": "invalid request",
		})
		return
	}

	if err := hh.injectionService.ValidateErrorEvent(c.Request.Context(), &event, pk); err != nil {
		c.IndentedJSON(http.StatusUnauthorized, gin.H{
			"message": "project unauthorized",
		})
		return
	}

	eventJson, err := json.Marshal(event)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, gin.H{
			"message": "error event marshaling",
		})
		return
	}

	hh.wg.Add(1)
	go func(event string) {
		defer hh.wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := hh.queue.Save(ctx, event); err != nil {
			log.Printf("error sanding to kafka")
		}
	}(string(eventJson))

	c.IndentedJSON(http.StatusAccepted, gin.H{})
}

func (hh *HttpHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	hh.router.ServeHTTP(w, req)
}
