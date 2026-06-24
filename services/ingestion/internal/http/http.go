package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"pulseguard/services/ingestion/internal/cache"
	"pulseguard/services/ingestion/internal/queue"
	"pulseguard/services/ingestion/internal/service"
	"pulseguard/services/pkg/contracts"

	"github.com/gin-gonic/gin"
)

type HttpHandler struct {
	injectionService service.InjectionService
	queue            queue.QueueSaver
	router           *gin.Engine
}

func NewHttpHandler(injectService service.InjectionService, queue queue.QueueSaver) *HttpHandler {
	newRouter := gin.New()
	handler := &HttpHandler{
		injectionService: injectService,
		queue:            queue,
		router:           newRouter,
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

		if errors.Is(err, cache.ErrUndefinedKey) {
			c.IndentedJSON(http.StatusUnauthorized, gin.H{
				"message": "project unauthorized",
			})
			return
		} else if errors.Is(err, cache.ErrRedis) {
			c.IndentedJSON(http.StatusInternalServerError, gin.H{
				"message": "error project key checking",
			})
			return
		}
	}

	eventJson, err := json.Marshal(event)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, gin.H{
			"message": "error event marshaling",
		})
		return
	}

	if err := hh.queue.Save(c.Request.Context(), eventJson); err != nil {
		c.IndentedJSON(http.StatusInternalServerError, gin.H{
			"message": "error event saving",
		})
		return
	}

	c.IndentedJSON(http.StatusAccepted, gin.H{})
}

func (hh *HttpHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	hh.router.ServeHTTP(w, req)
}
