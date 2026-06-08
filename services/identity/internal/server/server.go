package server

import(
	"pulseguard/services/identity/internal/service"
)

type Server struct {
	indent *service.IdentityService
}

func New() *Server {
	return &Server{}
}
