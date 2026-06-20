package cache

import "context"


type ProjectsCache interface {
	CheckKey(context.Context, string) (int, error)
}