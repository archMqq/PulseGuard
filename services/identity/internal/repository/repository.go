package repository

import "database/sql"

type IdentityRepository struct {
	db *sql.DB
}

func New(db *sql.DB) *IdentityRepository {
	return &IdentityRepository{
		db: db,
	}

}