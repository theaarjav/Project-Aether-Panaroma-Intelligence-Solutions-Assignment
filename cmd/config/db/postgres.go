package db

import (
	"database/sql"
	"log"
	"sync"

	_ "github.com/lib/pq" // Postgres driver
	"github.com/pressly/goose/v3"
)

var (
	postgresDB   *sql.DB
	postgresOnce sync.Once
)

func GetPostgresDB(dbUrl, migrationDir string) *sql.DB {
	postgresOnce.Do(func() {
		postgresDB = InitSQLAndMigrate(dbUrl, migrationDir)
	})
	return postgresDB
}

// InitSQL initializes Postgres and runs goose migrations
func InitSQLAndMigrate(dbUrl, migrationDir string) *sql.DB {
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping DB: %v", err)
	}

	log.Println("Postgres connected successfully!")

	// Run migrations
	if err := goose.Up(db, migrationDir); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	log.Println("Migrations applied successfully!")
	return db
}
