// Lab 7: Implement a SQLite video metadata service

package web

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteVideoMetadataService struct {
	db *sql.DB
}

// Uncomment the following line to ensure SQLiteVideoMetadataService implements VideoMetadataService
var _ VideoMetadataService = (*SQLiteVideoMetadataService)(nil)

func NewSQLiteVideoMetadataService(dsn string) (*SQLiteVideoMetadataService, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite failed: %v", err)
	}

	pragma := `PRAGMA busy_timeout = 5000`
	if _, err := db.Exec(pragma); err != nil {
		db.Close()
		return nil, fmt.Errorf("set busy timeout failed: %v", err)
	}

	createTable := `CREATE TABLE IF NOT EXISTS videos (
		id TEXT PRIMARY KEY,
		uploaded_at DATETIME NOT NULL
	);
	`
	if _, err := db.Exec(createTable); err != nil {
		db.Close()
		return nil, fmt.Errorf("Create table failed: %v", err)
	}
	return &SQLiteVideoMetadataService{db: db}, nil
}

func (s *SQLiteVideoMetadataService) Create(videoId string, uploadedAt time.Time) error {
	insert := `INSERT INTO videos (id, uploaded_at) VALUES (?, ?);`
	_, err := s.db.Exec(insert, videoId, uploadedAt.UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("insert metadate failed: %v", err)
	}
	return nil
}

func (s *SQLiteVideoMetadataService) List() ([]VideoMetadata, error) {
	slct := `SELECT id, uploaded_at FROM videos ORDER BY uploaded_at DESC;`
	rows, err := s.db.Query(slct)
	if err != nil {
		return nil, fmt.Errorf("query metadate failed: %v", err)
	}
	defer rows.Close()

	var metadataList []VideoMetadata
	for rows.Next() {
		var id string
		var uploadedAtStr string
		if err := rows.Scan(&id, &uploadedAtStr); err != nil {
			return nil, fmt.Errorf("scan metadata failed: %v", err)
		}
		ts, err := time.Parse(time.RFC3339, uploadedAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse uploaded time failed: %v", uploadedAtStr)
		}
		metadataList = append(metadataList, VideoMetadata{Id: id, UploadedAt: ts})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %v", err)
	}
	return metadataList, nil
}

func (s *SQLiteVideoMetadataService) Read(videoId string) (*VideoMetadata, error) {
	slct := `SELECT id, uploaded_at FROM videos WHERE id = ?;`
	row := s.db.QueryRow(slct, videoId)
	var id string
	var uploadedAtStr string
	if err := row.Scan(&id, &uploadedAtStr); err != nil {
		return nil, fmt.Errorf("scan metadata failed: %v", err)
	}
	ts, err := time.Parse(time.RFC3339, uploadedAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse uploaded time failed: %v", uploadedAtStr)
	}
	return &VideoMetadata{Id: id, UploadedAt: ts}, nil
}
