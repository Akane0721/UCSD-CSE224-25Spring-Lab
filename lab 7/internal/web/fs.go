// Lab 7: Implement a local filesystem video content service

package web

import (
	"fmt"
	"os"
	"path/filepath"
)

// FSVideoContentService implements VideoContentService using the local filesystem.
type FSVideoContentService struct {
	baseDir string
}

// Uncomment the following line to ensure FSVideoContentService implements VideoContentService
var _ VideoContentService = (*FSVideoContentService)(nil)

func NewFSVideoContentService(baseDir string) (*FSVideoContentService, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base dir: %v", err)
	}
	return &FSVideoContentService{baseDir: baseDir}, nil
}

func (s *FSVideoContentService) Write(videoId string, filename string, data []byte) error {
	videoDir := filepath.Join(s.baseDir, videoId)
	if err := os.MkdirAll(videoDir, 0755); err != nil {
		return fmt.Errorf("failed to create content directory: %w", err)
	}

	filePath := filepath.Join(videoDir, filename)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write content file %s: %w", filename, err)
	}
	return nil
}

func (s *FSVideoContentService) Read(videoId string, filename string) ([]byte, error) {
	videoDir := filepath.Join(s.baseDir, videoId)
	filePath := filepath.Join(videoDir, filename)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read content file %v: %v", filename, err)
	}
	return data, nil
}
