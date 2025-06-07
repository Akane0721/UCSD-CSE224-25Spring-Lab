// Lab 8: Implement a network video content service (server)

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"tritontube/internal/proto"
)

// Implement a network video content service (server)

type StorageServer struct {
	proto.UnimplementedVideoContentStorageServiceServer
	baseDir string
}

func NewStorageServer(baseDir string) *StorageServer {
	return &StorageServer{baseDir: baseDir}
}

func (s *StorageServer) StoreFile(ctx context.Context, req *proto.StoreFileRequest) (*proto.StoreFileResponse, error) {
	fullPath := filepath.Join(s.baseDir, filepath.Clean(req.Key))

	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return &proto.StoreFileResponse{Success: false}, fmt.Errorf("failed to create base dir: %v", err)
	}

	if err := os.WriteFile(fullPath, req.Data, 0644); err != nil {
		return &proto.StoreFileResponse{Success: false}, fmt.Errorf("failed to write data: %v", err)
	}

	return &proto.StoreFileResponse{Success: true}, nil
}

func (s *StorageServer) GetFile(ctx context.Context, req *proto.GetFileRequest) (*proto.GetFileResponse, error) {
	fullPath := filepath.Join(s.baseDir, filepath.Clean(req.Key))

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return &proto.GetFileResponse{Data: nil}, fmt.Errorf("failed to read file: %v", err)
	}

	return &proto.GetFileResponse{Data: data}, nil
}

func (s *StorageServer) DeleteFile(ctx context.Context, req *proto.DeleteFileRequest) (*proto.DeleteFileResponse, error) {
	fullPath := filepath.Join(s.baseDir, filepath.Clean(req.Key))

	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return &proto.DeleteFileResponse{Success: true}, nil
		}
		return &proto.DeleteFileResponse{Success: false}, fmt.Errorf("failed to delete file %v: %v", req.Key, err)
	}

	return &proto.DeleteFileResponse{Success: true}, nil
}
