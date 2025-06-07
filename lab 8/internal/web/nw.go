// Lab 8: Implement a network video content service (client using consistent hashing)

package web

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"tritontube/internal/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type HashRing struct {
	mutex        sync.Mutex
	nodes        map[uint64]string
	sortedHashes []uint64
}

func NewHashRing() *HashRing {
	return &HashRing{nodes: make(map[uint64]string)}
}

func hashStringToUint64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

func (h *HashRing) addNode(addr string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	hash := hashStringToUint64(addr)
	h.nodes[hash] = addr
	h.sortedHashes = append(h.sortedHashes, hash)
	sort.Slice(h.sortedHashes, func(i, j int) bool {
		return h.sortedHashes[i] < h.sortedHashes[j]
	})
}

func (h *HashRing) removeNode(addr string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	hash := hashStringToUint64(addr)
	delete(h.nodes, hash)

	idx := sort.Search(len(h.sortedHashes), func(i int) bool {
		return h.sortedHashes[i] >= hash
	})
	if idx < len(h.sortedHashes) && h.sortedHashes[idx] == hash {
		h.sortedHashes = append(h.sortedHashes[:idx], h.sortedHashes[idx+1:]...)
	}
}

func (h *HashRing) getNode(key string) (string, error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if len(h.nodes) == 0 {
		return "", fmt.Errorf("no nodes in the hash ring")
	}

	hash := hashStringToUint64(key)
	idx := sort.Search(len(h.sortedHashes), func(i int) bool {
		return h.sortedHashes[i] >= hash
	})
	return h.nodes[h.sortedHashes[uint64(idx%len(h.sortedHashes))]], nil

}

// NetworkVideoContentService implements VideoContentService using a network of nodes.
type NetworkVideoContentService struct {
	proto.UnimplementedVideoContentAdminServiceServer
	mutex    sync.RWMutex
	hashRing *HashRing
	nodes    map[string]proto.VideoContentStorageServiceClient
	conns    map[string]*grpc.ClientConn
	allKeys  []string
	allNodes []string
}

// Uncomment the following line to ensure NetworkVideoContentService implements VideoContentService
var _ VideoContentService = (*NetworkVideoContentService)(nil)

func NewNetworkVideoContentService() *NetworkVideoContentService {
	return &NetworkVideoContentService{
		hashRing: NewHashRing(),
		nodes:    make(map[string]proto.VideoContentStorageServiceClient),
		conns:    make(map[string]*grpc.ClientConn),
		allKeys:  []string{},
		allNodes: []string{},
	}
}

func (s *NetworkVideoContentService) StartAdminServer(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("admin listen to %v failed: %v", addr, err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterVideoContentAdminServiceServer(grpcServer, s)
	log.Printf("Successfully start admin server: %v", addr)
	return grpcServer.Serve(lis)
}

func (s *NetworkVideoContentService) Write(videoId string, filename string, data []byte) error {
	key := fmt.Sprintf("%v/%v", videoId, filename)
	s.mutex.Lock()
	s.allKeys = append(s.allKeys, key)
	s.mutex.Unlock()

	node, err := s.hashRing.getNode(key)
	fmt.Printf("Node for %v: %v\n", key, node)
	if err != nil {
		return err
	}
	client := s.nodes[node]
	_, err = client.StoreFile(context.Background(), &proto.StoreFileRequest{Key: key, Data: data})
	return err
}

func (s *NetworkVideoContentService) Read(videoId string, filename string) ([]byte, error) {
	key := fmt.Sprintf("%v/%v", videoId, filename)
	node, err := s.hashRing.getNode(key)
	if err != nil {
		return nil, err
	}

	client := s.nodes[node]
	resp, err := client.GetFile(context.Background(), &proto.GetFileRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return resp.Data, err
}

func (s *NetworkVideoContentService) AddNode(ctx context.Context, req *proto.AddNodeRequest) (*proto.AddNodeResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	addr := req.NodeAddress
	if _, exists := s.nodes[addr]; exists {
		return &proto.AddNodeResponse{MigratedFileCount: 0}, fmt.Errorf("node %v does not exist", addr)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &proto.AddNodeResponse{MigratedFileCount: 0}, err
	}

	client := proto.NewVideoContentStorageServiceClient(conn)
	s.nodes[addr] = client
	s.conns[addr] = conn
	s.allNodes = append(s.allNodes, addr)

	migrated := int32(0)
	for _, key := range s.allKeys {
		oldNode, err := s.hashRing.getNode(key)
		if err != nil {
			continue
		}
		s.hashRing.addNode(addr)
		newNode, err := s.hashRing.getNode(key)
		if err != nil {
			continue
		}
		s.hashRing.removeNode(addr)

		if oldNode != newNode {
			data, err := s.nodes[oldNode].GetFile(ctx, &proto.GetFileRequest{Key: key})
			if err != nil {
				continue
			}

			_, err = client.StoreFile(ctx, &proto.StoreFileRequest{Key: key, Data: data.Data})
			if err != nil {
				continue
			}
			_, err = s.nodes[oldNode].DeleteFile(ctx, &proto.DeleteFileRequest{Key: key})
			if err != nil {
				continue
			}
			migrated++
		}
	}

	s.hashRing.addNode(addr)
	return &proto.AddNodeResponse{MigratedFileCount: migrated}, nil
}

func (s *NetworkVideoContentService) RemoveNode(ctx context.Context, req *proto.RemoveNodeRequest) (*proto.RemoveNodeResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	addr := req.NodeAddress
	client, exists := s.nodes[addr]
	if !exists {
		return &proto.RemoveNodeResponse{MigratedFileCount: 0}, fmt.Errorf("node %s does not exist", addr)
	}

	migrated := int32(0)
	for _, key := range s.allKeys {
		oldNode, err := s.hashRing.getNode(key)
		if err != nil {
			continue
		}

		if oldNode == addr {
			s.hashRing.removeNode(addr)
			newNode, err := s.hashRing.getNode(key)
			if err != nil {
				continue
			}

			data, err := client.GetFile(ctx, &proto.GetFileRequest{Key: key})
			if err != nil {
				continue
			}
			_, err = s.nodes[newNode].StoreFile(ctx, &proto.StoreFileRequest{Key: key, Data: data.Data})
			if err != nil {
				continue
			}
			_, err = client.DeleteFile(ctx, &proto.DeleteFileRequest{Key: key})
			if err != nil {
				continue
			}
			migrated++
			s.hashRing.addNode(addr)
		}
	}

	s.conns[addr].Close()
	delete(s.conns, addr)
	delete(s.nodes, addr)
	s.hashRing.removeNode(addr)

	for i, nodeAddr := range s.allNodes {
		if nodeAddr == addr {
			s.allNodes = append(s.allNodes[:i], s.allNodes[i+1:]...)
			break
		}
	}

	return &proto.RemoveNodeResponse{MigratedFileCount: migrated}, nil
}

func (s *NetworkVideoContentService) ListNodes(ctx context.Context, req *proto.ListNodesRequest) (*proto.ListNodesResponse, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return &proto.ListNodesResponse{Nodes: s.allNodes}, nil
}
