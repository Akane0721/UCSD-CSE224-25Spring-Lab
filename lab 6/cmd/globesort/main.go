package main

import (
	"context"
	"encoding/binary"
	"fmt"
	pb "globesort/protos/pb"
	"io"
	"log"
	"math/bits"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

type GlobeSortConfig struct {
	Nodes []struct {
		NodeID int    `yaml:"nodeID"`
		Host   string `yaml:"host"`
		Port   int    `yaml:"port"`
	} `yaml:"nodes"`
}

func readConfig(configFilePath string) (*GlobeSortConfig, error) {
	config := GlobeSortConfig{}
	content, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(content, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

var (
	nodeID     int
	mutex      sync.Mutex
	recvCount  int
	expectRecv int
	recvReady  = make(chan struct{})
	buffer     = make(map[int][]*pb.Record)
	clients    = make(map[int]pb.NodeServiceClient)
)

const batchSize = 1000

type server struct {
	pb.UnimplementedNodeServiceServer
}

func (s *server) SendData(ctx context.Context, req *pb.DataRequest) (*pb.DataResponse, error) {
	mutex.Lock()
	defer mutex.Unlock()

	buffer[nodeID] = append(buffer[nodeID], req.Records...)
	if req.Final {
		recvCount++
	}
	if recvCount >= expectRecv {
		close(recvReady)
	}

	return &pb.DataResponse{Ack: true}, nil
}

func extractID(key []byte, total_bits int) int {
	if len(key) == 0 || total_bits == 0 {
		return 0
	}

	first_byte := key[0]
	return int(first_byte >> (8 - total_bits))
}

func sendBatch(ctx context.Context, target int, final bool) error {
	mutex.Lock()
	recs := buffer[target]
	buffer[target] = buffer[target][:0]
	mutex.Unlock()

	req := &pb.DataRequest{Records: recs, Final: final}

	_, err := clients[target].SendData(ctx, req)
	if err != nil {
		return fmt.Errorf("send data to node %v from node %v failed: %v", target, nodeID, err)
	}
	log.Printf("sent %v records from node %v to node %v", len(recs), nodeID, target)

	return nil
}

func readFile(file *os.File, idLength int) error {
	var length uint32
	err := binary.Read(file, binary.BigEndian, &length)
	if err != nil {
		return err
	}

	if length < 10 {
		return fmt.Errorf("total length of key and value less than 10: %d", length)
	}

	key := [10]byte{}
	_, err = io.ReadFull(file, key[:])
	if err != nil {
		return err
	}
	targetID := extractID(key[:], idLength)

	value_len := length - 10
	value := make([]byte, value_len)
	_, err = io.ReadFull(file, value)
	if err != nil {
		return err
	}

	mutex.Lock()

	buffer[targetID] = append(buffer[targetID], &pb.Record{
		Len:   length,
		Key:   key[:],
		Value: value,
	})
	shouldFlush := targetID != nodeID && len(buffer[targetID]) >= batchSize
	mutex.Unlock()

	if shouldFlush {
		sendBatch(context.Background(), targetID, false)
	}

	return nil
}

func ReadAndSend(input_path string, total_nodes uint) error {
	in, err := os.Open(input_path)
	if err != nil {
		return fmt.Errorf("open %v failed: %v", input_path, err)
	}
	defer in.Close()

	idLength := bits.Len(total_nodes) - 1
	for {
		err = readFile(in, idLength)
		if err == io.EOF {
			log.Printf("Read Finished.")
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func writeFile(file *os.File, record *pb.Record) error {
	err := binary.Write(file, binary.BigEndian, record.Len)
	if err != nil {
		return err
	}
	_, err = file.Write(record.Key[:])
	if err != nil {
		return err
	}
	_, err = file.Write(record.Value)
	return err
}

func SortAndSave(output_path string) error {
	sort.Slice(buffer[nodeID], func(i, j int) bool {
		return string(buffer[nodeID][i].Key) < string(buffer[nodeID][j].Key)
	})

	f, err := os.Create(output_path)
	if err != nil {
		return fmt.Errorf("node %v failed to create file %v: %v", nodeID, output_path, err)
	}
	defer f.Close()

	for i := range buffer[nodeID] {
		err := writeFile(f, buffer[nodeID][i])
		if err != nil {
			return fmt.Errorf("node %v failed to save file %v: %v", nodeID, output_path, err)
		}
	}
	return nil
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(os.Stdout)

	if len(os.Args) != 5 {
		fmt.Println("Usage:", os.Args[0], "<nodeID> <inputFilePath> <outputFilePath> <configFilePath>")
		os.Exit(1)
	}

	var err error
	nodeID, err = strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Server ID must be an integer! Got '%s'", os.Args[1])
	}

	inputFilePath := os.Args[2]
	outputFilePath := os.Args[3]
	configFilePath := os.Args[4]

	// log.Printf("serverID: %d", nodeID)
	// log.Printf("inputFilePath: %s", inputFilePath)
	// log.Printf("outputFilePath: %s", outputFilePath)
	// log.Printf("configFilePath: %s", configFilePath)

	config, err := readConfig(configFilePath)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	log.Printf("Configured node: %+v", config.Nodes[nodeID])

	// Initialization
	expectRecv = len(config.Nodes) - 1
	if expectRecv == 0 {
		close(recvReady)
	}
	var addr string
	for _, node := range config.Nodes {
		buffer[node.NodeID] = []*pb.Record{}
		if node.NodeID == nodeID {
			addr = fmt.Sprintf("%s:%d", node.Host, node.Port)
		}
	}

	// Start Server
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen %v: %v", addr, err)
	}
	s := grpc.NewServer()
	pb.RegisterNodeServiceServer(s, &server{})
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Dial all peers, for-loop & sleep
	unconnected := make(map[int]string)
	for _, node := range config.Nodes {
		if node.NodeID == nodeID {
			continue
		}
		targetAddr := fmt.Sprintf("%v:%v", node.Host, node.Port)
		unconnected[node.NodeID] = targetAddr
	}

	for len(unconnected) > 0 {
		for id, targetAddr := range unconnected {
			conn, err := grpc.NewClient(targetAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("node %v dial node %v failed: %v, retry in 500ms...", nodeID, id, err)
				continue
			}
			clients[id] = pb.NewNodeServiceClient(conn)
			delete(unconnected, id)
			log.Printf("node %v dial node %v succeed", nodeID, id)
		}
		if len(unconnected) > 0 {
			time.Sleep(300 * time.Millisecond)
		}
	}

	// Read records and send
	total_nodes := uint(len(config.Nodes))
	err = ReadAndSend(inputFilePath, total_nodes)
	if err != nil {
		log.Fatal(err)
	}

	// Send remaining records
	for target := range buffer {
		if target == nodeID {
			continue
		}
		sendBatch(context.Background(), target, true)
	}

	<-recvReady

	// Sort and write to file
	err = SortAndSave(outputFilePath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Node %v done: wrote %v records to %v", nodeID, len(buffer[nodeID]), outputFilePath)
}
