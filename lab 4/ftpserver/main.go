package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	port := flag.Int("port", 3333, "Port to accept connections on")
	host := flag.String("host", "127.0.0.1", "Host to bind to")
	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatalf("Please provide the directory: Usage: %v [-host <host>] [-port <port>] <directory>", os.Args[0])
	}
	directory := flag.Arg(0)

	log.Printf("%s:%d", *host, *port)
	addr := *host + ":" + strconv.Itoa(*port)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}
	defer lis.Close()

	for {
		conn, err := lis.Accept()
		if err != nil {
			continue
		}
		go Ftp(conn, directory)
	}
}

func Ftp(conn net.Conn, dir string) {
	defer conn.Close()
	log.Printf("Connected to: %v\n", conn.RemoteAddr())
	reader := bufio.NewReader(conn)

	line, err := reader.ReadString('\n')
	if err != nil {
		if err != io.EOF {
			log.Printf("Error Read: %v\n", err)
		}
		return
	}
	log.Print(line)

	args := strings.Fields(strings.Trim(line, "\r\n"))
	if len(args) < 2 {
		conn.Write([]byte("Invalid Command\n"))
		return
	}
	cmd, filename := args[0], args[1]
	path := filepath.Join(dir, filepath.Base(filename))
	log.Println(path)

	switch cmd {
	case "STOR":
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			log.Fatalf("Failed to make dir: %v\n", dir)
		}

		fp, err := os.Create(path)
		if err != nil {
			conn.Write([]byte("Failed to write file\n"))
			return
		}
		defer fp.Close()

		io.Copy(fp, reader)
		log.Printf("Stored: %v\n", filename)
		// conn.Write([]byte("Succeed!"))

	case "RETR":
		rateLimit := 0
		if len(args) == 3 {
			if rateLimit, err = strconv.Atoi(args[2]); err != nil {
				fmt.Fprintf(conn, "Error setting rate limit: %v\n", err)
			}
		}
		fp, err := os.Open(path)
		if err != nil {
			conn.Write([]byte("Failed to open file\n"))
			return
		}
		defer fp.Close()

		if rateLimit == 0 {
			io.Copy(conn, fp)
		} else {
			sendWithRateLimit(conn, fp, rateLimit)
		}
		log.Printf("Sent: %v\n", filename)
		// conn.Write([]byte("Succeed!"))

	default:
		conn.Write([]byte("Unknown Command\n"))
		return
	}
}

func sendWithRateLimit(conn net.Conn, fp io.Reader, bitsRateLimit int) {
	buf := make([]byte, 1024)
	bytesRateLimit := bitsRateLimit / 8

	for {
		n, err := fp.Read(buf)
		if err != nil {
			return
		}
		if n > 0 {
			start := time.Now()
			conn.Write(buf[:n])
			elasped := time.Since(start)
			interval := time.Duration(n * int(time.Second) / bytesRateLimit)
			if elasped < interval {
				time.Sleep(interval - elasped)
			}
		}
	}
}
