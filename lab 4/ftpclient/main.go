package main

import (
	"flag"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func main() {
	port := flag.Int("port", 3333, "Port to accept connections on")
	host := flag.String("host", "127.0.0.1", "Host to bind to")
	flag.Parse()

	if flag.NArg() < 2 {
		log.Fatalf("Usage: %s <STOR|RETR> <filename> [rate]", os.Args[0])
	}
	cmd := flag.Arg(0)
	filename := flag.Arg(1)
	rate := flag.Arg(2)
	addr := *host + ":" + strconv.Itoa(*port)
	log.Printf("Connecting to %s on port %d", *host, *port)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v\n", addr)
	}
	defer conn.Close()

	switch cmd {
	case "STOR":
		conn.Write([]byte("STOR " + filepath.Base(filename) + "\n"))
		upload, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Failed to open file: %v\n", filename)
		}
		defer upload.Close()

		io.Copy(conn, upload)
		log.Printf("Uploaded: %v\n", filename)

	case "RETR":
		cmd := "RETR " + filepath.Base(filename) + " " + rate + "\n"
		// if rate == "" {
		// 	cmd += "0\n"
		// } else {
		// 	cmd += rate + "\n"
		// }
		conn.Write([]byte(cmd))

		download, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Failed to create file: %v\n", err)
		}
		defer download.Close()

		startTime := time.Now()
		io.Copy(download, conn)
		log.Printf("Downloaded: %v\n", filename)
		total_elasped := time.Since(startTime)
		log.Printf("Total transmission time: %v\n", total_elasped)

	default:
		log.Fatalf("Unknown command\n")
	}

}
