package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

func main() {
	// Command-line flags for host and port
	port := flag.Int("port", 3333, "Port to accept connections on")
	host := flag.String("host", "127.0.0.1", "Host to bind to")
	flag.Parse()

	address := *host + ":" + strconv.Itoa(*port)

	log.Printf("Server will accept connections on %s...", address)

	In, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}
	defer In.Close()

	for {
		conn, err := In.Accept()
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		log.Printf("Receive connection from: %v\n", conn.RemoteAddr())
		go Calculator(conn)
	}
}

func Calculator(conn net.Conn) {
	defer conn.Close()
	// fmt.Fprintln(conn, "Connected to server...")
	reader := bufio.NewReader(conn)
	var cur int64 = 0
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			// fmt.Fprintf(conn, "Error Reading String: %v\n", err)
			return
		}

		line = strings.Trim(line, "\r\n")
		log.Println(line)
		if line == "" {
			// fmt.Fprintf(conn, "Output: \033[F%d\n", cur)
			fmt.Fprintf(conn, "%v\r\n", cur)
			log.Printf("Output: %v\n", cur)
			continue
		}

		Args := strings.Split(line, " ")
		if len(Args) != 2 {
			fmt.Fprintln(conn, "Error: Invalid Command Format")
			continue
		}

		cmd := strings.ToUpper(Args[0])
		num, err := strconv.ParseInt(Args[1], 10, 64)
		if err != nil {
			fmt.Fprintf(conn, "Error reading number: %v\n", err)
			continue
		}

		switch cmd {
		case "SET":
			cur = num
		case "ADD":
			cur += num
		case "SUB":
			cur -= num
		case "MUL":
			cur *= num
		default:
			fmt.Fprintln(conn, "Error: Unknown Command")
		}
	}

}
