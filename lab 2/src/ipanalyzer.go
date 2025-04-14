package main

import (
	"fmt"
	"log"
	"math"
	"net"
	"os"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 2 && len(os.Args) != 3 {
		log.Fatalf("Usage: %s cidr_block [ip_address]", os.Args[0])
	}

	// os.Args[1] contains the cidr_block
	// os.Args[2] optionally contains the IP address to test

	// Replace the line below and start coding your logic from here.

	cidr := os.Args[1]
	_, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}

	if len(os.Args) == 3 {
		targetIp := net.ParseIP(os.Args[2])
		if targetIp == nil {
			log.Fatalf("Invalid IP address: %v\n", os.Args[2])
		}

		if ipnet.Contains(targetIp) {
			fmt.Println("true")
		} else {
			fmt.Println("false")
		}
		return
	}

	fmt.Printf("Analyzing network: %v\n\n", cidr)

	networkAddress := ipnet.IP.Mask(ipnet.Mask)
	fmt.Printf("Network address: %v\n", networkAddress)

	broadcastAddress := make(net.IP, len(networkAddress))
	copy(broadcastAddress, networkAddress)
	prefixSize, _ := ipnet.Mask.Size()
	for i := prefixSize; i < 32; i++ {
		broadcastAddress[i/8] |= 1 << (7 - i%8)
	}
	fmt.Printf("Broadcast address: %v\n", broadcastAddress)

	subnetMask := net.IPv4(ipnet.Mask[0], ipnet.Mask[1], ipnet.Mask[2], ipnet.Mask[3])
	fmt.Printf("Subnet mask: %v\n", subnetMask)

	usableHosts := math.Pow(2, float64(32-prefixSize)) - 2
	fmt.Printf("Number of usable hosts: %v\n", usableHosts)
}
