package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
)

type record struct {
	len uint32
	key [10]byte
	val []byte
}

func readFile(file *os.File) (*record, error) {
	var length uint32
	err := binary.Read(file, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}

	if length < 10 {
		return nil, fmt.Errorf("Total length of key and value less than 10: %d", length)
	}

	key := [10]byte{}
	_, err = io.ReadFull(file, key[:])
	if err != nil {
		return nil, err
	}

	value_len := length - 10
	value := make([]byte, value_len)
	_, err = io.ReadFull(file, value)
	if err != nil {
		return nil, err
	}

	return &record{
		len: length,
		key: key,
		val: value,
	}, nil
}

func writeFile(file *os.File, record *record) error {
	err := binary.Write(file, binary.BigEndian, record.len)
	if err != nil {
		return err
	}
	_, err = file.Write(record.key[:])
	if err != nil {
		return err
	}
	_, err = file.Write(record.val)
	return err
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 3 {
		log.Fatalf("Usage: %v inputfile outputfile\n", os.Args[0])
	}

	log.Printf("Sorting %s to %s\n", os.Args[1], os.Args[2])

	inputFile := os.Args[1]
	outputFile := os.Args[2]

	in, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("Failed to open input file: %v\n", err)
		os.Exit(1)
	}
	defer in.Close()

	records := []*record{}
	for {
		record, err := readFile(in)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Failed to read record: %v\n", err)
			os.Exit(1)
		}
		records = append(records, record)
	}

	sort.Slice(records, func(i, j int) bool {
		return string(records[i].key[:]) < string(records[j].key[:])
	})

	out, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Failed to create output file: %v\n", err)
	}
	defer out.Close()

	for _, record := range records {
		err := writeFile(out, record)
		if err != nil {
			fmt.Printf("Failed to write record: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Complete!")
}
