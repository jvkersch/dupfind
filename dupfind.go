package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/alecthomas/kong"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type Context struct {
}

type BuildCmd struct {
	Path    string `arg:"" name:"path" help:"Directory to index." type:"path"`
	Index   string `arg:"" help:"Index file." type:"path"`
	Workers int    `short:"j" help:"Number of parallel workers" default:"4"`
}

type FindCmd struct {
	Path    string `arg:"" name:"path" help:"Directory of files to look up." type:"path"`
	Index   string `arg:"" help:"Index file." type:"path"`
	Workers int    `short:"j" help:"Number of parallel workers" default:"4"`
	Short   bool   `help:"For duplicate files, only print out path"`
}

type Metadata struct {
	Path     string `json:"path"`
	Checksum string `json:"checksum"`
}

func produceMetadata(root string, workers int) <-chan Metadata {

	paths := make(chan string)
	metadata := make(chan Metadata)

	// close metadata channel once all producers are done
	var gather sync.WaitGroup
	go func() {
		gather.Wait()
		close(metadata)
	}()

	// start producer
	go produceFilePaths(root, paths)

	// start consumer/producer (path -> metadata)
	for i := 0; i < workers; i++ {
		gather.Add(1)
		go func(consumerID int) {
			defer gather.Done()
			consumeFilePaths(consumerID, paths, metadata)
		}(i)
	}

	return metadata
}

func (b *BuildCmd) Run(ctx *Context) error {

	metadata := produceMetadata(b.Path, b.Workers)
	writeIndex(metadata, b.Index)

	return nil
}

func produceFilePaths(root string, paths chan<- string) {
	defer close(paths)

	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			paths <- path
		}
		return nil
	})
}

func consumeFilePaths(id int, paths <-chan string, metadata chan<- Metadata) {
	for path := range paths {
		checksum, err := computeChecksum(path)
		if err != nil {
			log.Printf("Could not compute checksum for file %s: %v", path, err)
			continue
		}
		metadata <- Metadata{Path: path, Checksum: checksum}
	}
}

func computeChecksum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func writeIndex(metadata <-chan Metadata, index string) {

	var records []Metadata
	for record := range metadata {
		records = append(records, record)
	}

	file, err := os.Create(index)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	jsonData, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		panic(err)
	}
	file.Write(jsonData)

	fmt.Printf("Index file %s written.\n", index)
}

func (f *FindCmd) Run(ctx *Context) error {

	index := loadIndex(f.Index)
	metadata := produceMetadata(f.Path, f.Workers)
	lookupRecords(metadata, index, f.Short)

	return nil
}

func loadIndex(path string) map[string]string {

	jsonData, err := os.ReadFile(path)
	if err != nil {
		log.Fatal("Error reading file:", err)
	}

	var records []Metadata
	err = json.Unmarshal(jsonData, &records)
	if err != nil {
		log.Fatal("Error unmarshaling JSON:", err)
	}

	index := make(map[string]string)
	for _, record := range records {
		index[record.Checksum] = record.Path
	}

	return index
}

func lookupRecords(metadata <-chan Metadata, index map[string]string, short bool) {
	for record := range metadata {
		indexPath, duplicate := index[record.Checksum]
		if duplicate {
			if short {
				file := filepath.Base(record.Path)
				fmt.Println(file)
			} else {
				fmt.Printf("File %s is duplicate with index file %s\n",
					record.Path, indexPath)
			}
		}
	}
}

var cli struct {
	Build BuildCmd `cmd:"" help:"Build index"`
	Find  FindCmd  `cmd:"" help:"Look up files in index"`
}

func main() {
	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{})
	ctx.FatalIfErrorf(err)
}
