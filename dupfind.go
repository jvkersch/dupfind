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

type Metadata struct {
	Path     string `json:"path"`
	Checksum string `json:"checksum"`
}

func (b *BuildCmd) Run(ctx *Context) error {

	paths := make(chan string)
	metadata := make(chan Metadata)
	var wg sync.WaitGroup

	// close metadata channel once all producers are done
	var gather sync.WaitGroup
	go func() {
		gather.Wait()
		close(metadata)
	}()

	// start producer (paths)
	wg.Add(1)
	go func() {
		defer wg.Done()
		produceFilePaths(b.Path, paths)
	}()

	// start consumer/producer (paths -> metadata)
	for i := 0; i < b.Workers; i++ {
		wg.Add(1)
		gather.Add(1)
		go func(consumerID int) {
			defer wg.Done()
			defer gather.Done()
			consumeFilePaths(consumerID, paths, metadata)
		}(i)
	}

	// start consumer (metadata)
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeMetadata(metadata, b.Index)
	}()

	wg.Wait()
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

func consumeMetadata(metadata <-chan Metadata, index string) {
	file, err := os.Create(index)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for record := range metadata {
		jsonData, err := json.Marshal(record)
		if err != nil {
			panic(err)
		}
		file.Write(jsonData)
		file.WriteString("\n")
	}

	fmt.Printf("Index file %s written.\n", index)
}

var cli struct {
	Build BuildCmd `cmd:"" help:"Build index"`
}

func main() {
	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{})
	ctx.FatalIfErrorf(err)
}
