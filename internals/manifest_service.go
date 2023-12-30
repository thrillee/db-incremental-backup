package internals

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

type CreateManifestRequest struct {
	ExportDir  string
	Manifiests []ManifestData
	AllowZip   bool
	Tag        string
}

func writeToZip(file_path, fileName string, zipWriter *zip.Writer) {
	f1, err := os.Open(file_path)
	errCheck(err)
	defer f1.Close()

	fmt.Println("writing first file to archive...")
	w1, err := zipWriter.Create(fileName)
	if err != nil {
		panic(err)
	}
	if _, err := io.Copy(w1, f1); err != nil {
		panic(err)
	}
}

func createZip(req CreateManifestRequest, manifestPath string) {
	log.Println("creating zip archive...")

	fileName := fmt.Sprintf("archive-%s.zip", req.Tag)
	archive_path := fmt.Sprintf("%s/%s", req.ExportDir, fileName)
	log.Printf("ZIP OUT DIR -> %s....\n", archive_path)

	archive, err := os.Create(archive_path)
	errCheck(err)
	defer archive.Close()

	zipWriter := zip.NewWriter(archive)
	defer zipWriter.Close()
	for _, md := range req.Manifiests {
		file_path := md.file_path
		if md.file_path == "" {
			continue
		}

		writeToZip(file_path, md.FileName, zipWriter)

	}

	writeToZip(manifestPath, "manifest.json", zipWriter)

	// cleanZipFiles(req.Manifiests, manifestPath)
}

func cleanZipFiles(manifests []ManifestData, manifestPath string) {
	log.Println("<<<<<<<<<<<Cleaning ZIP Files>>>>>>>>>>>")
	defer log.Println("<<<<<<<<<<<Cleaning ZIP Files Completed>>>>>>>>>>>")

	for _, md := range manifests {
		log.Printf("Deleting -> %s....\n", md.file_path)
		os.Remove(md.file_path)
	}
	os.Remove(manifestPath)
	log.Printf("Deleting -> %s....\n", manifestPath)
}

func CreateManifest(req CreateManifestRequest) {
	fileName := fmt.Sprintf("%s-%s.json", "manifest", req.Tag)

	manifest_dir := fmt.Sprintf("%s/%s", req.ExportDir, fileName)

	b, err := json.MarshalIndent(req.Manifiests, "", "\t")
	if err != nil {
		log.Fatal(err)
	}

	b = append(b, byte('\n'))

	if err := os.WriteFile(manifest_dir, b, 0644); err != nil {
		log.Fatal(err)
	}

	log.Printf("Manifest data written to %s", manifest_dir)

	if req.AllowZip {
		createZip(req, manifest_dir)
	}
}
