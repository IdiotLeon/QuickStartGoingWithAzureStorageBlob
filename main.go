package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type Credentials struct {
	AzureStorageAccountName string `json:"azure_storage_account_name"`
	AzureStorageAccountKey  string `json:"azure_storage_access_key"`
}

func randomString() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return strconv.Itoa(r.Int())
}

func handleErrors(err error) {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok {
			switch serr.ServiceCode() {
			case azblob.ServiceCodeContainerAlreadyExists:
				fmt.Println("Received 409. Container already exists")
				return
			}
		}
		log.Fatal(err)
	}
}

func main() {
	fmt.Printf("Azure Blob Storage quick start sample\n")

	// From the Azure portal, to get your storage account name and account key
	credentialsFile, err := os.Open("credentials.json")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully loading credentials.json")
	defer credentialsFile.Close()
	byteValue, _ := ioutil.ReadAll(credentialsFile)
	var credentials Credentials
	json.Unmarshal(byteValue, &credentials)

	if len(credentials.AzureStorageAccountName) == 0 || len(credentials.AzureStorageAccountKey) == 0 {
		log.Fatal("Either the AzureStorageAccountName or AzureStorageAccountKey variable is empty")
	}

	// To create a default request pipeline the storage account name and key
	credential, err := azblob.NewSharedKeyCredential(credentials.AzureStorageAccountName, credentials.AzureStorageAccountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// To create a random string for the quick start container
	containerName := fmt.Sprintf("quickstart-%s", randomString())

	// From the Azure portal, to get your storage account blob service URL endpoint
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", credentials.AzureStorageAccountName, containerName))

	// To create a ContainerURL object that wraps the container URL and a request pipeline to make requests
	containerURL := azblob.NewContainerURL(*URL, p)

	// To create the container
	fmt.Printf("Creating a container named %s\n", containerName)
	ctx := context.Background() // This is a never-expiring context
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	handleErrors(err)

	// To create a file to test the upload and download
	fmt.Printf("Creating a dummy file to test the upload and download\n")
	data := []byte("hello world this is a blob\n")
	fileName := randomString()
	err = ioutil.WriteFile(fileName, data, 0700)
	handleErrors(err)

	// To upload a blob
	blobURL := containerURL.NewBlockBlobURL(fileName)
	file, err := os.Open(fileName)
	handleErrors(err)

	// One can use the low-level PutBlob API to upload files. Low-level APIs are simple wrappers for the Azure Storage REST APIs.
	// Note that PutBlob can upload up to 256MB data in one shot. Details: https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob
	// Following is commented out intentionally because we will instead use UploadFileToBlockBlob API to upload the blob
	// _, err = blobURL.PutBlob(ctx, file, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	// handleErrors(err)

	// The high-level API UploadFileToBlockBlob function uploads blocks in parallel for optimal performance, and can handle large files as well.
	// This function calls PutBlock/PutBlockList for files larger 256 MBs, and calls PutBlob for any file smaller
	fmt.Printf("Uploading the file with blob name: %s\n", fileName)
	_, err = azblob.UploadFileToBlockBlob(ctx, file, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})
	handleErrors(err)

	// To list the container created
	fmt.Println("Listing the blobs in the container:")
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// To get a result segment starting with the blob indicated by the current Marker
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		handleErrors(err)

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// To process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			fmt.Print(" Blob name: " + blobInfo.Name + "\n")
		}
	}

	// To download the blob
	downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)

	// <Note>: automatically retries are preformed if the connection fails
	bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})

	// To read the body into a buffer
	downloadedData := bytes.Buffer{}
	_, err = downloadedData.ReadFrom(bodyStream)
	handleErrors(err)

	// To print the downloaded blob data, which is in downloadData's buffer.
	fmt.Printf("Press enter key to delete the sample files, example cntainer, and exit the application.\n")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
	fmt.Printf("Cleaning up.\n")
	containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	file.Close()
	os.Remove(fileName)
}
