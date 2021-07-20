package azblobext

import (
	"context"
	"io/ioutil"
	"os"

	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

// create a test file
func generateFile(fileName string, fileSize int) []byte {
	// generate random data
	_, bigBuff := getRandomDataAndReader(fileSize)

	// write to file and return the data
	ioutil.WriteFile(fileName, bigBuff, 0666)
	return bigBuff
}

func performUploadAndDownloadFileTest(
	c *chk.C,
	fileSize, blockSize, parallelism, downloadOffset, downloadCount int,
) {
	// Set up file to upload
	fileName := "BigFile.bin"
	fileData := generateFile(fileName, fileSize)

	// Open the file to upload
	file, err := os.Open(fileName)
	c.Assert(err, chk.IsNil)
	defer os.Remove(fileName)
	defer file.Close()

	// Get the service URL
	bsu, err := getBSU()
	c.Assert(err, chk.IsNil)

	// Set up test container
	containerURL, _ := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// Set up test blob
	blockBlobURL, _ := getBlockBlobURL(c, containerURL)

	// Upload the file to a block blob
	response, err := UploadFileToBlockBlob(context.Background(), file, blockBlobURL,
		azblob.UploadToBlockBlobOptions{
			BlockSize:   int64(blockSize),
			Parallelism: uint16(parallelism),
			// If Progress is non-nil, this function is called periodically as bytes are uploaded.
			Progress: func(bytesTransferred int64) {
				c.Assert(
					bytesTransferred > 0 && bytesTransferred <= int64(fileSize),
					chk.Equals,
					true,
				)
			},
		},
		azblob.ClientProvidedKeyOptions{})
	c.Assert(err, chk.IsNil)
	c.Assert(response.Response().StatusCode, chk.Equals, 201)

	// Set up file to download the blob to
	destFileName := "BigFile-downloaded.bin"
	destFile, err := os.Create(destFileName)
	c.Assert(err, chk.IsNil)
	defer os.Remove(destFileName)
	defer destFile.Close()

	// Perform download
	err = DownloadBlobToFile(
		context.Background(),
		blockBlobURL.BlobURL,
		int64(downloadOffset),
		int64(downloadCount),
		destFile,
		azblob.DownloadFromBlobOptions{
			BlockSize:   int64(blockSize),
			Parallelism: uint16(parallelism),
			// If Progress is non-nil, this function is called periodically as bytes are uploaded.
			Progress: func(bytesTransferred int64) {
				c.Assert(
					bytesTransferred > 0 && bytesTransferred <= int64(fileSize),
					chk.Equals,
					true,
				)
			},
		},
		azblob.ClientProvidedKeyOptions{},
	)

	// Assert download was successful
	c.Assert(err, chk.IsNil)

	// Assert downloaded data is consistent
	var destBuffer []byte
	if downloadCount == azblob.CountToEnd {
		destBuffer = make([]byte, fileSize-downloadOffset)
	} else {
		destBuffer = make([]byte, downloadCount)
	}

	n, err := destFile.Read(destBuffer)
	c.Assert(err, chk.IsNil)

	if downloadOffset == 0 && downloadCount == 0 {
		c.Assert(destBuffer, chk.DeepEquals, fileData)
	} else {
		if downloadCount == 0 {
			c.Assert(n, chk.Equals, fileSize-downloadOffset)
			c.Assert(destBuffer, chk.DeepEquals, fileData[downloadOffset:])
		} else {
			c.Assert(n, chk.Equals, downloadCount)
			c.Assert(destBuffer, chk.DeepEquals, fileData[downloadOffset:downloadOffset+downloadCount])
		}
	}
}

func (s *azblobextSuite) TestUploadAndDownloadFileInChunks(c *chk.C) {
	fileSize := 8 * 1024
	blockSize := 1024
	parallelism := 3
	performUploadAndDownloadFileTest(c, fileSize, blockSize, parallelism, 0, 0)
}

func (s *azblobextSuite) TestUploadAndDownloadFileSingleIO(c *chk.C) {
	fileSize := 1024
	blockSize := 2048
	parallelism := 3
	performUploadAndDownloadFileTest(c, fileSize, blockSize, parallelism, 0, 0)
}

func (s *azblobextSuite) TestUploadAndDownloadFileSingleRoutine(c *chk.C) {
	fileSize := 8 * 1024
	blockSize := 1024
	parallelism := 1
	performUploadAndDownloadFileTest(c, fileSize, blockSize, parallelism, 0, 0)
}

func (s *azblobextSuite) TestUploadAndDownloadFileEmpty(c *chk.C) {
	fileSize := 0
	blockSize := 1024
	parallelism := 3
	performUploadAndDownloadFileTest(c, fileSize, blockSize, parallelism, 0, 0)
}

func (s *azblobextSuite) TestUploadAndDownloadFileNonZeroOffset(c *chk.C) {
	fileSize := 8 * 1024
	blockSize := 1024
	parallelism := 3
	downloadOffset := 1000
	downloadCount := 0
	performUploadAndDownloadFileTest(
		c,
		fileSize,
		blockSize,
		parallelism,
		downloadOffset,
		downloadCount,
	)
}

func (s *azblobextSuite) TestUploadAndDownloadFileNonZeroCount(c *chk.C) {
	fileSize := 8 * 1024
	blockSize := 1024
	parallelism := 3
	downloadOffset := 0
	downloadCount := 6000
	performUploadAndDownloadFileTest(
		c,
		fileSize,
		blockSize,
		parallelism,
		downloadOffset,
		downloadCount,
	)
}

func (s *azblobextSuite) TestUploadAndDownloadFileNonZeroOffsetAndCount(c *chk.C) {
	fileSize := 8 * 1024
	blockSize := 1024
	parallelism := 3
	downloadOffset := 1000
	downloadCount := 6000
	performUploadAndDownloadFileTest(
		c,
		fileSize,
		blockSize,
		parallelism,
		downloadOffset,
		downloadCount,
	)
}
