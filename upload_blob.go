package azblobext

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// uploadReaderAtToBlockBlob uploads a buffer in blocks to a block blob.
func uploadReaderAtToBlockBlob(ctx context.Context, reader io.ReaderAt, readerSize int64,
	blockBlobURL azblob.BlockBlobURL, o azblob.UploadToBlockBlobOptions) (azblob.CommonResponse, error) {
	if o.BlockSize == 0 {
		// If bufferSize > (BlockBlobMaxStageBlockBytes * BlockBlobMaxBlocks), then error
		if readerSize > azblob.BlockBlobMaxStageBlockBytes*azblob.BlockBlobMaxBlocks {
			return nil, errors.New("content is too large to upload to a block blob")
		}

		// If bufferSize <= BlockBlobMaxUploadBlobBytes, then Upload should be used with just 1 I/O request
		if readerSize <= azblob.BlockBlobMaxUploadBlobBytes {
			o.BlockSize = azblob.BlockBlobMaxUploadBlobBytes // Default if unspecified
		} else {
			o.BlockSize = readerSize / azblob.BlockBlobMaxBlocks   // buffer / max blocks = block size to use all 50,000 blocks
			if o.BlockSize < azblob.BlobDefaultDownloadBlockSize { // If the block size is smaller than 4MB, round up to 4MB
				o.BlockSize = azblob.BlobDefaultDownloadBlockSize
			}
			// StageBlock will be called with blockSize blocks and a Parallelism of (BufferSize / BlockSize).
		}
	}

	if readerSize <= azblob.BlockBlobMaxUploadBlobBytes {
		// If the size can fit in 1 Upload call, do it this way
		var body io.ReadSeeker = io.NewSectionReader(reader, 0, readerSize)

		if o.Progress != nil {
			body = pipeline.NewRequestBodyProgress(body, o.Progress)
		}

		return blockBlobURL.Upload(ctx, body, o.BlobHTTPHeaders, o.Metadata, o.AccessConditions, o.BlobAccessTier, o.BlobTagsMap)
	}

	numBlocks := uint16(((readerSize - 1) / o.BlockSize) + 1)
	blockIDList := make([]string, numBlocks) // Base-64 encoded block IDs
	progress := int64(0)
	progressLock := &sync.Mutex{}

	err := azblob.DoBatchTransfer(ctx, azblob.BatchTransferOptions{
		OperationName: "uploadReaderAtToBlockBlob",
		TransferSize:  readerSize,
		ChunkSize:     o.BlockSize,
		Parallelism:   o.Parallelism,
		Operation: func(offset int64, count int64, ctx context.Context) error {
			// This function is called once per block.
			// It is passed this block's offset within the buffer and its count of bytes
			// Prepare to read the proper block/section of the buffer
			var body io.ReadSeeker = io.NewSectionReader(reader, offset, count)
			blockNum := offset / o.BlockSize
			if o.Progress != nil {
				blockProgress := int64(0)
				body = pipeline.NewRequestBodyProgress(body,
					func(bytesTransferred int64) {
						diff := bytesTransferred - blockProgress
						blockProgress = bytesTransferred
						progressLock.Lock() // 1 goroutine at a time gets a progress report
						progress += diff
						o.Progress(progress)
						progressLock.Unlock()
					})
			}

			// Block IDs are unique values to avoid issue if 2+ clients are uploading blocks
			// at the same time causing PutBlockList to get a mix of blocks from all the clients.
			blockIDList[blockNum] = base64.StdEncoding.EncodeToString(newUUID().bytes())
			_, err := blockBlobURL.StageBlock(ctx, blockIDList[blockNum], body, o.AccessConditions.LeaseAccessConditions, nil)
			return err
		},
	})
	if err != nil {
		return nil, err
	}

	// All put blocks were successful, call Put Block List to finalize the blob
	return blockBlobURL.CommitBlockList(ctx, blockIDList, o.BlobHTTPHeaders, o.Metadata, o.AccessConditions, o.BlobAccessTier, o.BlobTagsMap)
}

// UploadFileToBlockBlob uploads a file in blocks to a block blob.
func UploadFileToBlockBlob(ctx context.Context, file *os.File,
	blockBlobURL azblob.BlockBlobURL, o azblob.UploadToBlockBlobOptions) (azblob.CommonResponse, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return uploadReaderAtToBlockBlob(ctx, file, stat.Size(), blockBlobURL, o)
}
