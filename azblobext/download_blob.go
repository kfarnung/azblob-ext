package azblobext

import (
	"context"
	"io"
	"os"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// downloadBlobToWriterAt downloads an Azure blob to a buffer with parallel.
func downloadBlobToWriterAt(ctx context.Context, blobURL azblob.BlobURL, offset int64, count int64,
	writer io.WriterAt, o azblob.DownloadFromBlobOptions, initialDownloadResponse *azblob.DownloadResponse) error {
	if o.BlockSize == 0 {
		o.BlockSize = azblob.BlobDefaultDownloadBlockSize
	}

	if count == azblob.CountToEnd { // If size not specified, calculate it
		if initialDownloadResponse != nil {
			count = initialDownloadResponse.ContentLength() - offset // if we have the length, use it
		} else {
			// If we don't have the length at all, get it
			dr, err := blobURL.Download(ctx, 0, azblob.CountToEnd, o.AccessConditions, false)
			if err != nil {
				return err
			}
			count = dr.ContentLength() - offset
		}
	}

	if count <= 0 {
		// The file is empty, there is nothing to download.
		return nil
	}

	// Prepare and do parallel download.
	progress := int64(0)
	progressLock := &sync.Mutex{}

	err := azblob.DoBatchTransfer(ctx, azblob.BatchTransferOptions{
		OperationName: "downloadBlobToWriterAt",
		TransferSize:  count,
		ChunkSize:     o.BlockSize,
		Parallelism:   o.Parallelism,
		Operation: func(chunkStart int64, count int64, ctx context.Context) error {
			dr, err := blobURL.Download(ctx, chunkStart+offset, count, o.AccessConditions, false)
			if err != nil {
				return err
			}

			body := dr.Body(o.RetryReaderOptionsPerBlock)
			if o.Progress != nil {
				rangeProgress := int64(0)
				body = pipeline.NewResponseBodyProgress(
					body,
					func(bytesTransferred int64) {
						diff := bytesTransferred - rangeProgress
						rangeProgress = bytesTransferred
						progressLock.Lock()
						progress += diff
						o.Progress(progress)
						progressLock.Unlock()
					})
			}

			_, err = io.Copy(newSectionWriter(writer, chunkStart, count), body)
			body.Close()
			return err
		},
	})
	if err != nil {
		return err
	}

	return nil
}

// DownloadBlobToFile downloads an Azure blob to a local file.
// The file would be truncated if the size doesn't match.
// Offset and count are optional, pass 0 for both to download the entire blob.
func DownloadBlobToFile(ctx context.Context, blobURL azblob.BlobURL, offset int64, count int64,
	file *os.File, o azblob.DownloadFromBlobOptions) error {
	// 1. Calculate the size of the destination file
	var size int64

	if count == azblob.CountToEnd {
		// Try to get Azure blob's size
		props, err := blobURL.GetProperties(ctx, o.AccessConditions)
		if err != nil {
			return err
		}
		size = props.ContentLength() - offset
	} else {
		size = count
	}

	// 2. Compare and try to resize local file's size if it doesn't match Azure blob's size.
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() != size {
		if err = file.Truncate(size); err != nil {
			return err
		}
	}

	if size > 0 {
		return downloadBlobToWriterAt(ctx, blobURL, offset, size, file, o, nil)
	}

	// if the blob's size is 0, there is no need in downloading it
	return nil
}
