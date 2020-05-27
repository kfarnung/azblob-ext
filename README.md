# azblob Extensions

[![GoDoc](https://godoc.org/github.com/kfarnung/go-azblobext?status.svg)](https://godoc.org/github.com/kfarnung/go-azblobext)
[![Build Status](https://travis-ci.com/kfarnung/go-azblobext.svg?branch=master)](https://travis-ci.com/kfarnung/go-azblobext)

Extensions for the [Azure Storage Blob SDK for Go]. The primary changes are
around the "high level" functions of the SDK including:

- DownloadBlobToFile
- UploadFileToBlockBlob

The original code is from the SDK with modifications to support more efficient
file operations.

[Azure Storage Blob SDK for Go]: https://github.com/Azure/azure-storage-blob-go
