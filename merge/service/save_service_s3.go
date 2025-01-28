package service

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/tidwall/btree"
)

type s3SaveService struct {
	fsSaveService
	url    string
	key    string
	secret string
	bucket string
	region string
	path   string
	secure bool
}

func (s *s3SaveService) Save(fields [][2]string, data map[string]any, index *btree.BTreeG[int32]) error {
	uid, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	tmpFileName := path.Join("/tmp", uid.String()+".1.parquet")
	err = s.saveTmpFile(tmpFileName, fields, data, index)
	if err != nil {
		return err
	}
	defer os.Remove(tmpFileName)

	return s.uploadToS3(tmpFileName)
}

func (s *s3SaveService) uploadToS3(filePath string) error {
	// Initialize minio client
	minioClient, err := minio.New(s.url, &minio.Options{
		Creds:  credentials.NewStaticV4(s.key, s.secret, ""),
		Secure: s.secure,
		Region: s.region,
	})
	if err != nil {
		return fmt.Errorf("failed to create minio client: %w", err)
	}

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file information
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Get the file name from the path
	fileName := path.Base(filePath)

	// Create the S3 key (path in the bucket)
	s3Key := path.Join(s.path, fileName)

	// Upload the file to S3
	_, err = minioClient.PutObject(context.Background(), s.bucket, s3Key, file, fileInfo.Size(), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to S3: %w", err)
	}

	return nil
}
