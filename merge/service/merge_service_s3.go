package service

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"os"
	"path/filepath"
	"quackpipe/service/db"
	"strings"
)

type s3MergeService struct {
	fsMergeService
	s3Config
	tmpPath string
}

func (s *s3MergeService) GetFilesToMerge(iteration int) ([]FileDesc, error) {
	minioClient, err := minio.New(s.url, &minio.Options{
		Creds:  credentials.NewStaticV4(s.key, s.secret, ""),
		Secure: s.secure,
		Region: s.region,
	})
	if err != nil {
		return nil, err
	}
	var res []FileDesc
	snapshot := ""
	c := minioClient.ListObjects(context.Background(), s.bucket,
		minio.ListObjectsOptions{
			Prefix:     s.s3Config.path,
			MaxKeys:    1000,
			StartAfter: snapshot,
		})
	snapshot = "a"
	suffix := fmt.Sprintf("%d.parquet", iteration)
	for snapshot != "" {
		snapshot = ""
		for obj := range c {
			if !strings.HasSuffix(obj.Key, suffix) {
				continue
			}
			res = append(res, FileDesc{
				name: obj.Key,
				size: obj.Size,
			})
			snapshot = obj.Key
		}
		if snapshot == "" {
			break
		}
		c = minioClient.ListObjects(context.Background(), s.bucket,
			minio.ListObjectsOptions{
				Prefix:     s.s3Config.path,
				MaxKeys:    1000,
				StartAfter: snapshot,
			})
	}
	return nil, nil
}

func (s *s3MergeService) PlanMerge(descs []FileDesc, maxSize int64, iteration int) []PlanMerge {
	return s.fsMergeService.PlanMerge(descs, maxSize, iteration)
}

func (s *s3MergeService) merge(p PlanMerge) error {
	// Create a temporary merged file
	tmpFilePath := filepath.Join(s.tmpPath, p.To)

	conn, err := db.ConnectDuckDB("?allow_unsigned_extensions=1")
	if err != nil {
		return err
	}
	_, err = conn.Exec("INSTALL chsql FROM community")
	if err != nil {
		fmt.Println("Error loading chsql extension: ", err)
		return err
	}
	_, err = conn.Exec("LOAD chsql")
	if err != nil {
		fmt.Println("Error loading chsql extension: ", err)
		return err
	}
	defer conn.Close()

	from := make([]string, len(p.From))
	for i, file := range p.From {
		from[i] = fmt.Sprintf("s3://%s/%s", s.bucket, file)
	}

	_, err = conn.Exec(`CREATE SECRET (
  TYPE S3, 
  KEY_ID ?, 
  SECRET ?, 
  ENDPOINT ?, 
  USE_SSL ?, 
  URL_STYLE 'path'
);`, s.key, s.secret, s.url, s.secure)
	if err != nil {
		return err
	}
	createTableSQL := fmt.Sprintf(
		`COPY(SELECT * FROM read_parquet_mergetree(ARRAY['%s'], '%s'))TO '%s' (FORMAT 'parquet')`,
		strings.Join(p.From, "','"),
		strings.Join(s.table.OrderBy, ","), tmpFilePath)
	_, err = conn.Exec(createTableSQL)
	if err != nil {
		fmt.Println("Error read_parquet_mergetree: ", err)
		return err
	}
	defer os.Remove(tmpFilePath)

	saveSvc := s3SaveService{
		fsSaveService: fsSaveService{},
		s3Config:      s.s3Config,
	}

	return saveSvc.uploadToS3(tmpFilePath)
}

func (s *s3MergeService) DoMerge(merges []PlanMerge) error {
	return s.doMerge(merges, s.merge)
}
