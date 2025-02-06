package service

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/sync/errgroup"
	"os"
	"path/filepath"
	"quackpipe/service/db"
	"strings"
	"sync"
)

var conn = sync.Pool{New: func() any {
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
	return conn
}}

func getConn() (*sql.DB, error) {
	res := conn.Get()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res.(*sql.DB), nil
}

func releaseConn(_conn *sql.DB) {
	conn.Put(_conn)
}

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
			Prefix:     s.s3Config.path + "/",
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
			name := strings.TrimPrefix(obj.Key, s.s3Config.path+"/")
			res = append(res, FileDesc{
				name: name,
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
	return res, nil
}

func (s *s3MergeService) PlanMerge(descs []FileDesc, maxSize int64, iteration int) []PlanMerge {
	plans := s.fsMergeService.PlanMerge(descs, maxSize, iteration)
	for i := range plans {
		uid, _ := uuid.NewUUID()
		plans[i].To = fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1)
	}
	return plans
}

func escapeString(s string) string {
	return strings.Replace(s, "'", "''", -1)
}

func (s *s3MergeService) merge(p PlanMerge) error {

	conn, err := getConn()
	if err != nil {
		return err
	}
	defer releaseConn(conn)

	tmpFilePath := filepath.Join(s.tmpPath, p.To)

	from := make([]string, len(p.From))
	for i, file := range p.From {
		from[i] = fmt.Sprintf("s3://%s/%s/%s", s.bucket, s.s3Config.path, file)
	}

	createTableSQL := fmt.Sprintf(`
SET s3_access_key_id='%s';
SET s3_secret_access_key='%s';
SET s3_endpoint='%s';
SET s3_use_ssl=%t;
SET s3_url_style='path';
COPY(
   SELECT * FROM read_parquet_mergetree(ARRAY['%s'], '%s')
)TO '%s' (FORMAT 'parquet')`,
		escapeString(s.key),
		escapeString(s.secret),
		escapeString(s.url),
		s.secure,
		strings.Join(from, "','"),
		strings.Join(s.table.OrderBy, ","), tmpFilePath)
	fmt.Println(createTableSQL)
	_, err = conn.Exec(createTableSQL)
	return err
}

func (s *s3MergeService) DoMerge(merges []PlanMerge) error {
	return s.doMerge(merges, s.merge)
}

func (s *s3MergeService) UploadTmp(absolutePathFrom string, absolutePathTo string) error {
	defer os.Remove(absolutePathFrom)

	saveSvc := s3SaveService{
		fsSaveService: fsSaveService{},
		s3Config:      s.s3Config,
	}

	err := saveSvc.uploadToS3Ex(absolutePathFrom, absolutePathTo)
	return err
}

func (s *s3MergeService) Drop(files []string) {
	saveSvc := s3SaveService{
		fsSaveService: fsSaveService{},
		s3Config:      s.s3Config,
	}

	minioClient, err := saveSvc.createMinioClient()
	if err != nil {
		fmt.Println("Error creating minio client: ", err)
		return
	}
	eg := errgroup.Group{}
	for _, f := range files {
		_f := s.Join(s.s3Config.path, f)
		eg.Go(func() error {
			return minioClient.RemoveObject(context.Background(), s.bucket, _f, minio.RemoveObjectOptions{})
		})
	}
	err = eg.Wait()
	if err != nil {
		fmt.Println("Error removing objects from S3: ", err)
	}
}

func (s *s3MergeService) TmpDir() string {
	return s.tmpPath
}

func (s *s3MergeService) DataDir() string {
	return s.s3Config.path
}

func (s *s3MergeService) Join(parts ...string) string {
	return strings.Join(parts, "/")
}

func (s *s3MergeService) DropTmp(files []string) {
	for _, file := range files {
		os.Remove(filepath.Join(s.TmpDir(), file))
	}
}
