package service

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"

	"github.com/google/uuid"
	"github.com/tidwall/btree"
)

type httpSaveService struct {
	fsSaveService
	httpUrl string
	tmpPath string
}

func (s *httpSaveService) Save(fields [][2]string, data map[string]any, index *btree.BTreeG[int32]) error {
	uid, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	tmpFileName := path.Join(s.tmpPath, uid.String()+".1.parquet")
	err = s.saveTmpFile(tmpFileName, fields, data, index)
	if err != nil {
		return err
	}
	defer os.Remove(tmpFileName)

	return s.uploadToHTTP(tmpFileName)
}

func (s *httpSaveService) uploadToHTTP(filePath string) error {
	c := &http.Client{}
	fileName := path.Base(filePath)
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()
	req, err := http.NewRequest("POST", s.httpUrl+"/"+fileName, f)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	res, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("unexpected status code %d: %s", res.StatusCode, string(b))
	}
	return nil
}
