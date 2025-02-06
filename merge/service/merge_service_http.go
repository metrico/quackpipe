package service

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type httpMergeService struct {
	*fsMergeService
	baseUrl string
	path    string
	tmpPath string
}

type lsResponse struct {
	Size int
	MD5  string
}

func (s *httpMergeService) GetFilesToMerge(iteration int) ([]FileDesc, error) {
	res, err := http.Get(s.baseUrl)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get files: %w", http.StatusText(res.StatusCode))
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	body := make(map[string]lsResponse)
	err = json.Unmarshal(b, &body)
	if err != nil {
		return nil, err
	}
	suf := fmt.Sprintf("%d.parquet", iteration)
	var files []FileDesc
	for k, v := range body {
		if !strings.HasSuffix(k, suf) {
			continue
		}
		if !strings.HasPrefix(k, s.path+"/") {
			continue
		}
		files = append(files, FileDesc{
			name: k,
			size: int64(v.Size),
		})
	}
	return files, nil
}

func (s *httpMergeService) PlanMerge(descs []FileDesc, maxSize int64, iteration int) []PlanMerge {
	plans := s.fsMergeService.PlanMerge(descs, maxSize, iteration)
	for i := range plans {
		uid, _ := uuid.NewUUID()
		plans[i].To = fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1)
	}
	return plans
}

func (s *httpMergeService) merge(p PlanMerge) error {
	conn, err := getConn()
	if err != nil {
		return err
	}
	defer releaseConn(conn)
	g := errgroup.Group{}
	for i, file := range p.From {
		_p := file
		_i := i
		g.Go(func() error {
			url := fmt.Sprintf("%s/%s", s.baseUrl, strings.Trim(_p, "/"))
			c := http.Client{}
			res, err := c.Get(url)
			if err != nil {
				return err
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				return fmt.Errorf("failed to download file: %w", http.StatusText(res.StatusCode))
			}
			f, err := os.Create(path.Join(s.tmpPath, fmt.Sprintf("%d.%s", _i, p.To)))
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = io.Copy(f, res.Body)
			return err
		})
	}
	globEx := filepath.Join(s.tmpPath, fmt.Sprintf("*.%s", p.To))
	defer removeFilesByGlob(globEx)
	err = g.Wait()
	if err != nil {
		return err
	}
	// Create a temporary merged file
	tmpFilePath := filepath.Join(s.tmpPath, p.To)

	createTableSQL := fmt.Sprintf(
		`COPY(SELECT * FROM read_parquet_mergetree(ARRAY['%s'], '%s'))TO '%s' (FORMAT 'parquet')`,
		escapeString(globEx),
		strings.Join(s.table.OrderBy, ","), tmpFilePath)
	fmt.Println(createTableSQL)
	_, err = conn.Exec(createTableSQL)
	return err
}

func (s *httpMergeService) DoMerge(merges []PlanMerge) error {
	return s.doMerge(merges, s.merge)
}

func (s *httpMergeService) UploadTmp(absolutePathFrom string, absolutePathTo string) error {
	f, err := os.Open(absolutePathFrom)
	if err != nil {
		return err
	}

	res, err := http.Post(
		s.baseUrl+"/"+strings.Trim(absolutePathTo, "/"), "application/octet-stream", f)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to upload file: %w", http.StatusText(res.StatusCode))
	}
	return err
}

func (s *httpMergeService) Drop(files []string) {
	for _, file := range files {
		req, err := http.NewRequest(http.MethodDelete, s.baseUrl+"/"+strings.Trim(file, "/"), nil)
		if err != nil {
			continue
		}
		client := &http.Client{}
		res, err := client.Do(req)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if res.StatusCode != http.StatusOK {
			res.Body.Close()
			fmt.Printf("Failed to delete file %s: %s\n", file, http.StatusText(res.StatusCode))
			continue
		}
		res.Body.Close()
	}
}

func (s *httpMergeService) TmpDir() string {
	return s.tmpPath
}

func (s *httpMergeService) DataDir() string {
	return ""
}

func (s *httpMergeService) Join(parts ...string) string {
	return strings.Join(parts, "/")
}

func (s *httpMergeService) DropTmp(files []string) {
	for _, file := range files {
		os.Remove(filepath.Join(s.TmpDir(), file))
	}
}

func removeFilesByGlob(pattern string) error {
	// Find all files matching the pattern
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("error finding files: %w", err)
	}

	// Remove each file
	for _, match := range matches {
		if err := os.Remove(match); err != nil {
			return fmt.Errorf("error removing file %s: %w", match, err)
		}
	}

	return nil
}
