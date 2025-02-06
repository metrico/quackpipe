package service

import "golang.org/x/sync/semaphore"

type combinedMergeService struct {
	from mergeService
	to   mergeService
}

var sem = semaphore.NewWeighted(5)

func (c *combinedMergeService) GetFilesToMerge(iteration int) ([]FileDesc, error) {
	return c.from.GetFilesToMerge(iteration)
}

func (c *combinedMergeService) PlanMerge(descs []FileDesc, i int64, i2 int) []PlanMerge {
	return c.from.PlanMerge(descs, i, i2)
}

func (c *combinedMergeService) DoMerge(merges []PlanMerge) error {
	return c.from.DoMerge(merges)
}

func (c *combinedMergeService) UploadTmp(absolutePathFrom string, absolutePathTo string) error {
	return c.to.UploadTmp(absolutePathFrom, absolutePathTo)
}

func (c *combinedMergeService) Drop(files []string) {
	c.from.Drop(files)
}

func (c *combinedMergeService) DropTmp(files []string) {
	c.from.DropTmp(files)
}

func (c *combinedMergeService) TmpDir() string {
	return c.from.TmpDir()
}

func (c *combinedMergeService) FromDataDir() string {
	return c.from.DataDir()
}

func (c *combinedMergeService) ToDataDir() string {
	return c.to.DataDir()
}
