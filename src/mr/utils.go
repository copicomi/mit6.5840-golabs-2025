package mr

import (
	"log"
	"os"
	"path/filepath"
	"regexp"
)

func RemoveFiles(pattern string, path string) error {
	regex, err := regexp.Compile(pattern)
	if err != nil {
		log.Fatalf("cannot compile regex")
		return err
	}
	
	files, err := os.ReadDir(path)
	if err != nil {
		log.Fatalf("cannot read dir")
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filename := file.Name()
		if regex.MatchString(filename) {
			filePath := filepath.Join(path, file.Name())
			err := os.Remove(filePath)
			if err != nil {
				return err
			}

		}
	}
	return nil

}
func GetFiles(pattern string, path string) (filelist []*os.File, err error){
	regex, err := regexp.Compile(pattern)
	if err != nil {
		log.Fatalf("cannot compile regex")
		return nil, err
	}
	
	files, err := os.ReadDir(path)
	if err != nil {
		log.Fatalf("cannot read dir")
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filename := file.Name()
		if regex.MatchString(filename) {
			filePath := filepath.Join(path, file.Name())
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatalf("cannot open file")
				return nil, err
			}
			filelist = append(filelist, file)
		}
	}
	return filelist, nil

}