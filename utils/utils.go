package utils

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

func GetFileSize(dir string) (int64, error) {
	fi, err := os.Stat(dir)
	if err != nil {
		return 0, err
	}

	return fi.Size(), nil
}

// 读取文件行数
func ReadLine(dir string) int {
	file, err := os.Open(dir)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	defer file.Close()

	fd := bufio.NewReader(file)
	count := 0
	for {
		_, err := fd.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		count++
	}
	fmt.Println("文件共【", count, "】行")
	return count
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return true
	}

	return false
}

func HashCode(s string) int {
	v := int(crc32.ChecksumIEEE([]byte(s)))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	// v == MinInt
	return 0
}
