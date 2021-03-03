package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// fsimage.csv里一行里各列的内容
const (
	PATH = iota
	REPLICATION
	MTIME
	ATIME
	BLOCKSIZE
	BLOCKSCOUNT
	FILESIZE
	NSQUOTA
	DSQUOTA
	PERMISSION
	USERNAME
	GROUPNAME
)

var inputPath string = "./fsimage.csv"
var outputPath string = "./output.csv"
const TIME_LAYOUT = "2006-01-02 15:04:05"
var CURRENTTIME string = time.Now().Format(TIME_LAYOUT)

/* 
 * 返回处理好的字符串
 *
 * 处理前：
 *   原始行 = Path|Replication|MTime|ATime|BlockSize|BlocksCount|FileSize|NSQUOTA|DSQUOTA|Permission|UserName|GroupName
 *
 * 处理后：
 *   原始行|ParentPath|MTimeTillNowDistribution|ATimeTillNowDistribution|FileSizeDistribution|PathDepth|CurrentTime|partPath|partDay
*/
func parseOneLine(line []string) (string, error) {
	parentPath, pathDepth, err := parsePath(line[PATH])
	if err != nil {
		log.Printf("parse path fail, path is %s\n", line[PATH])
		return "", err
	}

	var mtimeDistri int
	line[MTIME], mtimeDistri, err = parseTime(line[MTIME])
	if err != nil {
		log.Printf("parse mtime fail, mtime is %s\n", line[MTIME])
		return "", err
	}	

	var atimeDistri int
	line[ATIME], atimeDistri, err = parseTime(line[ATIME])
	if err != nil {
		log.Printf("parse atime fail, atime is %s\n", line[ATIME])
		return "", err
	}

	sizeDistri, err := parseBlockSize(line[BLOCKSIZE])
	if err != nil {
		log.Printf("parse block size fail, block size is %s\n", line[BLOCKSIZE])
		return "", err
	}

	line = append(line, parentPath, strconv.Itoa(atimeDistri), 
	strconv.Itoa(mtimeDistri), strconv.Itoa(sizeDistri), 
	strconv.Itoa(pathDepth), CURRENTTIME, parentPath, 
	strings.Split(CURRENTTIME, " ")[0], "\n")
	return strings.Join(line, "|"), err
}

/*
 * 入参：待处理的路径
 * 返回值：
 *   路径的父目录
 *   目录的深度
*/
func parsePath(path string) (string, int, error) {
	if path == "" {
		return "", -1, errors.New("path is empty")
	}

	lastSlashIndex := strings.LastIndex(path, "/")
	if lastSlashIndex == -1 {
		return "", -1, errors.New("can't find /")
	} else if lastSlashIndex == 0 { // 根目录
		return "", 0, nil
	} else {
		return path[:lastSlashIndex], strings.Count(path[:lastSlashIndex], "/")-1, nil
	}
}

/*
* 入参：时间
* 返回值：
*   1) 为时间添加秒的部分:00，以符合impala的时间格式
*   2) 根据距今的天数，进行分类，分为以下天数：<1, <7, <14, <30, <180, <360, <720, >720(721)
*   天数向上取整，如，不满1天的，均返回1。大于720天的，返回721
*/
func parseTime(date string) (string, int, error) {
	if date == "" {
		return "", -1, errors.New("time is empty")
	}

	formatedTimeString := date + ":00"
	pre, err := time.Parse(TIME_LAYOUT, formatedTimeString)
	if err != nil {
		fmt.Println("err ", err)
		return "", 0, err
	}
	now := time.Now()
	duration := now.Sub(pre) 
	day := int(duration.Hours() / 24)

	if day <= 1 {
		day = 1
	} else if day <= 7 {
		day = 7
	} else if day <= 14 {
		day = 14
	} else if day <= 30 {
		day = 30
	} else if day <= 180 {
		day = 180
	} else if day <= 360 {
		day = 360
	} else if day <= 720 {
		day = 720
	} else {
		day = 721
	}

	return formatedTimeString, day, nil
}

/*
 * 入参：文件的大小（字节）
 * 返回值：文件额大小，按照以下大小分区：1MB, 64MB, 128MB, 1G, 10G, 50G, >50G
*/
func parseBlockSize(size string) (int, error) {
	if size == "" {
		return -1, errors.New("size is empty")
	}

	sizeInt, err := strconv.ParseInt(size, 10, 64)
	sizeInt /= 1048576

	if sizeInt <= 1 {
		sizeInt = 1
	} else if sizeInt <= 64 {
		sizeInt = 64
	} else if sizeInt <= 128 {
		sizeInt = 128
	} else if sizeInt <= 1024 {
		sizeInt = 1024
	} else if sizeInt <= 10240 {
		sizeInt = 10240
	} else if sizeInt <= 51200 {
		sizeInt = 51200
	} else {
		sizeInt = 51201
	}

	return int(sizeInt), err
}

func main() {
	// 打开待处理的文件
	inputFile, err := os.Open(inputPath)
	if err != nil {
		log.Fatal(err)
	}
	defer inputFile.Close()

	// 打开存放结果的文件
	outputFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	scanner := bufio.NewScanner(inputFile)

	var count int
	startTime := time.Now()

	// 一行一行读入文件进行处理
	for scanner.Scan() {
		count++

		line := scanner.Text()
		slicedLine := strings.Split(line, "|")
		parsedLine, _ := parseOneLine(slicedLine)

		// 打印处理10w个文件所花费的时间
		if count == 100000 {
			endTime := time.Now()
			fmt.Printf("Convert %d line taken %v\n", count, endTime.Sub(startTime))
			startTime = endTime
			count = 0
		}
		writer.WriteString(parsedLine)
	}
	writer.Flush()
}
