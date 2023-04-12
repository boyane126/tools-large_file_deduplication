package internal

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sync"
	"time"

	"github.com/boyane126/tools/large_file_deduplication/utils"
)

// LineHand 处理数据内容接口
type LineHand interface {
	Hand(line []byte) (ret int, err error)
}

// DefaultLine 默认行内容处理器 （行hashcode处理）
type DefaultLine struct{}

func (l DefaultLine) Hand(line []byte) (ret int, err error) {
	lineLen := len(line)
	if lineLen == 0 {
		err = fmt.Errorf("数据错误:%s", string(line))
		return
	}
	ret = utils.HashCode(string(line))
	return
}

// OriginData 去重对象
type OriginData struct {
	dir        string // 去重文件
	fileSize   int64  // 文件大小
	blockSize  int64
	tempAmount int
	tempDir    string
	tempPre    string
	outLineNum int64  // 扔掉开头行数
	resultFile string // 结果文件
	hand       LineHand
}

func NewOriginData(hand LineHand, dir, resultFile string, blockSize, outLineNum int64) *OriginData {
	fileSize, _ := utils.GetFileSize(dir)
	tempNum := int(math.Floor(float64(fileSize / blockSize)))
	if tempNum == 0 {
		tempNum = 1
	}

	return &OriginData{
		dir:        dir,
		fileSize:   fileSize,
		blockSize:  blockSize,
		tempAmount: tempNum,
		tempDir:    ".temp",
		tempPre:    "temp-",
		outLineNum: outLineNum,
		resultFile: resultFile,
		hand:       hand,
	}
}

func (o *OriginData) Run() {
	o.ready()
	o.cutFile()
	o.makeupFiles()
	o.deleteTempFiles()
}

// 准备
func (o *OriginData) ready() {
	// 临时文件夹是否存在
	if utils.PathExists(o.tempDir) == true {
		fmt.Println("创建临时文件夹")
		os.Mkdir(o.tempDir, os.ModePerm)
	}
}

// 将大文件拆分为若干小文件
func (o *OriginData) cutFile() {
	st := time.Now()
	ch := make(chan struct{})
	defer func() {
		ch <- struct{}{}
		log.Println("文件切割耗时：", time.Now().Sub(st).Seconds(), "s")
	}()

	// 创建临时文件
	fileIO := make([]*os.File, o.tempAmount)
	for i := 0; i < o.tempAmount; i++ {
		outputFile, err := os.Create(fmt.Sprintf("%s/%s%d.temp", o.tempDir, o.tempPre, i))
		if err != nil {
			panic(err)
		}
		fileIO[i] = outputFile
		defer fileIO[i].Close()
	}

	// 打开源文件
	oFileHand, err := os.Open(o.dir)
	if err != nil {
		panic(err)
	}
	defer oFileHand.Close()

	// 打印写入数据行数
	var writeLine int64
	go o.logPrint(ch, &writeLine)

	lineReader := bufio.NewReader(oFileHand)
	for i := 0; i < int(o.outLineNum); i++ {
		lineReader.ReadLine()
	}

	var wg sync.WaitGroup
	for {
		line, err := lineReader.ReadBytes('\n')
		if err == io.EOF {
			break
		}

		// 数据处理
		only, err := o.lineHand(line)
		if err != nil {
			log.Println(err, string(line))
			continue
		}

		// 插入数据处理
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 将读取的行数据进行hashcode存入对应临时文件
			fileN := only % o.tempAmount
			if _, err = fileIO[fileN].Write(line); err != nil {
				log.Println(err, string(line))
				return
			}
		}()

		writeLine += 1
	}

	log.Println("等待中...")
	wg.Wait()
	log.Println("拆分文件完成")
}

func (o *OriginData) logPrint(ch chan struct{}, writeLine *int64) {
	for {
		select {
		case <-ch:
			log.Println("停止读取数据行")
			return
		default:
			log.Println("读取数据行数：", writeLine)
			time.Sleep(time.Second * 2)
		}
	}
}

// 遍历小文件，去重后组装
func (o *OriginData) makeupFiles() {
	dataFile, err := os.Create(o.resultFile)
	if err != nil {
		log.Println("创建最终文件时发生错误:", err)
		return
	}

	st := time.Now()
	bufWriter := bufio.NewWriter(dataFile)
	defer func() {
		bufWriter.Flush()
		dataFile.Close()
		fmt.Println("文件写入耗时：", time.Now().Sub(st).Seconds(), "s")
	}()

	fileIO := make([]*os.File, o.tempAmount)
	for i := 0; i < o.tempAmount; i++ {
		outputFile, err := os.Open(fmt.Sprintf("%s/%s%d.temp", o.tempDir, o.tempPre, i))
		if err != nil {
			log.Println(err)
			return
		}

		fileIO[i] = outputFile
		defer fileIO[i].Close()
	}

	// 对每个文件去重 然后存入最终文件
	for _, file := range fileIO {
		log.Println(fmt.Sprintf("开始去重 %s", file.Name()))
		// 对每个文件去重
		data := o.qcHand(file)
		log.Println(fmt.Sprintf("去重完成 %s", file.Name()))

		for k, v := range data {
			_, err = bufWriter.Write(v)
			if err != nil {
				log.Println(err)
				return
			}
			delete(data, k)
		}

		fmt.Println(fmt.Sprintf("写入文件完成 %s", file.Name()))
	}
}

// 删除临时文件
func (o *OriginData) deleteTempFiles() {
	defer func() {
		log.Println("删除临时文件成功")
	}()
	if err := os.RemoveAll(o.tempDir); err != nil {
		log.Println(err)
	}
}

// 去重处理
func (o *OriginData) qcHand(f *os.File) map[int][]byte {
	f.Seek(0, 0)
	lineReader := bufio.NewReader(f)
	var maps map[int][]byte
	maps = make(map[int][]byte)
	for {
		line, err := lineReader.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		key, err := o.lineHand(line)
		if err != nil {
			log.Println(err, string(line))
			continue
		}
		if _, ok := maps[key]; !ok {
			maps[key] = line
		}
	}

	return maps
}

// 处理数据内容
func (o *OriginData) lineHand(line []byte) (ret int, err error) {
	return o.hand.Hand(line)
}
