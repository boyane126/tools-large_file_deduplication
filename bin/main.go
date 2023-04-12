package main

import (
	"github.com/boyane126/tools/large_file_deduplication/conf"
	"github.com/boyane126/tools/large_file_deduplication/internal"
)

/**
1.将文件切分为小临时文件（每个1G）；切分方法：(电话号码) % (临时文件数量) ==> 这样相同的电话号码就会存入同一个临时文件
2.遍历所有临时文件，每个文件调用去重函数。将值写入结果文件。

*/

func main() {
	dir := conf.ObjConfig.ODir
	blockSize := conf.ObjConfig.BurstSize
	outLineNum := conf.ObjConfig.OutLineNum
	resultFile := conf.ObjConfig.QcDir

	qc := internal.NewOriginData(internal.DefaultLine{}, dir, resultFile, blockSize, outLineNum)
	qc.Run()
}
