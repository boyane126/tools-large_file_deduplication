package conf

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/boyane126/tools/large_file_deduplication/utils"
)

type Config struct {
	ODir       string `json:"o_dir"`
	QcDir      string `json:"qc_dir"`
	BurstSize  int64  `json:"burst_size"`
	OutLineNum int64  `json:"out_line_num"`
}

var ObjConfig *Config

func init() {
	ObjConfig = &Config{}
	ObjConfig.initConf()
}

// 读取配置文件
func (c *Config) initConf() {
	confDir := "/conf.json"
	if utils.PathExists(confDir) == false {
		fmt.Println("配置文件不存在")
		os.Exit(1)
	}
	bytes, err := os.ReadFile("./conf.json")
	if err != nil {
		fmt.Println(err)
		return
	}
	if err = json.Unmarshal(bytes, c); err != nil {
		fmt.Println(err)
		return
	}

}
