package main

import (
	"ODBCtest/common"

	_ "gitee.com/opengauss/openGauss-connector-go-pq"
	"github.com/gin-gonic/gin"
)

// type user struct {
// 	id     int
// 	name   string
// 	amount int
// }

func main() {
	common.InitDB()
	// db := common.InitDB()
	// defer func() {
	// 	sqlDB, err := db.DB()
	// 	if err != nil {
	// 		log.Println("sql close err" + err.Error())
	// 	}
	// 	sqlDB.Close()
	// }()
	r := gin.Default()
	r = CollectRoute(r)
	panic(r.Run(":8877")) // 监听并在 0.0.0.0:8080 上启动服务

}
