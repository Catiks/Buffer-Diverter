package main

import (
	"ODBCtest/common"
	"ODBCtest/middleware"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type users struct {
	Id     int    `json:"id" db:"usesysid"`
	Name   string `json:"name" db:"usename"`
	Buffer int    `json:"buffer" db:"buffer"`
}

func Info(ctx *gin.Context) {
	db := common.GetDB()
	var num1, num2, num3 float64
	var buffer_number int
	rows, err := db.Query("select usename, usesysid from pg_user")
	if err != nil {
		log.Println(err)
	}
	results := []users{} // creating empty slice
	defer rows.Close()
	for rows.Next() {
		user := users{} // creating new struct for every row
		err = rows.Scan(&user.Name, &user.Id)
		fmt.Println(rows.Columns()) // printing result

		if err != nil {
			log.Println(err)
		}
		err = db.QueryRow(fmt.Sprintf("SELECT get_user_buffer_number(%d)", user.Id)).Scan(&buffer_number)
		if err != nil {
			log.Fatal(err)
		}
		user.Buffer = buffer_number
		results = append(results, user) // add new row information
	}
	fmt.Println(results) // printing result

	err = db.QueryRow("SELECT get_user_buffer_number(10)").Scan(&num1)
	if err != nil {
		log.Fatal(err)
	}
	err = db.QueryRow("SELECT get_user_buffer_number(16384)").Scan(&num2)
	if err != nil {
		log.Fatal(err)
	}
	err = db.QueryRow("SELECT get_user_buffer_number(16389)").Scan(&num3)
	if err != nil {
		log.Fatal(err)
	}
	// err = db.QueryRow("SELECT total_buffer()").Scan(&num)
	// fmt.Println("SELECT get_user_buffer_number(10)", num1/1000)
	// rows, errQuery := db.Query("select * from test1")
	// if errQuery != nil {
	// 	fmt.Println("errQuery ", errQuery)
	// }
	// defer rows.Close()
	// i := 0
	// for rows.Next() {
	// 	u := user{}
	// 	if errGetRows := rows.Scan(&u.id, &u.amount, &u.name); errGetRows == nil {
	// 		fmt.Printf("age = %v, amount = %v, name = %v\n", u.id, u.amount, u.name)
	// 	} else {
	// 		fmt.Println("errGetRows ", errGetRows)
	// 	}
	// 	i++
	// }
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK, "data": gin.H{"users": results}})
}

func openStrict(ctx *gin.Context) {
	db := common.GetDB()
	var s string
	err := db.QueryRow("SELECT set_buffer_strict_mode(true)").Scan(&s)
	if err != nil {
		log.Fatal(err)
	}
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK, "data": gin.H{"result1": s}})
}

func closeStrict(ctx *gin.Context) {
	db := common.GetDB()
	var s string
	err := db.QueryRow("SELECT set_buffer_strict_mode(false)").Scan(&s)
	if err != nil {
		log.Fatal(err)
	}
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK, "data": gin.H{"result1": s}})
}

func gettotal(ctx *gin.Context) {
	db := common.GetDB()
	var s int
	err := db.QueryRow("SELECT get_total_buffer_number()").Scan(&s)
	if err != nil {
		log.Fatal(err)
	}
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK, "data": gin.H{"result1": s}})
}

func setUserBuffer(ctx *gin.Context) {
	db := common.GetDB()
	user := ctx.Query("user")
	number := ctx.Query("number")
	qrow := fmt.Sprintf("SELECT set_user_buffer_number(%v, %v)", user, number)
	var s int
	err := db.QueryRow(qrow).Scan(&s)
	if err != nil {
		log.Fatal(err)
	}
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK, "data": gin.H{"result1": s}})
}

func getUserBuffer(ctx *gin.Context) {
	db := common.GetDB()
	user := ctx.Query("user")
	qrow := fmt.Sprintf("SELECT get_user_buffer_number(%v)", user)
	// fmt.Println(qrow, user)
	var s int
	err := db.QueryRow(qrow).Scan(&s)
	if err != nil {
		log.Fatal(err)
	}
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK, "data": gin.H{"result1": s}})
}

func getUserBufferBought(ctx *gin.Context) {
	db := common.GetDB()
	user := ctx.Query("user")
	qrow := fmt.Sprintf("SELECT get_user_bought_buffer_number(%v)", user)
	// fmt.Println(qrow, user)
	var s int
	err := db.QueryRow(qrow).Scan(&s)
	if err != nil {
		log.Fatal(err)
	}
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK, "data": gin.H{"result1": s}})
}
func Root(ctx *gin.Context) {
	log.Println("Handling root request...")
	ctx.File("templates/main.html")

	// ctx.HTML(http.StatusOK, "templates/main.html", nil)
}
func CollectRoute(r *gin.Engine) *gin.Engine {
	r.Use(middleware.CORSMiddleware())
	r.GET("/info", Info)
	r.GET("/openStrict", openStrict)
	r.GET("/closeStrict", closeStrict)
	r.GET("/gettotal", gettotal)
	r.GET("/setUserBuffer", setUserBuffer)
	r.GET("/getUserBuffer", getUserBuffer)
	r.GET("/getUserBufferBought", getUserBufferBought)
	// r.LoadHTMLFiles("templates/main.html")

	r.GET("/", Root)

	// get_total_buffer_number()
	// set_user_buffer_number(id)
	return r
}
