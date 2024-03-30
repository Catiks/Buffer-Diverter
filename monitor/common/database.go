package common

import (
	"database/sql"

	_ "gitee.com/opengauss/openGauss-connector-go-pq"
)

var DB *sql.DB

func InitDB() *sql.DB {
	connStr := "host=127.0.0.1 port=5432 user=dell password=Gauss@123 dbname=postgres sslmode=disable"
	sqlDB, errOpenGauss := sql.Open("opengauss", connStr)

	if errOpenGauss != nil {
		panic("failed to connect database, errOpenGauss" + errOpenGauss.Error())
	}

	// var date string
	// err = db.QueryRow("select current_date ").Scan(&date)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println("select current_date ", date)
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
	// u1 := user{}
	// err1 := db.QueryRow("select * from test1").Scan(&u1.id, &u1.amount, &u1.name)
	// if err1 != nil {
	// 	fmt.Println("err1 ", err1)
	// }
	// fmt.Println("select * from test1 ", u1.id, u1.amount, u1.name)
	// u2 := user{}
	// err2 := db.QueryRow("select * from test1 where id = $1", "2").Scan(&u2.id, &u2.amount, &u2.name)
	// if err2 != nil {
	// 	fmt.Println("err2 ", err2)
	// }
	// fmt.Println("select * from test1 where id = 2", u2.id, u2.amount, u2.name)

	DB = sqlDB

	return sqlDB
}

func GetDB() *sql.DB {
	return DB
}
