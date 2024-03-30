package middleware

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

func CORSMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ctx.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		ctx.Writer.Header().Set("Access-Control-Max-Age", "86400")
		ctx.Writer.Header().Set("Access-Control-Allow-Methods", "*")
		ctx.Writer.Header().Set("Access-Control-Allow-Headers", "*")
		ctx.Writer.Header().Set("Access-Control-Allow-Credentials", "true")

		fmt.Println("[lxz debug] nonono")
		if ctx.Request.Method == http.MethodOptions {
			fmt.Println("[lxz debug] hahaha")
			ctx.AbortWithStatus(200)
		} else {
			ctx.Next()
		}
	}
}
