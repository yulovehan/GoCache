package router

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/youngyangyang04/KamaCache-Go/admin/controller"
)

func NewRouter(ctrl *controller.AdminController, dashboardPath string) *gin.Engine {
	r := gin.Default()
	r.Use(cors())

	if dashboardPath != "" {
		r.StaticFile("/", dashboardPath)
		r.StaticFile("/dashboard.html", dashboardPath)
	}

	api := r.Group("/api")
	{
		api.GET("/health", ctrl.Health)
		api.GET("/nodes", ctrl.Nodes)
		api.POST("/nodes", ctrl.AddNode)
		api.DELETE("/nodes", ctrl.DeleteNode)
		api.GET("/hash-ring", ctrl.HashRing)
		api.GET("/stats", ctrl.Stats)
		api.GET("/cache/query", ctrl.Query)
		api.POST("/cache/set", ctrl.Set)
		api.DELETE("/cache", ctrl.Delete)
	}

	return r
}

func cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type,Authorization")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}
