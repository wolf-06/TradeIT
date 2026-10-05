package main

import (
	"TradeIT/internal/database"
	"TradeIT/internal/models"
	"TradeIT/services/user-order-service/controller"
	"fmt"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("starting User-Order Service on :8000")

	if err := godotenv.Load(); err != nil {
		log.Fatalln("Error in loading the env file: ", err)
	}

	// Fail fast instead of signing/verifying JWTs with an empty key (anyone could
	// forge a token for any user id in that case).
	if os.Getenv("jwtkey") == "" {
		log.Fatalln("jwtkey is not set: refusing to start with an empty JWT signing key")
	}

	router := gin.Default()
	database.InitDb()
	if database.SetDB() == nil {
		// Without this the service starts happily and every request then panics on a
		// nil *gorm.DB (e.g. when a db_* variable in the environment shadows .env).
		log.Fatalln("cannot connect to the database - check db_host/db_port/db_user/db_name/db_passwd and that it is running")
	}
	models.InitTables()

	userController := controller.InitUserController()
	userController.InitUserControllerRoutes(router)

	orderController := controller.InitOrderController()
	orderController.InitOrderRoutes(router)

	router.Run(":8000")
}
