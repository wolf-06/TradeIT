package services

import (
	"TradeIT/internal/database"
	"TradeIT/internal/middleware"
	"TradeIT/internal/models"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type UserService struct {
	db *gorm.DB
}

type userData struct {
	Name   string `json:"name" binding:"required"`
	Email  string `json:"email" binding:"required,email"`
	Passwd string `json:"password" binding:"required,min=6"`
}

func InitUserService() *UserService {
	return &UserService{
		db: database.SetDB(),
	}
}

func (us *UserService) RegisterUserService(c *gin.Context) uint64 {
	var temp userData
	if err := c.ShouldBindJSON(&temp); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return 0
	}

	// One account per email address (the credentials table also has a unique index on
	// email, this just turns the constraint violation into a clear response).
	var existing int64
	if err := us.db.Model(&models.Credential{}).Where("email = ?", temp.Email).Count(&existing).Error; err != nil {
		log.Println("error while checking for an existing email: ", err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "unable to create user"})
		return 0
	}
	if existing > 0 {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "email is already registered"})
		return 0
	}

	fmt.Println(temp)
	user := models.User{ //creates the user entry in user details
		Name:  temp.Name,
		Email: temp.Email,
		Funds: 10000.00,
	}
	// No Id is set on purpose: the database sequence assigns it, so two concurrent
	// registrations can no longer collide on a random id. Rows written by the previous
	// random-id generator still occupy ids in the 1000-9999 range, so retry if the
	// sequence happens to land on one of them (nextval has already moved past it).
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		err = us.db.Create(&user).Error
		if err == nil || !strings.Contains(err.Error(), "duplicate key") || attempt == 3 {
			break
		}
		log.Println("primary key conflict while creating a user, retrying: ", err)
	}
	if err != nil {
		log.Println("error in creating user entry: ", err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "unable to create user"})
		return 0
	}

	errr := us.db.Create(&models.Credential{ //creates the credential entry for authorisations
		Id:     user.Id,
		Email:  temp.Email,
		Passwd: temp.Passwd,
		Token:  "",
	}).Error
	if errr != nil {
		log.Println("error in creating credential entry: ", errr)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "unable to create user credentials"})
		return 0
	}
	return user.Id
}

func (us *UserService) LoginUserService(c *gin.Context) string {
	var Cred middleware.LoginCred
	if err := c.ShouldBindJSON(&Cred); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return ""
	}

	status, token, errr := middleware.LoginValidator(us.db, Cred.Email, Cred.Passwd)

	if status && errr == "" {
		return token
	} else {
		return errr
	}
}

func (us *UserService) UpdateUserEmailService(c *gin.Context) string {
	var param middleware.UpdateEmailParameters
	if err := c.BindJSON(&param); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return ""
	}
	// A caller may only change their own account, so the id from the token wins over
	// whatever id was supplied in the body.
	user_id, _ := c.Get("userid")
	param.Id = strconv.FormatUint(uint64(user_id.(float64)), 10)

	return middleware.EmailUpdater(us.db, param)
}

func (us *UserService) UpdateUserPasswdService(c *gin.Context) string {
	var param middleware.UpdatePasswdParameters
	if err := c.BindJSON(&param); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return ""
	}
	// A caller may only change their own password, so the id from the token wins over
	// whatever id was supplied in the body.
	user_id, _ := c.Get("userid")
	param.Id = strconv.FormatUint(uint64(user_id.(float64)), 10)

	return middleware.PasswdUpdator(us.db, param)
}

func (us *UserService) GetFundsService(c *gin.Context) float32 {
	userid, _ := c.Get("userid")
	x := userid.(float64)
	return middleware.FundsInfo(us.db, x)
}
