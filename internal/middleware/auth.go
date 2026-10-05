package middleware

import (
	"TradeIT/internal/models"
	"fmt"
	"log"
	"strconv"

	"gorm.io/gorm"
)

type UpdateEmailParameters struct {
	Id     string `json:"id" binding:"required"`
	Email  string `json:"email" binding:"required,email"`
	Passwd string `json:"password" binding:"required"`
}

type LoginCred struct {
	Email  string `json:"email" binding:"required,email"`
	Passwd string `json:"password" binding:"required"`
}

type UpdatePasswdParameters struct {
	Id        string `json:"id" binding:"required"`
	OldPasswd string `json:"password" binding:"required"`
	NewPasswd string `json:"newPassword" binding:"required,min=6"`
}

func LoginValidator(db *gorm.DB, email string, passwd string) (bool, string, string) {
	var Cred models.Credential
	err := db.Where("email= ?", email).Find(&Cred).Error
	if err != nil {
		// Returning a message instead of a log.Fatal: a database hiccup must not take
		// the whole service down.
		log.Println("Error in cred from tables ", err)
		return false, "", "internal server error"
	}

	if Cred.Email == email && Cred.Passwd == passwd {
		return true, CreateToken(Cred.Id), ""
	} else if Cred.Email == email && Cred.Passwd != passwd {
		return false, "", "check the credentials"
	} else {
		return false, "", "user doesn't exist "
	}
}

func PasswdValidator(db *gorm.DB, parameter UpdateEmailParameters) bool {
	var userCount models.Credential
	fmt.Println(parameter)
	id, _ := strconv.Atoi(parameter.Id)
	result := db.Where("id = ? AND passwd =?", id, parameter.Passwd).Find(&userCount)
	if result.Error != nil {
		log.Println("error in getting the creds from the db: ", result.Error)
		return false
	}
	if result.RowsAffected == 1 {
		fmt.Println(userCount)
		userCount.Email = parameter.Email
		fmt.Println(userCount)
		db.Save(&userCount)
		fmt.Println(userCount)
		return true
	} else {
		fmt.Printf("incorrect Passwd")
		return false
	}
}

func EmailUpdater(db *gorm.DB, parameter UpdateEmailParameters) string {
	if PasswdValidator(db, parameter) {
		var user models.User
		id, _ := strconv.Atoi(parameter.Id)
		if err := db.Where("Id = ?", id).Find(&user).Error; err != nil {
			log.Println("error in entry finding (mail updation): ", err)
			return "internal server error"
		}
		user.Email = parameter.Email
		if err := db.Save(&user).Error; err != nil {
			log.Println("error in saving the updates to database: ", err)
			return "internal server error"
		}
		return "value changed succesfully"
	} else {
		return "password incorrect"
	}
}

func PasswdUpdator(db *gorm.DB, parameters UpdatePasswdParameters) string {
	var authDetail models.Credential
	if err := db.Where("id = ? AND passwd = ?", parameters.Id, parameters.OldPasswd).Find(&authDetail).Error; err != nil {
		log.Println("Error in binding: ", err)
		return "internal server error"
	}
	if authDetail.Passwd == parameters.OldPasswd {
		authDetail.Passwd = parameters.NewPasswd
		db.Save(&authDetail)
		return "successfully changed password"
	}
	return "incorrect Password"
}
