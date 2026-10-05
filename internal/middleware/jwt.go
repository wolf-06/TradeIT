package middleware

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// jwtKey returns the HMAC signing key for the JWT tokens.
//
// It is deliberately read on every call: a package level
// `var Key = []byte(os.Getenv("jwtkey"))` is evaluated during package initialisation,
// which runs BEFORE main() calls godotenv.Load(). In any setup where jwtkey is only
// defined in .env (the documented setup) the key was therefore always empty, and a
// token signed with an empty key - forgeable by anyone - was accepted.
func jwtKey() []byte {
	return []byte(os.Getenv("jwtkey"))
}

func CreateToken(userid uint64) string {

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": userid,
		"exp": time.Now().Add(24 * time.Hour).Unix(),
	})
	tokenstring, err := token.SignedString(jwtKey())
	if err != nil {
		fmt.Print(err.Error())
	}
	return tokenstring
}

func VerifyToken() gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if header == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing Authorization Token"})
			return
		}
		Token, err := jwt.Parse(header, func(Token *jwt.Token) (interface{}, error) {
			if _, ok := Token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("invalid token signing method")
			}
			return jwtKey(), nil
		})
		if err != nil {
			// NOTE: a log.Fatalln here used to exit the whole process whenever any client
			// sent a malformed/expired token. Reject the request instead.
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid Token"})
			return
		}

		if claim, ok := Token.Claims.(jwt.MapClaims); ok && Token.Valid {
			c.Set("userid", claim["sub"])
			c.Next()
		} else {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid Token"})
		}
	}
}
