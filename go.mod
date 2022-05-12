module github.com/openrm/krakend-bloomd

go 1.13

require (
	github.com/devopsfaith/krakend-jose/v2 v2.0.2
	github.com/geetarista/go-bloomd v0.0.0-20140722181834-7f8e8a358bec
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/luraproject/lura/v2 v2.0.4
)

replace github.com/dgrijalva/jwt-go v3.2.0+incompatible => github.com/golang-jwt/jwt v3.2.2+incompatible
