package handlers

import (
	"errors"
	"fmt"
	"interface_hash_server/configs"
	"interface_hash_server/tools"
	"net/http"
)

// ExceptionHandle handles request of unproper URL
func ExceptionHandle(response http.ResponseWriter, request *http.Request) {

	response.Header().Set("Content-Type", configs.JSONContent)
	response.WriteHeader(http.StatusTemporaryRedirect)

	nextHref := configs.HTTP + configs.BaseURL
	errorMessage := "Not a proper path usage"

	fmt.Fprintf(response, tools.ErrorMessageTemplate, errorMessage, nextHref)

	tools.InfoLogger.Println("Not a proper path")
}

// responseInternalError is an internal error handler.
func responseBadRequest(response http.ResponseWriter, message string, nextMessage string, nextHref string) {
	response.Header().Set("Content-Type", configs.JSONContent)
	response.WriteHeader(http.StatusBadRequest)
	fmt.Fprintf(response, tools.SimpleTemplate, message, nextMessage, nextHref)
	tools.ErrorLogger.Println(errors.New(message))
}

// responseInternalError is an internal error handler.
func responseInternalError(response http.ResponseWriter, err error, nextHref string) {
	response.Header().Set("Content-Type", configs.JSONContent)
	response.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(response, tools.ErrorMessageTemplate, err.Error(), nextHref)
	tools.ErrorLogger.Printf("%s\n", err.Error())
}
