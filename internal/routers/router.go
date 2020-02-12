package routers

import (
	"net/http"

	"github.com/gorilla/mux"

	"interface_hash_server/internal/handlers"
)

func SetUpInterfaceRouter(router *mux.Router) {

	/* @POST
	 * Set Value
	 * Request URI : http://~/hash/data
	 * Request Data format : {
			data : [
				{ key : , value : },
				{ key : , value : }, ... ,
			]
		}
	*/
	router.HandleFunc("", handlers.SetKeyValue).Methods(http.MethodPost)

	/* @GET
	 * Get Value From Key
	 * Request URI : http://~/hash/data/key
	 */
	router.HandleFunc("/{key}", handlers.GetValueFromKey).Methods(http.MethodGet)

	/* @DELETE
	 * DELETE Value From Key
	 * Request URI : http://~/hash/data/key
	 */
	// router.HandleFunc("/{key}", handlers.DeleteKeyValue).Methods(http.MethodDelete)
}
