package tools

const (
	//SimpleSuccessTemplate is used for Response without any data added
	SimpleSuccessTemplate = `{
		"message": "%s Successful",
		"_links" : {
			"message" : "%s",
			"href" : "%s"
			}
		}`

	//RedisResponseTemplate is used for Response with requested data added
	RedisResponseTemplate = `{
		"message": "%s",
		"response" : %s,
		"_links" : {
			"message" : "%s",
			"href" : "%s"
			}
		}`

	//ErrorMessageTemplate is used for Response with error request
	ErrorMessageTemplate = `{
		"message": "%s",
		"_links" : {
			"message" : "Main Url",
			"href" : "%s"
			}
		}`

	//SimpleTemplate is used for Response without any data added
	SimpleTemplate = `{
		"message": "%s",
		"_links" : {
			"message" : "%s",
			"href" : "%s"
			}
		}`
)
