package response

import (
	"encoding/json"
)

type GetResultTemplate struct {
	RedisResult
	BasicTemplate
}

func (template GetResultTemplate) Marshal(curMsg, nextMsg, nextLink string) ([]byte, error) {

	template.Message = curMsg
	template.NextLink.Message = nextMsg
	template.NextLink.Href = nextLink

	encodedTemplate, err := json.Marshal(template)
	if err != nil {
		return nil, err
	}

	return encodedTemplate, nil
}
