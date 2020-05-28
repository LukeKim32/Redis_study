package response

import (
	"encoding/json"
	"hash_interface/tools"
)

type GetResultTemplate struct {
	RedisResult
	BasicTemplate
}

func (template GetResultTemplate) Marshal(
	result, address, curMsg, nextMsg, nextLink string,
) ([]byte, error) {

	tools.InfoLogger.Println("GET Marshal!!! ", result, address)
	template.Result = result
	template.NodeAdrress = address
	template.Message = curMsg
	template.NextLink.Message = nextMsg
	template.NextLink.Href = nextLink

	encodedTemplate, err := json.Marshal(template)
	if err != nil {
		return nil, err
	}

	return encodedTemplate, nil
}
