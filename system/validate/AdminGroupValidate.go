package validate

func PageParamError() map[string]string {
	formError := make(map[string]string)
	formError["Page.required"] = "页数参数缺失"
	formError["PageSize.required"] = "每页条数参数缺失"
	return formError
}
