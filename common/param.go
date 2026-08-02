package common

import (
	"time"
)

const (
	DefaultPage     int = 1
	DefaultPageSize int = 20
	MaxPageSize     int = 500
)

type IdParam struct {
	Id int64 `json:"id" form:"id" uri:"id" binding:"required"`
}

func IdParamError() map[string]string {
	return map[string]string{
		"Id.required": "主键参数不能为空",
	}
}

type IdArrParam struct {
	Id []int64 `json:"id" form:"id" binding:"required"`
}

// TimeParam 时间区间参数
type TimeParam struct {
	StartTime string `json:"startTime" form:"startTime" binding:"required"` //开始时间
	EndTime   string `json:"endTime" form:"endTime" binding:"required"`     //结束时间
	st        time.Time
	et        time.Time
}

func (req *TimeParam) IsValid() bool {
	if req == nil {
		return false
	}
	if req.StartTime == "" || req.EndTime == "" {
		return false
	}

	st, err := parseLocalTime(req.StartTime)
	if err != nil {
		return false
	}
	req.st = st
	et, err := parseLocalTime(req.EndTime)
	if err != nil {
		return false
	}
	req.et = et

	return true
}

func (req *TimeParam) GetTime() (start, end time.Time) {
	if !req.IsValid() {
		return
	}

	return req.st, req.et
}

func (req *TimeParam) DiffDays() int {
	t1, t2 := req.GetTime()
	return int(t2.Sub(t1).Hours() / 24)
}

func parseLocalTime(value string) (time.Time, error) {
	layout := DateFormat
	if len(value) > 10 {
		layout = TimeFormat
	}
	return time.Parse(layout, value)
}

// PageParam 用于查询的类
type PageParam struct {
	Page     int `json:"page" form:"page" binding:"required"`
	PageSize int `json:"pageSize" form:"pageSize" binding:"required"`
}

func (bqp *PageParam) IsValid() bool {
	return bqp.Page > 0 && bqp.PageSize > 0
}

func (bqp *PageParam) Offset() int {
	offset := 0
	if bqp.Page > 1 {
		offset = (bqp.Page - 1) * bqp.PageSize
	}
	return offset
}

func (bqp *PageParam) GetLimit() (limit, offset int) {
	if bqp.Page < DefaultPage {
		bqp.Page = DefaultPage
	}
	if bqp.PageSize <= 0 {
		bqp.PageSize = DefaultPageSize
	}
	if bqp.PageSize > MaxPageSize {
		bqp.PageSize = MaxPageSize
	}

	limit = bqp.PageSize
	offset = (bqp.Page - 1) * bqp.PageSize
	return
}
