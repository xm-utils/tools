package system

import (
	"context"
	"fmt"
	"strings"

	"github.com/beego/beego/v2/client/orm"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/beego/database"
	"github.com/xm-utils/tools/common"
	"github.com/xm-utils/tools/redis"
	"github.com/xm-utils/tools/system/validate"
)

type Dict struct {
	log *logrus.Entry
}

func NewDict() *Dict {
	return &Dict{
		log: logrus.WithField("module", "DictController"),
	}
}

func (ctrl *Dict) GetList(c *gin.Context) {
	var form validate.DictListForm
	if errData := shouldBind(c, &form, validate.DictListFormError()); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	cond := orm.NewCondition()
	if len(form.Code) > 0 {
		cond = cond.And("type_code__contains", form.Code)
	}
	if len(form.Name) > 0 {
		cond = cond.And("type_name__contains", form.Name)
	}
	list, total, err := database.FindAll[DictType](database.ListParam{
		Param: cond,
		Page:  form.PageParam,
	})

	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}
	common.GinSuccess(c, common.PageResponse{
		TotalCount: total,
		List:       list,
	})
}

func (ctrl *Dict) SelectList(c *gin.Context) {
	list, err := database.FindList[DictType](orm.NewCondition().And("status", common.StatusEnable))
	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}

	res := make([]common.SelectOption[string], len(list))
	for i, role := range list {
		res[i] = common.SelectOption[string]{
			Type:     role.DictType,
			TypeName: role.DictName,
		}
	}
	common.GinSuccess(c, res)
}

func (ctrl *Dict) Add(c *gin.Context) {
	form := validate.DictAddForm{}
	formErr := validate.DictAddFormError()
	_ = c.ShouldBind(&form)

	if errData := shouldBind(c, &form, formErr); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	old := database.ReadOne(DictType{DictType: form.DictType}, "dict_type")
	if old != nil {
		common.GinError(c, common.CommonSystemError, fmt.Sprintf("字典类型已存在: %s", form.DictType))
		return
	}

	var saveData = DictType{
		BaseModel: BaseModel{
			Remark:   form.Remark,
			CreateBy: c.GetString(AdminUsernameKey),
		},
		DictName: form.DictName,
		DictType: form.DictType,
		Status:   form.Status,
	}

	if err := database.Insert(nil, &saveData); err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}
	common.GinSuccess(c, "")
}

func (ctrl *Dict) Edit(c *gin.Context) {
	form := validate.DictEditForm{}
	formErr := validate.DictEditFormError()

	if errData := shouldBind(c, &form, formErr); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	old := database.FindOne[DictType](form.Id)
	if old == nil {
		common.GinError(c, common.CommonDataNotExist, "字典不存在")
		return
	}

	var saveData = DictType{
		BaseModel: BaseModel{
			Remark:   form.Remark,
			UpdateBy: c.GetString(AdminUsernameKey),
		},
		Id:       form.Id,
		DictName: form.DictName,
		DictType: form.DictType,
		Status:   form.Status,
	}

	err := database.UpdateModel(nil, old, saveData)
	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}
	common.GinSuccess(c, "")
}

func (ctrl *Dict) Del(c *gin.Context) {
	var form common.IdParam
	var formErr = common.IdParamError()
	if errData := shouldBindUri(c, &form, formErr); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	one := database.FindOne[DictType](form.Id)
	if one == nil {
		common.GinSuccess(c, "")
		return
	}

	err := database.NewOrmTx().Execute(func(o orm.TxOrmer) error {
		if _, err := o.Delete(one); err != nil {
			return err
		}
		if _, err := o.Delete(&DictData{DictType: one.DictType}, "DictType"); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}

	redis.HDel(context.Background(), SYS_DICT_CACHE_KEY, fmt.Sprintf("id:%d", form.Id))

	common.GinSuccess(c, "")
}

func (ctrl *Dict) Info(c *gin.Context) {

	var form common.IdParam
	var formErr = common.IdParamError()
	if errData := shouldBindUri(c, &form, formErr); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	val, err := redis.HGet[*DictType](context.Background(), SYS_DICT_CACHE_KEY, fmt.Sprintf("id:%d", form.Id))
	if err == nil && val != nil {
		common.GinSuccess(c, val)
		return
	}

	model := database.FindOne[DictType](form.Id)
	if model == nil {
		common.GinError(c, common.CommonDataNotExist, "字典类型不存在")
		return
	}
	_ = redis.HSet(context.Background(), SYS_DICT_CACHE_KEY, fmt.Sprintf("id:%d", model.Id), model)

	common.GinSuccess(c, model)
}

func (ctrl *Dict) RefreshCache(c *gin.Context) {

	err := redis.Delete(c.Request.Context(), SYS_DICT_CACHE_KEY)
	if err != nil {
		common.GinError(c, AdminDictRefreshCacheFailure)
		return
	}
	err = redis.Delete(c.Request.Context(), SYS_DICT_DETAIL_CACHE_KEY)
	if err != nil {
		common.GinError(c, AdminDictRefreshCacheFailure)
		return
	}

	common.GinSuccess(c, "")
}

type DictDetail struct {
	log *logrus.Entry
}

func NewDictDetail() *DictDetail {
	return &DictDetail{
		log: logrus.WithField("module", "DictDetailController"),
	}
}

func (ctrl *DictDetail) GetList(c *gin.Context) {
	form := validate.DictDetailListForm{}
	formErr := validate.DictDetailListFormError()

	if errData := shouldBind(c, &form, formErr); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	cond := orm.NewCondition()
	if form.Code != "" {
		cond = cond.And("dict_type", form.Code)
	}
	list, total, err := database.FindAll[DictData](database.ListParam{
		Param: cond,
		Page:  form.PageParam,
	})

	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}
	common.GinSuccess(c, &common.PageResponse{
		TotalCount: total,
		List:       list,
	})
}

func (ctrl *DictDetail) Add(c *gin.Context) {
	form := validate.DictDetailAddForm{}
	formErr := validate.DictDetailAddFormError()
	if errData := shouldBind(c, &form, formErr); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	old := database.ReadOne(DictData{DictType: form.DictType, DictValue: form.DictValue}, "dict_type", "dict_value")
	if old != nil {
		common.GinError(c, common.CommonSystemError, fmt.Sprintf("字典项已存在: %s", form.DictValue))
		return
	}

	var saveData = &DictData{
		BaseModel: BaseModel{
			Remark:   form.Remark,
			CreateBy: c.GetString(AdminUsernameKey),
		},
		DictSort:  form.DictSort,
		DictLabel: form.DictLabel,
		DictValue: form.DictValue,
		DictType:  form.DictType,
		CssClass:  form.CssClass,
		ListClass: form.ListClass,
		Status:    form.Status,
	}

	err := database.Insert(nil, saveData)
	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}
	common.GinSuccess(c, "")
}

func (ctrl *DictDetail) Edit(c *gin.Context) {
	form := validate.DictDetailEditForm{}
	formErr := validate.DictDetailEditFormError()

	if errData := shouldBind(c, &form, formErr); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	old := database.FindOne[DictData](form.Id)
	if old == nil {
		common.GinError(c, common.CommonDataNotExist, "字典项不存在")
		return
	}

	var saveData = DictData{
		BaseModel: BaseModel{
			Remark: form.Remark,
		},
		Id:        form.Id,
		DictSort:  form.DictSort,
		DictLabel: form.DictLabel,
		DictValue: form.DictValue,
		DictType:  form.DictType,
		CssClass:  form.CssClass,
		ListClass: form.ListClass,
		Status:    form.Status,
	}
	err := database.UpdateModel(nil, old, saveData)
	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}
	common.GinSuccess(c, "")
}

func (ctrl *DictDetail) Del(c *gin.Context) {
	var form *common.IdParam
	var formErr = common.IdParamError()
	if errData := shouldBindUri(c, &form, formErr); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}
	old := database.FindOne[DictData](form.Id)
	if old == nil {
		common.GinError(c, common.CommonDataNotExist, "字典项不存在")
		return
	}
	err := database.Delete(nil, old)
	if err != nil {
		common.GinError(c, common.CommonSystemError, err.Error())
		return
	}

	common.GinSuccess(c, "")
}

func (ctrl *DictDetail) Info(c *gin.Context) {

	var form *common.IdParam
	var formErr = common.IdParamError()
	if errData := shouldBindUri(c, &form, formErr); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}
	old := database.FindOne[DictData](form.Id)
	if old == nil {
		common.GinError(c, common.CommonDataNotExist, "字典项不存在")
		return
	}

	common.GinSuccess(c, old)
}

func (ctrl *DictDetail) FindByType(c *gin.Context) {

	var param struct {
		TypeCode string `json:"type" form:"type" comment:"字典类型"  binding:"required"`
	}
	if errData := shouldBind(c, &param, map[string]string{
		"TypeCode.required": "字典类型",
	}); errData != nil {
		common.GinError(c, common.CommonParamError, errData.Error())
		return
	}

	types := strings.Split(param.TypeCode, ",")
	resultMap := make(map[string][]common.SelectOption[string])
	nt := make([]string, 0)
	for _, model := range types {
		if ops, err := redis.HGet[[]common.SelectOption[string]](context.Background(), SYS_DICT_DETAIL_CACHE_KEY, model); err != nil {
			nt = append(nt, model)
		} else {
			resultMap[model] = ops
		}
	}
	if len(nt) <= 0 {
		common.GinSuccess(c, resultMap)
		return
	}

	nt = common.Deduplicate(nt)
	cond := orm.NewCondition().And("dict_type__in", nt) //.And("state", common.StatusEnable)
	list, err := database.FindList[DictData](cond)
	if err != nil {
		common.GinSuccess(c, resultMap)
		return
	}

	toMap := common.ListToMap(list,
		func(detail *DictData) string {
			return detail.DictType
		},
		func(detail *DictData) common.SelectOption[string] {
			return common.SelectOption[string]{
				Type:     detail.DictLabel,
				TypeName: detail.DictValue,
			}
		})

	for t, ops := range toMap {
		_ = redis.HSet(context.Background(), SYS_DICT_DETAIL_CACHE_KEY, t, ops)
		resultMap[t] = ops
	}

	common.GinSuccess(c, resultMap)
}
