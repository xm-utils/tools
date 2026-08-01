// 时间工具
// Neo

package common

import (
	"time"
)

const TimeFormat = "2006-01-02 15:04:05"
const DateFormat = "2006-01-02"

var (
	TimeLocation = time.Local
	UTC4         = time.FixedZone("UTC-4", -(60 * 60 * 4))
	UTC8         = time.FixedZone("UTC+8", 8*60*60)
)

// /时间范围
type TimeRange []time.Time

func (object TimeRange) Len() int {
	return len(object)
}
func (object TimeRange) Less(i, j int) bool {
	return object[i].Before(object[j])
}
func (object TimeRange) Swap(i, j int) {
	object[i], object[j] = object[j], object[i]
}

// Time2RangeV1 时间区间切割
func Time2RangeV1(start, end time.Time) (snapshotTimeRange, nonSnapshotTimeRange []time.Time) {
	now := time.Now()
	time000000 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	if start == time000000 || start.After(time000000) {
		// 开始时间大于等于今日凌晨，均不能使用快照
		nonSnapshotTimeRange = append(nonSnapshotTimeRange, start, end)
		return
	}

	// 开始日期所在的起点
	start000000 := time.Date(start.Year(), start.Month(), start.Day(),
		0, 0, 0, 0, time.Local)
	// 开始日期所在的终点
	start235959 := time.Date(start.Year(), start.Month(), start.Day(),
		23, 59, 59, 0, time.Local)
	// 结束日期所在的起点
	end000000 := time.Date(end.Year(), end.Month(), end.Day(),
		0, 0, 0, 0, time.Local)
	// 结束日期所在的终点
	end235959 := time.Date(end.Year(), end.Month(), end.Day(),
		23, 59, 59, 0, time.Local)
	if start == start000000 {
		// 开始日期对齐
		if end == end235959 {
			// 结束日期也对齐
			snapshotTimeRange = append(snapshotTimeRange, start, end)
		} else if end.Before(end235959) {
			// 结束日期未对齐
			if end000000.Year() > start.Year() /*跨年*/ ||
				end000000.YearDay() > start000000.YearDay() /*超过一天*/ {
				// 有快照可用
				snapshotTimeRange = append(snapshotTimeRange, start,
					end235959.AddDate(0, 0, -1))
				// 末尾区间查询原始数据
				nonSnapshotTimeRange = append(nonSnapshotTimeRange, end000000, end)
			}
		}
	} else if start.After(start000000) {
		// 开始日期未对齐
		if end == end235959 {
			// 结束日期对齐
			if start.Year() < end.Year() /*跨年*/ ||
				start000000.YearDay() < end000000.YearDay() {
				nonSnapshotTimeRange = append(nonSnapshotTimeRange, start, start235959)
				snapshotTimeRange = append(snapshotTimeRange,
					start000000.AddDate(0, 0, 1),
					end)
			} else {
				nonSnapshotTimeRange = append(nonSnapshotTimeRange, start, end)
			}
		} else if end.Before(end235959) {
			// 结束日期未对齐
			if start.Year() < end.Year() /*跨年*/ ||
				start000000.AddDate(0, 0, 1).YearDay() < end000000.YearDay() /*超过一天*/ {
				snapshotTimeRange = append(snapshotTimeRange, start000000.AddDate(0, 0, 1),
					end235959.AddDate(0, 0, -1))
				nonSnapshotTimeRange = append(nonSnapshotTimeRange, start, start235959, end000000, end)
			}
		}
	}
	return
}

func ParseTime2RangesV2(startTimeStr, endTimeStr string) (originalTimeRange, snapshotTimeRange []time.Time, err error) {
	const expectStartNumber = 0
	const expectEndNumber = 23 + 59 + 59
	var startTime, endTime time.Time

	now := time.Now()
	now = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, time.Local)
	startTime, err = time.ParseInLocation(TimeFormat, startTimeStr, time.Local)
	endTime, err = time.ParseInLocation(TimeFormat, endTimeStr, time.Local)

	if nil != err {
		return
	}

	if startTime.After(endTime) {
		startTime, endTime = endTime, startTime
	}

	if startTime.After(now) {
		return
	}

	if endTime.After(now) {
		endTime = now
	}

	// 預期起始的 時+分+秒 = 0
	startSpec := startTime.Hour() + startTime.Minute() + startTime.Second()
	// 預期結束的 時+分+秒 = 141
	endSpec := endTime.Hour() + endTime.Minute() + endTime.Second()

	// 結束=now時 會擔心統計表還未寫入新資料
	if startSpec == expectStartNumber && endSpec == expectEndNumber {
		snapshotTimeRange = append(snapshotTimeRange, startTime)

		if endTime == now {
			snapshotTimeRange = append(snapshotTimeRange, endTime.Add(-24*time.Hour))
			originalTimeRange = append(originalTimeRange, time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 0, 0, 0, 0, time.Local))
			originalTimeRange = append(originalTimeRange, time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 23, 59, 59, 0, time.Local))
			return
		}

		snapshotTimeRange = append(snapshotTimeRange, endTime)
		return
	}

	if startSpec != expectStartNumber {
		originalTimeRange = append(originalTimeRange, startTime)
		originalTimeRange = append(originalTimeRange, time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 23, 59, 59, 0, time.Local))

		middleStart := startTime.Add(24 * time.Hour)
		snapshotTimeRange = append(snapshotTimeRange, time.Date(middleStart.Year(), middleStart.Month(), middleStart.Day(), 0, 0, 0, 0, time.Local))

		if endSpec == expectEndNumber {
			snapshotTimeRange = append(snapshotTimeRange, endTime)
		}

	}

	if endSpec != expectEndNumber {
		originalTimeRange = append(originalTimeRange, time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 0, 0, 0, 0, time.Local))
		originalTimeRange = append(originalTimeRange, endTime)

		if startSpec == expectStartNumber {
			snapshotTimeRange = append(snapshotTimeRange, startTime)
		}

		middleEnd := endTime.Add(-24 * time.Hour)
		snapshotTimeRange = append(snapshotTimeRange, time.Date(middleEnd.Year(), middleEnd.Month(), middleEnd.Day(), 23, 59, 59, 0, time.Local))
	}

	if len(snapshotTimeRange) > 0 && snapshotTimeRange[1].Before(snapshotTimeRange[0]) {
		snapshotTimeRange = nil
		originalTimeRange = []time.Time{
			startTime, endTime,
		}
	}
	return
}

// /解析时间段
func ParseTime2Ranges(startTimeStr, endTimeStr string) (timeRange1, timeRange2 []time.Time, err error) {
	return ParseTime2RangesV2(startTimeStr, endTimeStr)

	//const timeFormat = "2006-01-02 15:04:05"
	//var startTime, endTime time.Time
	//startTime, err = time.ParseInLocation(timeFormat, startTimeStr, time.Local)
	//if nil != err {
	//	return
	//}
	//endTime, err = time.ParseInLocation(timeFormat, endTimeStr, time.Local)
	//if nil != err {
	//	return
	//}
	//
	//timeRange2, timeRange1 = Time2RangeV1(startTime, endTime)
	//return

	//now := time.Now()
	//if startTime.Unix() >= time.Date(now.Year(), now.Month(), now.Day(),
	//	0, 0, 0, 0, time.Local).Unix() {
	//	timeRange1 = append(timeRange1, startTime, endTime)
	//} else {
	//	if 0 == startTime.Hour() && 0 == startTime.Minute() && 0 == startTime.Second() {
	//		if 23 == endTime.Hour() && 59 == endTime.Minute() && 59 == endTime.Second() {
	//			loopStart := startTime
	//			for loopStart.YearDay() <= endTime.YearDay() {
	//				timeRange2 = append(timeRange2, loopStart, time.Date(loopStart.Year(), loopStart.Month(), loopStart.Day(),
	//					23, 59, 59, 0, time.Local))
	//				loopStart = loopStart.AddDate(0, 0, 1)
	//			}
	//		} else {
	//			loopStart := startTime
	//			for loopStart.YearDay() < endTime.YearDay() {
	//				timeRange2 = append(timeRange2, loopStart, time.Date(loopStart.Year(), loopStart.Month(), loopStart.Day(),
	//					23, 59, 59, 0, time.Local))
	//				loopStart = loopStart.AddDate(0, 0, 1)
	//			}
	//			timeRange1 = append(timeRange1, time.Date(endTime.Year(), endTime.Month(), endTime.Day(),
	//				0, 0, 0, 0, time.Local), endTime)
	//		}
	//	} else {
	//		if 23 == endTime.Hour() && 59 == endTime.Minute() && 59 == endTime.Second() {
	//			timeRange1 = append(timeRange1, startTime, time.Date(startTime.Year(), startTime.Month(), startTime.Day(),
	//				23, 59, 59, 0, time.Local))
	//			loopStart := time.Date(startTime.Year(), startTime.Month(), startTime.Day(),
	//				0, 0, 0, 0, time.Local).AddDate(0, 0, 1)
	//			for loopStart.YearDay() <= endTime.YearDay() {
	//				timeRange2 = append(timeRange2, loopStart, time.Date(loopStart.Year(), loopStart.Month(), loopStart.Day(),
	//					23, 59, 59, 0, time.Local))
	//				loopStart = loopStart.AddDate(0, 0, 1)
	//			}
	//		} else {
	//			if endTime.YearDay() > startTime.YearDay() {
	//				timeRange1 = append(timeRange1, startTime)
	//				timeRange1 = append(timeRange1, time.Date(startTime.Year(), startTime.Month(), startTime.Day(),
	//					23, 59, 59, 0, time.Local))
	//			}
	//			loopStart := time.Date(startTime.Year(), startTime.Month(), startTime.Day(),
	//				0, 0, 0, 0, time.Local).AddDate(0, 0, 1)
	//			for loopStart.YearDay() < endTime.YearDay() {
	//				timeRange2 = append(timeRange2, loopStart, time.Date(loopStart.Year(), loopStart.Month(), loopStart.Day(),
	//					23, 59, 59, 0, time.Local))
	//				loopStart = loopStart.AddDate(0, 0, 1)
	//			}
	//			timeRange1 = append(timeRange1, time.Date(endTime.Year(), endTime.Month(), endTime.Day(),
	//				0, 0, 0, 0, time.Local), endTime)
	//		}
	//	}
	//	if 0 < len(timeRange2) {
	//		min := timeRange2[0]
	//		max := timeRange2[1]
	//		for i := 2; i < len(timeRange2); i++ {
	//			t := timeRange2[i]
	//			if t.Before(min) {
	//				min = t
	//			}
	//			if t.After(max) {
	//				max = t
	//			}
	//		}
	//		if max.After(now) && startTime.YearDay() < now.YearDay() {
	//			if max.YearDay() > now.AddDate(0, 0, 1).YearDay() {
	//				max = time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 0, time.Local)
	//			}
	//			timeRange1 = append(timeRange1,
	//				time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local),
	//				max)
	//			tmp := max.AddDate(0, 0, -1)
	//			max = time.Date(tmp.Year(), tmp.Month(), tmp.Day(), 23, 59, 59, 0, time.Local)
	//		}
	//		timeRange2 = []time.Time{min, max}
	//	}
	//	if 0 < len(timeRange1) {
	//		sort.Sort(TimeRange(timeRange1))
	//	}
	//}
	//return
}

// / 时间范围是否包含当前时间戳
func TimeRangeContainsToday(startTime, endTime string) (bool, error) {
	//判断是否包含当天数据
	containsToday := false
	today := time.Now()
	todayStartTimestamp := time.Date(today.Year(), today.Month(), today.Day(),
		0, 0, 0, 0, time.Local).Unix()
	todayEndTimestamp := time.Date(today.Year(), today.Month(), today.Day(),
		23, 59, 59, 0, time.Local).Unix()
	startTimestamp := int64(-1)
	endTimestamp := int64(-1)
	if 0 < len(startTime) {
		reqTime, err := time.ParseInLocation(TimeFormat, startTime, time.Local)
		if nil != err {
			return false, err
		}

		startTimestamp = reqTime.Unix()
	}
	if 0 < len(endTime) {
		reqTime, err := time.ParseInLocation(TimeFormat, endTime, time.Local)
		if nil != err {
			return false, err
		}

		endTimestamp = reqTime.Unix()
	}
	if -1 != startTimestamp || -1 != endTimestamp {
		if -1 != startTimestamp && -1 != endTimestamp {
			if endTimestamp >= todayStartTimestamp || endTimestamp >= todayEndTimestamp {
				containsToday = true
			}
		} else if -1 != startTimestamp {
			if startTimestamp >= todayStartTimestamp {
				containsToday = true
			}
		} else if -1 != endTimestamp {
			if endTimestamp >= todayStartTimestamp || endTimestamp >= todayEndTimestamp {
				containsToday = true
			}
		}
	}
	return containsToday, nil
}

// 針對以天為單位的統計表
func ParseTimeByDay(startTimeStr, endTimeStr string) (timeRange, timeMiddleRange []time.Time, err error) {
	const expectStartNumber = 0
	const expectEndNumber = 23 + 59 + 59

	now := Now()
	startTime := ParseLocalTime(startTimeStr)
	endTime := ParseLocalTime(endTimeStr)

	if startTime.After(endTime) {
		startTime, endTime = endTime, startTime
	}

	if startTime.After(now) {
		return
	}

	if endTime.After(now) {
		endTime = now
	}

	// 預期起始的 時+分+秒 = 0
	startSpec := startTime.Hour() + startTime.Minute() + startTime.Second()
	// 預期結束的 時+分+秒 = 141
	endSpec := endTime.Hour() + endTime.Minute() + endTime.Second()

	// 結束=now時 會擔心統計表還未寫入新資料
	if startSpec == expectStartNumber && endSpec == expectEndNumber {
		timeMiddleRange = append(timeMiddleRange, startTime)

		if endTime == now {
			timeMiddleRange = append(timeMiddleRange, endTime.Add(-24*time.Hour))
			timeRange = append(timeRange, StartByTime(endTime)) //time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 0, 0, 0, 0, time.Local))
			timeRange = append(timeRange, EndByTime(endTime))   //time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 23, 59, 59, 0, time.Local))
			return
		}

		timeMiddleRange = append(timeMiddleRange, endTime)
		return
	}

	if startSpec != expectStartNumber {
		timeRange = append(timeRange, startTime)
		timeRange = append(timeRange, EndByTime(startTime)) // time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 23, 59, 59, 0, time.Local))

		middleStart := startTime.Add(24 * time.Hour)
		timeMiddleRange = append(timeMiddleRange, StartByTime(middleStart)) // time.Date(middleStart.Year(), middleStart.Month(), middleStart.Day(), 0, 0, 0, 0, time.Local))

		if endSpec == expectEndNumber {
			timeMiddleRange = append(timeMiddleRange, endTime)
		}

	}

	if endSpec != expectEndNumber {
		timeRange = append(timeRange, StartByTime(endTime)) // time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 0, 0, 0, 0, time.Local))
		timeRange = append(timeRange, endTime)

		if startSpec == expectStartNumber {
			timeMiddleRange = append(timeMiddleRange, startTime)
		}

		middleEnd := endTime.Add(-24 * time.Hour)
		timeMiddleRange = append(timeMiddleRange, EndByTime(middleEnd)) // time.Date(middleEnd.Year(), middleEnd.Month(), middleEnd.Day(), 23, 59, 59, 0, time.Local))
	}

	if len(timeMiddleRange) > 0 && timeMiddleRange[1].Before(timeMiddleRange[0]) {
		timeMiddleRange = nil
		timeRange = []time.Time{
			startTime, endTime,
		}
	}
	return
}

// 針對以天為單位的統計表
// 生成報表時間為 01:00 offset = -1
// 查詢時間為 2020-07-01 00:10:00 ~ 2020-07-05 00:20:00
// timeRange = []{ 2020-07-01 00:10:00 ~ 2020-07-01 23:59:59, 2020-07-05 00:00:00 ~ 2020-07-05 00:20:00, 2020-07-04 00:00:00 ~ 2020-07-04 23:59:59}
// timeMiddleRange = []{ 2020-07-02 00:00:00 ~ 2020-07-03 23:59:59}
func ParseTimeByDayForReport(startTimeStr, endTimeStr string, endTimeHourOffset int) ([]time.Time, []time.Time, error) {
	timeRange, timeMiddleRange, err := ParseTimeByDay(startTimeStr, endTimeStr)
	if err != nil {
		return timeRange, timeMiddleRange, err
	}

	if len(timeRange) == 0 && len(timeMiddleRange) == 0 {
		return timeRange, timeMiddleRange, err
	}

	now := time.Now()
	endTime := ParseLocalTime(endTimeStr)
	//now = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), time.Local)
	//endTime, _ := time.ParseInLocation(TimeFormat, endTimeStr, time.Local)
	if endTime.After(now) {
		endTime = now
	}

	if len(timeRange) == 0 && len(timeMiddleRange) > 0 {
		lastIdx := len(timeMiddleRange)
		lastDayStart, lastDayEnd := timeMiddleRange[lastIdx-2], timeMiddleRange[lastIdx-1]

		checkTime := now.Add(time.Duration(endTimeHourOffset) * time.Hour)

		if checkTime.Before(lastDayEnd) && !checkTime.Equal(endTime) {
			if lastDayStart.Format(DateFormat) == lastDayEnd.Format(DateFormat) {
				timeMiddleRange = timeMiddleRange[:lastIdx-2]
				timeRange = append(timeRange, lastDayStart)
				timeRange = append(timeRange, lastDayEnd)
			} else {
				timeMiddleRange[lastIdx-1] = lastDayEnd.AddDate(0, 0, -1)
				timeRange = append(timeRange, lastDayEnd.Add(-(23*60*60+59*60+59)*time.Second))
				timeRange = append(timeRange, lastDayEnd)
			}
		}
	}

	if len(timeRange) > 0 && len(timeMiddleRange) > 0 {
		idx := len(timeRange)
		timeRangeEndTime := timeRange[idx-1].Add(time.Duration(endTimeHourOffset) * time.Hour)
		if now.Day() == timeRangeEndTime.Day() &&
			now.Month() == timeRangeEndTime.Month() &&
			now.Year() == timeRangeEndTime.Year() {
			return timeRange, timeMiddleRange, nil
		}

		lastIdx := len(timeMiddleRange)
		lastDayStart, lastDayEnd := timeMiddleRange[lastIdx-2], timeMiddleRange[lastIdx-1]
		if lastDayEnd.Day() == timeRangeEndTime.Day() &&
			lastDayEnd.Month() == timeRangeEndTime.Month() &&
			lastDayEnd.Year() == timeRangeEndTime.Year() {
			if lastDayStart.Format(DateFormat) == lastDayEnd.Format(DateFormat) {
				timeMiddleRange = timeMiddleRange[:lastIdx-2]
				timeRange = append(timeRange, lastDayStart)
				timeRange = append(timeRange, lastDayEnd)
			} else {
				timeMiddleRange[lastIdx-1] = lastDayEnd.AddDate(0, 0, -1)
				timeRange = append(timeRange, lastDayEnd.Add(-(23*60*60+59*60+59)*time.Second))
				timeRange = append(timeRange, lastDayEnd)
			}
		}
	}

	return timeRange, timeMiddleRange, nil
}

// 針對以小時為單位的統計表
func ParseTimeRangeByHour(startTimeStr, endTimeStr string) (timeRange, timeMiddleRange []time.Time, err error) {
	const expectStartNumber = 0
	const expectEndNumber = 118
	var startTime, endTime time.Time

	now := time.Now()
	now = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, time.Local)
	startTime, err = time.ParseInLocation(TimeFormat, startTimeStr, time.Local)
	endTime, err = time.ParseInLocation(TimeFormat, endTimeStr, time.Local)

	if nil != err {
		return
	}

	if startTime.After(endTime) {
		startTime, endTime = endTime, startTime
	}

	if startTime.After(now) {
		return
	}

	if endTime.After(now) {
		endTime = now
	}

	// 預期起始的 分+秒 = 0
	startSpec := startTime.Minute() + startTime.Second()
	// 預期結束的 分+秒 = 118
	endSpec := endTime.Minute() + endTime.Second()

	//避免遇到統計表建立中
	if (endSpec == expectEndNumber && endTime == now) || (endSpec == expectEndNumber && now.Sub(endTime).Minutes() < 5) {
		timeRange = []time.Time{
			startTime, endTime,
		}
		return
	}

	if startSpec == expectStartNumber && endSpec == expectEndNumber {
		timeMiddleRange = append(timeMiddleRange, startTime)
		timeMiddleRange = append(timeMiddleRange, endTime)
		return
	}

	if startSpec != expectStartNumber {
		timeRange = append(timeRange, startTime)
		timeRange = append(timeRange, time.Date(startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), 59, 59, 0, time.Local))

		middleStart := startTime.Add(1 * time.Hour)
		timeMiddleRange = append(timeMiddleRange, time.Date(middleStart.Year(), middleStart.Month(), middleStart.Day(), middleStart.Hour(), 0, 0, 0, time.Local))

		if endSpec == expectEndNumber {
			timeMiddleRange = append(timeMiddleRange, endTime)
		}

	}

	if endSpec != expectEndNumber {
		timeRange = append(timeRange, time.Date(endTime.Year(), endTime.Month(), endTime.Day(), endTime.Hour(), 0, 0, 0, time.Local))
		timeRange = append(timeRange, endTime)

		if startSpec == expectStartNumber {
			timeMiddleRange = append(timeMiddleRange, startTime)
		}

		middleEnd := endTime.Add(-1 * time.Hour)
		timeMiddleRange = append(timeMiddleRange, time.Date(middleEnd.Year(), middleEnd.Month(), middleEnd.Day(), middleEnd.Hour(), 59, 59, 0, time.Local))
	}

	if len(timeMiddleRange) > 0 && timeMiddleRange[1].Before(timeMiddleRange[0]) {
		timeMiddleRange = nil
		timeRange = []time.Time{
			startTime, endTime,
		}
	}
	return
}

// TimeMeasure / 时间测量
type TimeMeasure struct {
	start int64
}

// Start /开始
func (object *TimeMeasure) Start() {
	object.start = time.Now().UnixNano()
}

// Stop /停止
func (object *TimeMeasure) Stop() int64 {
	return time.Now().UnixNano() - object.start
}

func formatTime(t time.Time, loc *time.Location, format string) string {
	if t.IsZero() {
		return ""
	}
	return t.In(loc).Format(format)
}

func FormatUtcTime(t time.Time) string {
	return formatTime(t, time.UTC, TimeFormat)
}

func FormatLocalTime(t time.Time) string {
	return formatTime(t, TimeLocation, TimeFormat)
}

func FormatLocalDate(t time.Time) string {
	return formatTime(t, TimeLocation, DateFormat)
}

func FormatTime(t time.Time) string {
	return formatTime(t, t.Location(), TimeFormat)
}
func FormatDate(t time.Time) string {
	return formatTime(t, t.Location(), DateFormat)
}

func ParseLocalDate(value string) time.Time {
	t, _ := time.ParseInLocation(DateFormat, value, TimeLocation)
	return t
}
func ParseLocalTime(value string) time.Time {
	layout := DateFormat
	if len(value) > 10 {
		layout = TimeFormat
	}
	t, _ := time.ParseInLocation(layout, value, TimeLocation)
	return t
}

func ParseUtcTime(value string) time.Time {
	layout := DateFormat
	if len(value) > 10 {
		layout = TimeFormat
	}
	t, _ := time.ParseInLocation(layout, value, time.UTC)
	return t
}

func CountDays(searchTimeStr string) (float64, error) {
	now := Now()
	searchTime := ParseLocalTime(searchTimeStr) //time.ParseInLocation(TimeFormat, searchTimeStr, TimeLocation)
	d := now.Sub(searchTime)
	return d.Hours() / 24, nil
}

func RangeTimeSpaceByRecord(startTimeStr, endTimeStr string) (queryTime []string, originTime []time.Time, err error) {
	startTime, er1 := time.ParseInLocation(TimeFormat, startTimeStr, time.Local)
	if er1 != nil {
		return nil, nil, er1
	}
	endTime, er2 := time.ParseInLocation(TimeFormat, endTimeStr, time.Local)
	if er2 != nil {
		return nil, nil, er2
	}
	if startTime.After(endTime) {
		startTime, endTime = endTime, startTime
	}
	now := time.Now()
	space := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.Local).Add(-1 * time.Hour)
	if !startTime.After(space) && !endTime.After(space) {
		queryTime = append(queryTime, startTime.Format(TimeFormat))
		queryTime = append(queryTime, endTime.Format(TimeFormat))
		return
	} else if !startTime.Before(space) && !endTime.Before(space) {
		originTime = append(originTime, startTime)
		originTime = append(originTime, endTime)
		return
	}

	if !startTime.After(space) {
		originTime = append(originTime, space)
		originTime = append(originTime, endTime)
		queryTime = append(queryTime, startTime.Format(TimeFormat))
		queryTime = append(queryTime, space.Add(-1*time.Second).Format(TimeFormat))
	}
	return
}

func Now() time.Time {
	return time.Now().In(TimeLocation)
}

func GetDateStr(unix int64) string {
	t1 := time.Unix(unix, 0)
	return t1.Format("20060102")
}

// CurWeekStart 获得本周第一天
func CurWeekStart() time.Time {
	now := Now()
	//周日为每周第一天
	offset := int(now.Weekday())

	//周一为每周第一天
	//offset := int(now.Weekday())
	//if offset == 0 {
	//	offset = 7
	//}
	//offset -= 1
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, TimeLocation).AddDate(0, 0, -offset)
}

// CurMonthStart 获得本月一号时间
func CurMonthStart() time.Time {
	now := Now() // time.Now().In(TimeLocation)
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, TimeLocation)
}

// NextWeekStart 获得下周第一天
func NextWeekStart() time.Time {
	now := Now() // time.Now().In(TimeLocation)

	offset := 0
	//周日为每周第一天
	if now.Weekday() == 0 {
		offset = 7
	} else {
		offset = int(7 - now.Weekday())
	}

	//周一为每周第一天
	//offset := int(8 - now.Weekday())
	//if offset == 8 {
	//	offset = 1
	//}

	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, TimeLocation).AddDate(0, 0, offset)
}

// NextMonthStart 获得下个月一号时间
func NextMonthStart() time.Time {
	now := Now() // time.Now().In(TimeLocation)
	return time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, TimeLocation)
}

// CurTodayStart 获得今天开始时间
func CurTodayStart() time.Time {
	now := Now()            // time.Now().In(TimeLocation)
	return StartByTime(now) // time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, TimeLocation)
}

// CurTodayEnd 获得今天开始时间
func CurTodayEnd() time.Time {
	now := Now()          // time.Now().In(TimeLocation)
	return EndByTime(now) // time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 0, TimeLocation)
}

// NextDayStart 获得明天开始时间
func NextDayStart() time.Time {
	now := Now()                                // time.Now().In(TimeLocation)
	return StartByTime(now.Add(24 * time.Hour)) // time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, TimeLocation).AddDate(0, 0, 1)
}

// StartByTime 获得指定日期开始时间
func StartByTime(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

// EndByTime 获得指定日期开始时间
func EndByTime(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 0, t.Location())
}

// IsCurDay 是否是今天
func IsCurDay(t time.Time) bool {
	cur := CurTodayStart()
	if t.Year() != cur.Year() {
		return false
	}
	if t.Month() != cur.Month() {
		return false
	}
	if t.Day() != cur.Day() {
		return false
	}
	return true
}

// IsSameDay 是否是同一天
func IsSameDay(t1, t2 time.Time) bool {
	// 将两个时间调整到相同的时区（例如UTC），然后只比较年月日部分
	return t1.In(time.UTC).Format("2006-01-02") == t2.In(time.UTC).Format("2006-01-02")
}
