//网络工具
//Neo

package common

import (
	"net"
	"strings"

	"github.com/gin-gonic/gin"
)

// 获取请求方IP: 1）自行从HTTP头部获取；2）从框架接口获取；
// https://github.com/gin-gonic/gin/issues/2697
func GetClientIP(c *gin.Context) string {
	r := c.Request

	xRealIP := r.Header.Get("X-Real-Ip")
	if xRealIP != "" {
		return xRealIP
	}

	xForwardedFor := r.Header.Get("X-Forwarded-For")
	if xForwardedFor != "" {
		return xForwardedFor
	}

	return c.ClientIP()
}

// MyIpAddresses 获取本机IP地址
func LocalIpAddresses() (ips []string) {
	if addrArr, err := net.InterfaceAddrs(); nil != err {
		return
	} else {
		for _, addr := range addrArr {
			if ipAddr, ok := addr.(*net.IPNet); ok && !ipAddr.IP.IsLoopback() {
				if v4 := ipAddr.IP.To4(); nil != v4 {
					v4Str := v4.String()
					if 0 < len(v4Str) {
						ips = append(ips, v4.String())
					}
				}
			}
		}
	}
	return
}

// MyMACs 获取本机MAC地址
func MyMACs() (macs []string) {
	if netInterfaceAddr, err := net.Interfaces(); nil != err {
		return
	} else {
		for _, netInterface := range netInterfaceAddr {
			mac := netInterface.HardwareAddr.String()
			if 0 < len(mac) {
				macs = append(macs, mac)
			}
		}
	}
	return
}

func IsHttp(src string) bool {
	return strings.HasPrefix(src, "http") || strings.HasPrefix(src, "https")
}
