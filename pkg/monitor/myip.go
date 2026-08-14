package monitor

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hi2shark/santaizi-agent/pkg/util"
)

var (
	cfList = []string{
		"https://blog.cloudflare.com/cdn-cgi/trace",
		"https://dash.cloudflare.com/cdn-cgi/trace",
		"https://developers.cloudflare.com/cdn-cgi/trace",
	}
	CachedIP, GeoQueryIP, CachedCountryCode string
	manualCountryCode                       bool
	httpClientV4                            = util.NewSingleStackHTTPClient(time.Second*20, time.Second*5, time.Second*10, false)
	httpClientV6                            = util.NewSingleStackHTTPClient(time.Second*20, time.Second*5, time.Second*10, true)
)

type publicAddress struct {
	IP  string
	Loc string
}

// ConfigureIPReport prepares IP probe clients, optional NIC binding, and a manual country/region code.
func ConfigureIPReport(iface, countryCode string) {
	iface = strings.TrimSpace(iface)
	CachedCountryCode = strings.TrimSpace(countryCode)
	manualCountryCode = CachedCountryCode != ""
	httpClientV4 = util.NewSingleStackHTTPClient(time.Second*20, time.Second*5, time.Second*10, false, iface)
	httpClientV6 = util.NewSingleStackHTTPClient(time.Second*20, time.Second*5, time.Second*10, true, iface)
	if iface == "" || agentConfig == nil {
		return
	}
	agentConfig.NICAllowlist = map[string]bool{iface: true}
}

// UpdateIP 按设置时间间隔更新IP地址的缓存
func UpdateIP(useIPv6CountryCode bool, period uint32) {
	for {
		util.Println(agentConfig.Debug, "正在更新本地缓存IP信息")
		wg := new(sync.WaitGroup)
		wg.Add(2)
		var ipv4, ipv6 publicAddress
		go func() {
			defer wg.Done()
			ipv4 = fetchIP(cfList, false)
		}()
		go func() {
			defer wg.Done()
			ipv6 = fetchIP(cfList, true)
		}()
		wg.Wait()

		if ipv4.IP == "" && ipv6.IP == "" {
			if period > 60 {
				time.Sleep(time.Minute)
			} else {
				time.Sleep(time.Second * time.Duration(period))
			}
			continue
		}

		applyPublicAddresses(ipv4, ipv6, useIPv6CountryCode)
		time.Sleep(time.Second * time.Duration(period))
	}
}

func applyPublicAddresses(ipv4, ipv6 publicAddress, useIPv6CountryCode bool) {
	if ipv4.IP == "" && ipv6.IP == "" {
		return
	}
	if ipv4.IP == "" || ipv6.IP == "" {
		CachedIP = fmt.Sprintf("%s%s", ipv4.IP, ipv6.IP)
	} else {
		CachedIP = fmt.Sprintf("%s/%s", ipv4.IP, ipv6.IP)
	}

	loc := ipv4.Loc
	if ipv6.IP != "" && (useIPv6CountryCode || ipv4.IP == "") {
		GeoQueryIP = ipv6.IP
		loc = ipv6.Loc
	} else {
		GeoQueryIP = ipv4.IP
	}
	if manualCountryCode {
		return
	}
	if code := normalizeTraceLoc(loc); code != "" {
		CachedCountryCode = code
	}
}

func fetchIP(servers []string, isV6 bool) publicAddress {
	var found publicAddress
	var resp *http.Response
	var err error

	// 双栈支持参差不齐，不能随机请求，有些 IPv6 取不到 IP
	for i := 0; i < len(servers); i++ {
		if isV6 {
			resp, err = httpGetWithUA(httpClientV6, servers[i])
		} else {
			resp, err = httpGetWithUA(httpClientV4, servers[i])
		}
		// 遇到单栈机器提前退出
		if err != nil && strings.Contains(err.Error(), "no route to host") {
			return found
		}
		if err == nil {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				continue
			}
			resp.Body.Close()
			ip, loc := parseCloudflareTrace(string(body))
			// 没取到 v6 IP
			if isV6 && !strings.Contains(ip, ":") {
				continue
			}
			// 没取到 v4 IP
			if !isV6 && !strings.Contains(ip, ".") {
				continue
			}
			found.IP = ip
			found.Loc = loc
			return found
		}
	}
	return found
}

func parseCloudflareTrace(body string) (ip, loc string) {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "ip="):
			ip = strings.TrimPrefix(line, "ip=")
		case strings.HasPrefix(line, "loc="):
			loc = strings.TrimPrefix(line, "loc=")
		}
	}
	return ip, loc
}

func normalizeTraceLoc(loc string) string {
	code := strings.ToLower(strings.TrimSpace(loc))
	if len(code) != 2 || code == "xx" || code == "t1" {
		return ""
	}
	for _, r := range code {
		if r < 'a' || r > 'z' {
			return ""
		}
	}
	return code
}

func httpGetWithUA(client *http.Client, url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", util.MacOSChromeUA)
	return client.Do(req)
}
