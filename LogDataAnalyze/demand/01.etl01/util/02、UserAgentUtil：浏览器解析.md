##### 01、UserAgentUtil:浏览器代理对象userAgent的解析

```
package com.congcong.etl.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 浏览器代理对象userAgent的解析
 */
public class UserAgentUtil {
    private static final Logger logger = Logger.getLogger(UASparser.class);

    //用于解析功能的工具类
    public static UASparser uaSparser = null;

    /**
     * 静态代码块获取
     */
    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.warn("获取uasparser对象异常", e);
        }
    }

    /**
     * 用于解析用户userAgent的方法
     *
     * @param userAgent
     * @return
     */
    public static UserAgentInfo parserUserAgent(String userAgent) {
        //用于返回解析后的属性
        UserAgentInfo ui = null;
        //判断userAgent是否为空
        if (StringUtils.isEmpty(userAgent)) {
            return ui;
        }

        //正常解析
        try {
            cz.mallat.uasparser.UserAgentInfo info = uaSparser.parse(userAgent);
            if (info != null) {
                //添加
                ui = new UserAgentInfo();
                ui.setBrowserName(info.getUaFamily());
                ui.setBrowseVersion(info.getBrowserVersionInfo());
                ui.setOsName(info.getOsFamily());
                ui.setOsVersion(info.getOsName());
            }
        } catch (IOException e) {
            logger.warn("userAgent解析异常", e);
        }
        return ui;
    }

    /**
     * 用于封装解析后的属性
     */
    public static class UserAgentInfo {
        private String browserName;
        private String browseVersion;
        private String osName;
        private String osVersion;

        public UserAgentInfo() {
        }

        public UserAgentInfo(String browserName, String browseVersion, String osName, String osVersion) {
            this.browserName = browserName;
            this.browseVersion = browseVersion;
            this.osName = osName;
            this.osVersion = osVersion;
        }

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowseVersion() {
            return browseVersion;
        }

        public void setBrowseVersion(String browseVersion) {
            this.browseVersion = browseVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browseVersion='" + browseVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }
    }
}

```

##### 2、测试类

```
import com.congcong.etl.util.UserAgentUtil;

public class UserAgentTest {
    public static void main(String[] args) {
        System.out.println(UserAgentUtil.parserUserAgent("Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Mobile Safari/537.36"));
        System.out.println(UserAgentUtil.parserUserAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36"));
        System.out.println(UserAgentUtil.parserUserAgent("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)"));
        System.out.println(UserAgentUtil.parserUserAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36"));
    }
}


测试结果：
UserAgentInfo{browserName='Chrome Mobile', browseVersion='58.0.3029.110', osName='Android', osVersion='Android'}
UserAgentInfo{browserName='Chrome', browseVersion='31.0.1650.63', osName='Windows', osVersion='Windows 7'}
UserAgentInfo{browserName='IE', browseVersion='8.0', osName='Windows', osVersion='Windows 7'}
UserAgentInfo{browserName='Chrome', browseVersion='46.0.2490.71', osName='Windows', osVersion='Windows 7'}
```

