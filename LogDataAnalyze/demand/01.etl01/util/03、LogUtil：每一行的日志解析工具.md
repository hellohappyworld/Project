##### 每一行的日志解析工具

```
package com.congcong.etl.util;

import com.congcong.common.EventLogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 每一行的日志解析工具
 */
public class LogUtil {
    private static final Logger logger = Logger.getLogger(LogUtil.class);

    /**
     * 日志解析
     *
     * @param logText 日志格式：192.168.216.1^A1256798789.123^A192.168.216.111^1.png?en=e_l&ver=1&u_ud=679f-dfsa-u789-dfaa
     * @return
     */
    public static Map<String, String> handleLog(String logText) {
        Map<String, String> info = new ConcurrentHashMap<>();
        //判断logText是否为空
        if (StringUtils.isNotBlank(logText)) {
            String[] fields = logText.split(EventLogConstants.LOG_SEPARTOR);
            if (fields.length == 4) {
                //将字段存储到info中
                info.put(EventLogConstants.LOG_COLUMN_NAME_IP, fields[0]);
                info.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                        fields[1].replace(".", ""));//时间戳解析
                //将参数列表中的k-v解析存储到info中
                //获取？之后的参数列表
                int index = fields[3].indexOf("?");
                if (index > 0) {
                    //获取requestbody
                    String requestBody = fields[3].substring(index + 1);
                    //处理参数
                    handleParams(requestBody, info);
                    //解析ip
                    handleIp(info);
                    //解析userAgent
                    handleUserAgent(info);
                }
            }
        }

        return info;
    }


    /**
     * 解析userAgent
     *
     * @param info
     */
    private static void handleUserAgent(Map<String, String> info) {
        //获取userAgent的字段
        String userAgent = info.get(EventLogConstants.LOG_COLUMN_NAME_USERAGENT);

        if (StringUtils.isNotEmpty(userAgent)) {
            UserAgentUtil.UserAgentInfo userAgentInfo = UserAgentUtil.parserUserAgent(userAgent);
            if (userAgentInfo != null) {
                info.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, userAgentInfo.getBrowserName());
                info.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, userAgentInfo.getBrowseVersion());
                info.put(EventLogConstants.LOG_COLUMN_NAME_OS_NAME, userAgentInfo.getOsName());
                info.put(EventLogConstants.LOG_COLUMN_NAME_OS_VERSION, userAgentInfo.getOsVersion());
            }
        }
    }

    /**
     * 解析ip后存储到info中
     *
     * @param info
     */
    private static void handleIp(Map<String, String> info) {
        String ip = info.get(EventLogConstants.LOG_COLUMN_NAME_IP);
        if (StringUtils.isNotEmpty(ip)) {
            IpParserUtil.RegionInfo regionInfo = new IpParserUtil().parserIp(ip);
            if (regionInfo != null) {
                info.put(EventLogConstants.LOG_COLUMN_NAME_COUNTRY, regionInfo.getCountry());
                info.put(EventLogConstants.LOG_COLUMN_NAME_PROVINCE, regionInfo.getProvice());
                info.put(EventLogConstants.LOG_COLUMN_NAME_CITY, regionInfo.getCity());
            }
        }
    }

    /**
     * 处理参数列表
     *
     * @param requestBody
     * @param info
     */
    private static void handleParams(String requestBody, Map<String, String> info) {
        try {
            if (StringUtils.isNotBlank(requestBody) && !info.isEmpty()) {
                //拆分requestbody
                String params[] = requestBody.split("&");
                for (String param : params) {
                    int index = param.indexOf("=");
                    if (index > 0) {
                        //拆分param
                        String[] kvs = param.split("=");
                        //info
                        String k = kvs[0];
                        String v = URLDecoder.decode(kvs[1], "UTF-8");
                        if (StringUtils.isNotBlank(k)) {
                            info.put(k, v);
                        }
                    }
                }
            }
        } catch (UnsupportedEncodingException e) {
            logger.warn("value解析异常", e);
        }
    }

}

```

