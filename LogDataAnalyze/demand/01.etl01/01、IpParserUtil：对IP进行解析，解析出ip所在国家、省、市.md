##### 1、对IP进行解析，解析出ip所在国家、省、市

```
package com.congcong.etl.util;

import com.alibaba.fastjson.JSONObject;
import com.congcong.etl.util.ip.IPSeeker;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * ip的解析工具类，最终该会调用父类ipSeeker
 * 如果ip为null,则返回null
 * 如果ip是国外的ip,则直接显示国家即可
 * 如果ip是国内，则直接显示国家、省、市
 */
public class IpParserUtil extends IPSeeker {

    private static final Logger logger = Logger.getLogger(IpParserUtil.class);

    //封装ip解析出来的国家省市信息
    private RegionInfo info = new RegionInfo();

    /**
     * 使用纯真的ip数据库解析ip
     *
     * @param ip
     * @return
     */
    public RegionInfo parserIp(String ip) {
        //判断ip是否为空
        if (StringUtils.isEmpty(ip) || StringUtils.isEmpty(ip.trim())) {
            return info;
        }

        try {
            //ip不为空，正常解析
            String country = super.getCountry(ip);
            if ("局域网".equals(country)) {
                info.setCountry("中国");
                info.setProvice("北京");
                info.setCity("昌平区");
            } else if (country != null && StringUtils.isNotEmpty(country)) {
                //查找省的位置
                info.setCountry("中国");
                int index = country.indexOf("省");
                if (index > 0) {
                    //设置省份
                    info.setProvice(country.substring(0, index + 1));
                    //判断是否有市
                    int index2 = country.indexOf("市");
                    if (index2 > 0) {
                        //设置市
                        info.setCity(country.substring(index + 1,
                                Math.min(index2 + 1, country.length())));
                    }
                } else {
                    //代码走到这儿，就代表没有省份，就是直辖市、自治区、特别行政区
                    String flag = country.substring(0, 2);
                    switch (flag) {
                        case "内蒙":
                            info.setProvice("内蒙古");
                            country = country.substring(3);
                            index = country.indexOf("市");
                            if (index > 0) {
                                info.setCity(country.substring(0,
                                        Math.min(index + 1, country.length())));
                            }
                            break;
                        case "广西":
                        case "西藏":
                        case "新疆":
                        case "宁夏":
                            info.setProvice(flag);
                            country = country.substring(2);
                            index = country.indexOf("市");
                            if (index > 0) {
                                info.setCity(country.substring(0,
                                        Math.min(index + 1, country.length())));
                            }
                            break;
                        case "北京":
                        case "上海":
                        case "重庆":
                        case "天津":
                            info.setProvice(flag + "市");
                            country = country.substring(3);
                            index = country.indexOf("区");
                            if (index > 0) {
                                char ch = country.charAt(index - 1);
                                if (ch != '小' && ch != '校' && ch != '军') {
                                    info.setCity(country.substring(0,
                                            Math.min(index + 1, country.length())));
                                }
                            }
                            //在直辖市中如果有县
                            index = country.indexOf("县");
                            if (index > 0) {
                                info.setCity(country.substring(0,
                                        Math.min(index + 1, country.length())));
                            }
                            break;
                        case "香港":
                        case "澳门":
                        case "台湾":
                            info.setProvice(flag + "特别行政区");
                            break;
                        default:
                            break;
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("解析ip工具方法异常", e);
        }

        return info;
    }

    public RegionInfo parserIp1(String url, String charset) throws Exception {

        RegionInfo info = new RegionInfo();
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(url);

        if (null == url || !url.startsWith("http")) {
            throw new Exception("请求地址格式不对");
        }

        //设置请求的编码方式
        if (null != charset) {
            method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=" + charset);
        } else {
            method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=" + "utf-8");
        }
        int statusCode = client.executeMethod(method);

        if (statusCode != HttpStatus.SC_OK) {
            //打印服务器返回的状态
            System.out.println("Method failed:" + method.getStatusLine());
        }

        //返回响应消息
        byte[] responseBody = method.getResponseBodyAsString().getBytes(method.getResponseCharSet());
        //在返回响应消息使用(utf-8或gb2312)
        String response = new String(responseBody, "utf-8");

        JSONObject jo = JSONObject.parseObject(response);
        JSONObject j = (JSONObject) jo.get("data");

        //释放连接
        method.releaseConnection();
        //设置省市
        info.setCountry(j.get("country").toString());
        info.setProvice(j.get("region").toString());
        info.setCity(j.get("city").toString() + "市");

        return info;
    }

    /**
     * 该类主要用于封装ip解析出来的国家省市信息
     */
    public static class RegionInfo {
        private static final String DEFAULT_VALUE = "unknow";
        private String country = DEFAULT_VALUE;
        private String provice = DEFAULT_VALUE;
        private String city = DEFAULT_VALUE;

        public RegionInfo() {
        }

        public RegionInfo(String country, String provice, String city) {
            this.country = country;
            this.provice = provice;
            this.city = city;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvice() {
            return provice;
        }

        public void setProvice(String provice) {
            this.provice = provice;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "RegionInfo{" +
                    "country='" + country + '\'' +
                    ", provice='" + provice + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }
    }

}

```

##### 2、测试类

```
import com.congcong.etl.util.ip.IPSeeker;

public class IpUtilTest {
    public static void main(String[] args) {
        System.out.println(IPSeeker.getInstance().getCountry("140.250.129.253"));
    }
}



控制台打印结果：
山东省莱芜市
```

