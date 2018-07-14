##### 公用常量类

```
package com.congcong.common;

/**
 * 公用常量类
 */
public class EventLogConstants {

    /**
     * 事件枚举
     * lanunch事件：当用户第一次访问网站的时候触发该事件
     * pageview事件：当用户访问页面、刷新页面的时候触发该事件
     * chargeRequest事件：当用户下订单的时候触发该事件
     * chargesuccess事件：用户订单支付成功触发事件
     * chargerrefund事件：用户订单退款事件
     * event事件：当访客/用户触发业务定义的事件
     */
    public static enum EventEnum {
        LANUCH(1, "lanuch event", "e_l"),
        PAGEVIEW(2, "page view event", "e_pv"),
        CHARGEREQUEST(3, "charge request event", "e_crt"),
        CHARGESUCCESS(4, "charge success event", "e_cs"),
        CHARGEREFUND(5, "charge refund event", "e_r"),
        EVENT(6, "lanuch event", "e_e");

        //代表上述六个事件内部参数
        public final int id;
        public final String name;
        public final String alias;//别名

        EventEnum(int id, String name, String alias) {
            this.id = id;
            this.name = name;
            this.alias = alias;
        }

        /**
         * 根据别名获取原名
         *
         * @param alias
         * @return
         */
        public static EventEnum valueOfAlias(String alias) {
            for (EventEnum event : values()) {
                if (event.alias.equals(alias)) {
                    return event;
                }
            }
            return null;
//            throw new RuntimeException("该alias没有对应的枚举.alias" + alias);
        }
    }

    /**
     * hbase表相关
     */
    public static final String EVENT_LOG_HBASE_NAME = "event_logs";//表名
    public static final String EVENT_LOG_FAMILY_NAME = "info";//列簇

    /**
     * 日志收集字段常量
     */
    public static final String LOG_SEPARTOR = "\\^A";//分割符
    public static final String LOG_COLUMN_NAME_IP = "ip";//列名称
    public static final String LOG_COLUMN_NAME_SERVER_TIME = "s_time";
    public static final String LOG_COLUMN_NAME_VERSION = "ver";//版本号
    public static final String LOG_COLUMN_NAME_UUID = "u_ud";//用户/访客唯一标识符
    public static final String LOG_COLUMN_NAME_MEMBER_ID = "u_mid";//会员id，和业务系统一致
    public static final String LOG_COLUMN_NAME_SESSION_iD = "u_sd";//会话id
    public static final String LOG_COLUMN_NAME_CLIENT_TIME = "c_time";//客户端时间
    public static final String LOG_COLUMN_NAME_LANGUAGE = "l";
    public static final String LOG_COLUMN_NAME_USERAGENT = "b_iev";//浏览器信息useragent
    public static final String LOG_COLUMN_NAME_RESOLUTION = "b_rst";//分辨率
    public static final String LOG_COLUMN_NAME_CURRENT_URL = "p_url";//当前页面的url
    public static final String LOG_COLUMN_NAME_PREFFER_URL = "p_ref";//上一个url
    public static final String LOG_COLUMN_NAME_TITLE = "tt";//当前页面的标题
    public static final String LOG_COLUMN_NAME_PLATFORM_NAME = "pl";//平台名称

    /**
     * 订单相关字段
     */
    public static final String LOG_COLUMN_NAME_ORDER_ID = "oid";
    public static final String LOG_COLUMN_NAME_ORDER_NAME = "on";
    public static final String LOG_COLUMN_NAME_ORDER_CURRENCY_AMOUT = "cua";//订单支付总金额
    public static final String LOG_COLUMN_NAME_ORDER_CURRENCY_TYPE = "cut";//支付货币类型
    public static final String LOG_COLUMN_NAME_ORDER_PAYMENT_TYPE = "pt";//支付方式

    /**
     * 事件相关
     */
    public static final String LOG_COLUMN_NAME_EVENT_NAME = "en";//事件名称
    public static final String LOG_COLUMN_NAME_EVENT_CATEGORY = "ca";//事件类别
    public static final String LOG_COLUMN_NAME_EVENT_ACTION = "ac";//事件动作
    public static final String LOG_COLUMN_NAME_EVENT_START = "kv_";//事件的自定义属性
    public static final String LOG_COLUMN_NAME_EVENT_DURATION = "du";//事件持续时间

    /**
     * userAgent解析字段
     */
    public static final String LOG_COLUMN_NAME_BROWSER_NAME = "browser_name";//浏览器名称
    public static final String LOG_COLUMN_NAME_BROWSER_VERSION = "browser_version";//浏览器版本
    public static final String LOG_COLUMN_NAME_OS_NAME = "os_name";//操作系统名称
    public static final String LOG_COLUMN_NAME_OS_VERSION = "os_version";//操作系统版本

    /**
     * ip解析字段
     */
    public static final String LOG_COLUMN_NAME_COUNTRY = "country";//国家
    public static final String LOG_COLUMN_NAME_PROVINCE = "provice";//省
    public static final String LOG_COLUMN_NAME_CITY = "city";//市
}

```

