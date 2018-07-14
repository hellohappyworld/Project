在etl过程中，我们需要将我们收集得到的数据进行处理，包括：userAgent解析、ip地址解析、每一行的日志解析工具
在我们本次项目中ip解析采用的是纯真ip数据库，官网是http://www.cz88.net/
另外：ip解析可以采用淘宝提供的ip接口来进行解析
	地址：http://ip.taobao.com/
	接口：http://ip.taobao.com/service/getIpInfo.php?ip=[ip地址字串]
	      例如：http://ip.taobao.com/service/getIpInfo.php?ip=1.60.42.255

包括ip地址解析: 根据ip地址解析出所在国家、省、市
http://ip.taobao.com/service/getIpInfo.php?ip=219.148.177.187

userAgent解析:浏览器名字，浏览器版本，操作系统名字，操作系统版本


每一行的日志解析工具：如：192.168.216.1^A1256798789.123^A192.168.216.111^1.png?en=e_l&ver=1&u_ud=679f-dfsa-u789-dfaa
最终得到一个Map:
[
<ip,>
<s_time,>
<en,>
<ver,>
<u_ud,>
<country,>
<provice,>
<city,>
<browser_name,>
<browser_version,>
<os_name,>
<os_version,>
]