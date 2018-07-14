在etl过程中，我们需要将我们收集得到的数据进行处理，包括：userAgent解析、ip地址解析、服务器时间解析
在我们本次项目中ip解析采用的是纯真ip数据库，官网是http://www.cz88.net/
另外：ip解析可以采用淘宝提供的ip接口来进行解析
	地址：http://ip.taobao.com/
	接口：http://ip.taobao.com/service/getIpInfo.php?ip=[ip地址字串]
	      例如：http://ip.taobao.com/service/getIpInfo.php?ip=1.60.42.255

包括ip地址解析:
http://ip.taobao.com/service/getIpInfo.php?ip=219.148.177.187

userAgent解析:


服务器时间解析