##### 将hdfs中的数据清洗之后存储到hdfs，以便后续hive映射做数据仓库分析

```
package com.congcong.etl.tohdfs;

import com.congcong.common.EventLogConstants;
import com.congcong.etl.util.LogUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 将hdfs中的数据清洗之后存储到hdfs，以便后续hive映射做数据仓库分析
 */
public class ParserLogDataMapperToHdfs extends Mapper<LongWritable, Text, LogDataWritable, NullWritable> {
    public static final Logger logger = Logger.getLogger(ParserLogDataMapperToHdfs.class);
    //定义输入行数，输出行数，被过滤行数
    public static int inputRecords, ouputRecords, filterRecords;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        inputRecords++;
        logger.info("parse log data is:" + value.toString());
        try {
            Map<String, String> clientInfo = LogUtil.handleLog(value.toString());
            //判断clientInfo是否为空
            if (clientInfo.isEmpty()) {
                filterRecords++;
                return;
            }
            //代码走到这儿，肯定有可用的k-v
            String eventName = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);
            EventLogConstants.EventEnum type = EventLogConstants.EventEnum.valueOf(eventName);
            switch (type) {
                case LANUCH:
                case PAGEVIEW:
                case CHARGEREQUEST:
                case CHARGESUCCESS:
                case CHARGEREFUND:
                case EVENT:
                    handleLogToHdfs(clientInfo, context);
                    break;
                default:
                    filterRecords++;
                    logger.warn("this wvent is not handle!!!" + eventName);
                    break;
            }
        } catch (Exception e) {
            logger.warn("parser logdata exception:" + e);
            filterRecords++;
        }
    }

    /**
     * 将clientInfo的k-v输出到hdfs中
     *
     * @param clientInfo
     * @param context
     */
    private void handleLogToHdfs(Map<String, String> clientInfo, Context context) {
        try {
            LogDataWritable ld = new LogDataWritable();
            for (Entry<String, String> en : clientInfo.entrySet()) {
                switch (en.getKey()) {
                    case "s_time":
                        ld.setS_time(en.getValue());
                        break;
                    case "en":
                        ld.setEn(en.getValue());
                        break;
                    case "ver":
                        ld.setVer(en.getValue());
                        break;
                    case "u_ud":
                        ld.setU_ud(en.getValue());
                        break;
                    case "u_mid":
                        ld.setU_mid(en.getValue());
                        break;
                    case "c_time":
                        ld.setC_time(en.getValue());
                        break;
                    case "language":
                        ld.setLanguage(en.getValue());
                        break;
                    case "b_iev":
                        ld.setB_iev(en.getValue());
                        break;
                    case "b_rst":
                        ld.setB_rst(en.getValue());
                        break;
                    case "p_url":
                        ld.setP_url(en.getValue());
                        break;
                    case "p_ref":
                        ld.setP_ref(en.getValue());
                        break;
                    case "tt":
                        ld.setTt(en.getValue());
                        break;
                    case "pl":
                        ld.setPl(en.getValue());
                        break;
                    case "o_id":
                        ld.setO_id(en.getValue());
                        break;
                    case "on":
                        ld.setOn(en.getValue());
                        break;
                    case "cut":
                        ld.setCut(en.getValue());
                        break;
                    case "cua":
                        ld.setCua(en.getValue());
                        break;
                    case "pt":
                        ld.setPt(en.getValue());
                        break;
                    case "ca":
                        ld.setCa(en.getValue());
                        break;
                    case "ac":
                        ld.setAc(en.getValue());
                        break;
                    case "kv_":
                        ld.setKv_(en.getValue());
                        break;
                    case "du":
                        ld.setDu(en.getValue());
                        break;
                    case "os":
                        ld.setOs(en.getValue());
                        break;
                    case "os_v":
                        ld.setOs_v(en.getValue());
                        break;
                    case "browser":
                        ld.setBrowser(en.getValue());
                        break;
                    case "browser_v":
                        ld.setBrowser_v(en.getValue());
                        break;
                    case "country":
                        ld.setCountry(en.getValue());
                        break;
                    case "province":
                        ld.setProvince(en.getValue());
                        break;
                    case "city":
                        ld.setCity(en.getValue());
                        break;
                    default:
                        break;
                }
            }
            //输出
            context.write(ld, NullWritable.get());
        } catch (IOException | InterruptedException e) {
//            e.printStackTrace();
            logger.warn("写数据到hdfs异常", e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("total inputRecords:" + inputRecords + " total outputRecords:" + ouputRecords + " total filterRecords:" + filterRecords);
    }
}

```

