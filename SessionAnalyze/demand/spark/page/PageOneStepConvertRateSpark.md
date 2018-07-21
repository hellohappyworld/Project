#### PageOneStepConvertRateSpark

```
package com.qf.sessionanalyze1705.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.qf.sessionanalyze1705.constant.Constants;
import com.qf.sessionanalyze1705.dao.IPageSplitConvertRateDAO;
import com.qf.sessionanalyze1705.dao.ITaskDAO;
import com.qf.sessionanalyze1705.dao.factory.DAOFactory;
import com.qf.sessionanalyze1705.domain.PageSplitConvertRate;
import com.qf.sessionanalyze1705.domain.Task;
import com.qf.sessionanalyze1705.util.DateUtils;
import com.qf.sessionanalyze1705.util.NumberUtils;
import com.qf.sessionanalyze1705.util.ParamUtils;
import com.qf.sessionanalyze1705.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单跳转化率
 */
public class PageOneStepConvertRateSpark {
    public static void main(String[] args) {
        //模板代码
        SparkConf conf=new SparkConf();
        conf.setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=SparkUtils.getSQLContext(sc.sc());

        //获取数据
        SparkUtils.mockData(sc,sqlContext);

        //查询任务，获取任务信息参数
        long taskId= ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE);

        ITaskDAO taskDAO= DAOFactory.getTaskDAO();
        Task task=taskDAO.findById(taskId);

        if (task==null){
            System.out.println("你给的taskId找不到相应的数据");
        }

        JSONObject taskParam=JSONObject.parseObject(task.getTaskParam());

        //查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRDD=SparkUtils.getActionRDDByDateRange(sqlContext,taskParam);

        //对用户访问行为数据做映射，将数据映射为：<sessionId,访问行为数据> session粒度的数据
        //因为用户访问页面切片的生成，是基于每个session的访问数据生成的
        //如果脱离了session，生成的页面切片是没有意义的
        JavaPairRDD<String,Row> sessionId2ActionRDD=getSessionId2ActionRDD(actionRDD);

        //缓存session粒度的行为数据
        sessionId2ActionRDD=sessionId2ActionRDD.cache();

        //因为需要拿到每个sessionId对应的访问行为数据，才能生成切片
        //所以需要对session粒度的基础数据做分组
        JavaPairRDD<String,Iterable<Row>> groupedSessionId2ActionRDD=
                sessionId2ActionRDD.groupByKey();

        //这个需求中关键的一步是：每个session的单跳页面切片的生成和页面流的匹配算法
        //返回的格式为：<split,1>
        JavaPairRDD<String,Integer> pageSplitRDD=
                generateAndMatchPageSplit(sc,groupedSessionId2ActionRDD,taskParam);

        //获取切片的访问量
        Map<String,Object> pageSplitPvMap=pageSplitRDD.countByKey();

        //获取起始页面的访问量
        long startPagePv=getStartPagePv(taskParam,groupedSessionId2ActionRDD);

        //计算目标页面的各个页面的切片转化率
        //返回类型为：Map<String,Double>     key=各个页面切片   value=页面切片对应的转化率
        Map<String,Double> convertRateMap=
                computePageSplitConvertRate(taskParam,pageSplitPvMap,startPagePv);

        //把页面切片转化率存入数据库
        persistConvertRate(taskId,convertRateMap);

        sc.stop();
    }

    /**
     * 存储页面切片转化率
     *
     * @param taskId
     * @param convertRateMap
     */
    private static void persistConvertRate(long taskId, Map<String, Double> convertRateMap) {
        //把页面流对应的切片拼接到buffer
        StringBuffer buffer=new StringBuffer();

        for (Map.Entry<String,Double> convertRateEntry:convertRateMap.entrySet()){
            //获取切片
            String pageSplit=convertRateEntry.getKey();
            //获取转化率
            double converRate=convertRateEntry.getValue();
            //拼接
            buffer.append(pageSplit+"_"+converRate+"|");
        }

        //获取拼接后的切片和转化率
        String convertRate=buffer.toString();

        //截取掉最后的"|"
        convertRate=convertRate.substring(0,convertRate.length()-1);

        PageSplitConvertRate pageSplitConvertRate=new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskId);
        pageSplitConvertRate.setConvertRate(convertRate);

        IPageSplitConvertRateDAO pageSplitConvertRateDAO=DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);
    }

    /**
     * 计算页面切片转化率
     *
     * @param taskParam
     * @param pageSplitPvMap
     * @param startPagePv
     * @return
     */
    private static Map<String,Double> computePageSplitConvertRate(JSONObject taskParam, Map<String, Object> pageSplitPvMap, long startPagePv) {
        //用于存储页面切片以及对应的转化率：key=各个页面切片，value=页面切片对应的转化率
        Map<String,Double> converRateMap=new HashMap<String, Double>();

        //获取页面流
        String[] targetPages=ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        //初始化上个页面切片的访问量
        long lastPageSplitPv=0L;

        //获取目标页面流中的各个页面切片和访问量
        for (int i=1;i<targetPages.length;i++){
            //获取页面切片
            String targetPageSplit=targetPages[i-1]+"_"+targetPages[i];

            //获取每个页面切片对应的访问量
            long targetPageSplitPv=Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));

            //初始化转化率
            double convertRate=0.0;

            //生成切片
            if (i==1){
                convertRate= NumberUtils.formatDouble(targetPageSplitPv/startPagePv,2);
            }else {
                convertRate=NumberUtils.formatDouble(targetPageSplitPv/lastPageSplitPv,2);
            }

            converRateMap.put(targetPageSplit,convertRate);

            lastPageSplitPv=targetPageSplitPv;
        }

        return converRateMap;
    }

    /**
     * 获取起始页面的访问量
     *
     * @param taskParam
     * @param groupedSessionId2ActionRDD
     * @return
     */
    private static long getStartPagePv(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> groupedSessionId2ActionRDD) {
        //取出使用者的页面流
        String targetPageFlow=ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW);

        //从页面流中获取起始页面id
        final Long startPageId=Long.valueOf(targetPageFlow.split(",")[0]);

        JavaRDD<Long> startPageRDD=groupedSessionId2ActionRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
            @Override
            public Iterable<Long> call(Tuple2<String, Iterable<Row>> tup) throws Exception {
                //用于存储每个session访问的起始页面id
                List<Long> list=new ArrayList<Long>();

                //获取对应的行为数据
                Iterator<Row> it=tup._2.iterator();
                while (it.hasNext()){
                    Row row=it.next();
                    long pageId=row.getLong(3);
                    if (pageId==startPageId){
                        list.add(pageId);
                    }
                }

                return list;
            }
        });

        return startPageRDD.count();
    }


    /**
     * 页面切片的生成和页面流匹配算法的实现
     *
     * @param sc
     * @param groupedSessionId2ActionRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String,Integer> generateAndMatchPageSplit(JavaSparkContext sc, JavaPairRDD<String, Iterable<Row>> groupedSessionId2ActionRDD, JSONObject taskParam) {
        //首先获取页面流
        String targetPageFlow=ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW);

        //把目标页面流广播到相应的Executor端
        final Broadcast<String> targetPageFlowBroadcast=
                sc.broadcast(targetPageFlow);

        return groupedSessionId2ActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tup) throws Exception {
                //用于存储切片;格式为：<split,1>
                List<Tuple2<String,Integer>> list=new ArrayList<Tuple2<String, Integer>>();

                //获取当前session对应的行为数据
                Iterator<Row> it=tup._2.iterator();

                //获取使用者指定的页面流
                String[] targetPages=targetPageFlowBroadcast.value().split(",");

                /**
                 * 到这里，session粒度的访问数据已经获取到了
                 * 但默认情况下并没有按照时间排序
                 * 在实现转化率的时候需要把数据按照时间顺序进行排序
                 */
                //把访问行为数据放到list里，为了便于排序
                List<Row> rows=new ArrayList<>();
                while (it.hasNext()){
                    rows.add(it.next());
                }

                //排序，可以用自定义排序，但此时用匿名内部类的方式自定义排序
                Collections.sort(rows, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String actionTime1=o1.getString(4);
                        String actionTime2=o2.getString(4);

                        Date date1= DateUtils.parseTime(actionTime1);
                        Date date2=DateUtils.parseTime(actionTime2);

                        return (int)(date1.getTime()-date2.getTime());
                    }
                });

                /**
                 * 生成页面切片以及页面流的匹配
                 */
                //定义一个上一个页面的id
                Long lastPageId=null;

                for (Row row:rows){
                    long pageId=row.getLong(3);
                    if (lastPageId==null){
                        lastPageId=pageId;
                        continue;
                    }

                    //生成一个页面切片
                    //比如该用户请求的页面是:1,2,4,5
                    //上面访问的页面id:lastPageId=1
                    //这个请求的页面是：2
                    //那么生成的页面切片就是:1_2
                    String pageSplit=lastPageId+"_"+pageId;

                    //对这个页面切片判断一下，是否在使用者指定的页面流中
                    for (int i=1;i<targetPages.length;i++){
                        //比如说：用户指定的页面流是：1，2,4,5
                        //遍历的时候，从索引1开始，就是从第二个页面开始
                        //那么第一个页面切片就是:1_2
                        String targetPageSplit=targetPages[i-1]+"_"+targetPages[i];
                        if (pageSplit.equals(targetPageSplit)){
                            list.add(new Tuple2<String, Integer>(pageSplit,1));
                            break;
                        }
                    }

                    lastPageId=pageId;
                }

                return list;
            }
        });
    }

    /**
     * 生成session粒度的用户访问行为数据
     *
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String,Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String sessionId=row.getString(2);
                return new Tuple2<String, Row>(sessionId,row);
            }
        });
    }

}

```

