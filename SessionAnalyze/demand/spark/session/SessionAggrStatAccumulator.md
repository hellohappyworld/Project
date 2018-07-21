#### SessionAggrStatAccumulator

```
package com.qf.sessionanalyze1705.spark.session;

import com.qf.sessionanalyze1705.constant.Constants;
import com.qf.sessionanalyze1705.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * 使用自定义的数据格式，比如现在要用String格式，可以自定义mode1
 * 自定义类必须要可序列化，可以基于这种特殊的格式，实现复杂的分布式累加计算
 * 每个task，被分布在集群中各个节点的Executor中运行，可以根据需求，task给Accumulator传入不同的值
 * 最后根据不同的值，做复杂的计算逻辑
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {
    /**
     * zero方法，主要是对数据做初始化
     * 在这里只返回一个值，在初始化中，所有范围区间的数量都为0
     * 各个范围区间的统计数量的拼接，还是以字符串拼接的方式：key=value|key=value|key=value
     *
     * @param initialValue
     * @return
     */
    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0|";
    }

    /**
     * 主要是实现v1初始化的连接字符串
     * v1就是zero中初始值
     * v2是在遍历session的过程中，判断某个session对应的区间，然后用Constants.TIME_PERIOD_1s_3s进行累加
     * 实现的就是在v1中v2对应的value，进行累加1，最后更新高初始字符串中
     *
     * @param v1
     * @param v2
     * @return
     */
    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }

    /**
     * 可以理解为和addAccumulator一样的逻辑
     *
     * @param r1
     * @param r2
     * @return
     */
    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }

    private String add(String v1, String v2) {
        //判断v1为空的话，直接返回v2
        if (StringUtils.isEmpty(v1)) {
            return v2;
        }
        //v1不为空的话，从v1中提取v2对应的值，然后累加1
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldValue != null) {
            //将范围区间原有的值转换int类型，再进行累加
            int newValue = Integer.valueOf(oldValue) + 1;
            //将v1中v2对应的值，更新为新的累加后的值
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }

        return v1;
    }

}

```

