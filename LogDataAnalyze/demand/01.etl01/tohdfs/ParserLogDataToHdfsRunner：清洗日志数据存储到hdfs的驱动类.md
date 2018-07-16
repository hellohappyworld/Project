##### 清洗日志数据存储到hdfs的驱动类

```
package com.congcong.etl.tohdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * 清洗日志数据存储到hdfs的驱动类
 */
public class ParserLogDataToHdfsRunner implements Tool {
    public static final Logger logger = Logger.getLogger(ParserLogDataToHdfsRunner.class);
    public Configuration conf = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new ParserLogDataToHdfsRunner(), args);
        } catch (Exception e) {
            logger.warn("run parser log data exception:" + e);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = HBaseConfiguration.create();
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /**
     * 驱动方法
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //处理参数
        this.handleArgs(args, conf);

        //获取job
        Job job = Job.getInstance(conf, "parserLogDataToHdfs");
        job.setJarByClass(ParserLogDataToHdfsRunner.class);
        job.setMapperClass(ParserLogDataMapperToHdfs.class);
        job.setMapOutputKeyClass(LogDataWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        //设置输入路径
        this.setInputpath(job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 处理参数
     * yarn jar /home/wc.jar ... -d 2017-12-12
     *
     * @param args
     * @param conf
     */
    private void handleArgs(String[] args, Configuration conf) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-d".equals(args[i])) {
                if (i + 1 < args.length) {
                    date = args[i + 1];
                    break;
                }
            }
        }

        //将具体的时间设置到conf中
        conf.set("running_date", date);
    }

    /**
     * 设置输入路径
     *
     * @param job
     */
    public void setInputpath(Job job) {
        Configuration conf = job.getConfiguration();
        String date = conf.get("running_date");
        FileSystem fs = null;
        try {
            String[] fields = date.split("-");
            Path inpath = new Path("/logs/" + fields[1] + "/" + fields[2]);
            fs = FileSystem.get(conf);
            //判断输入数据的目录是否存在，如果存在则设置
            if (fs.exists(inpath)) {
                FileInputFormat.addInputPath(job, inpath);
            } else {
                throw new RuntimeException("your input path is not avliable.inputpath is:" + inpath.toString());
            }

            Path outpath = new Path("ods/month=" + fields[1] + "/day=" + fields[2]);
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);
        } catch (Exception e) {
            logger.warn("set Input path exception:" + e);
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
        }
    }
}

```

