package phone;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;


/**
 * @author xjm
 * @version 1.0
 * @date 2022-05-10 22:34
 */
public class FlowDriver {
    public static void main(String[] args) throws Exception {
        //1、配置连接hadoop集群的参数
        Configuration conf = new Configuration();
        //2、获取job对象实例
        Job job = Job.getInstance(conf);
        job.setJobName("FlowDriver");
        //3、驱动
        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FLowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FLowBean.class);

//        设置任务的输入文件或路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
//        设置任务的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //8、提交job
        System.exit(job.waitForCompletion(true)?0:1);

    }
}

