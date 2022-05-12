package phone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xjm
 * @version 1.0
 * @date 2022-05-11 23:41
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FLowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phoneNum = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        FLowBean bean = new FLowBean(upFlow, downFlow);
        context.write(new Text(phoneNum), bean);
    }

}
