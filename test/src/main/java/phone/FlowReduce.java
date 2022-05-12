package phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @author xjm
 * @version 1.0
 * @date 2022-05-11 23:41
 */
public class FlowReduce extends Reducer<Text, FLowBean, Text, FLowBean> {
    @Override
    protected void reduce(Text key, Iterable<FLowBean> values, Context context) throws IOException, InterruptedException {
        Iterator<FLowBean> iterator = values.iterator();
        long upFlow = 0;
        long downFlow = 0;
        while (iterator.hasNext()) {
            FLowBean bean = iterator.next();
            upFlow += bean.getUpflow();
            downFlow += bean.getDownflow();
        }
        FLowBean total = new FLowBean(upFlow, downFlow);
        context.write(key, total);
    }

}