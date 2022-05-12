package phone;


import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * @author xjm
 * @version 1.0
 * @date 2022-05-11 22:49
 */
public class FLowBean implements Writable {
    private long upflow;
    private long downflow;
    private long sumflow;

    //如果空参构造函数被覆盖，一定要显示定义一下，否则在反序列化时会抛出异常
    public FLowBean() {
    }

    public FLowBean(long upflow, long downflow) {
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = upflow + downflow;
    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    @Override
    public String toString() {
        return upflow + "\t" + downflow + "\t" + sumflow;
    }

    //序列化，将对象的字段信息写入输出流
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upflow);
        dataOutput.writeLong(downflow);
        dataOutput.writeLong(sumflow);
    }

    //反序列化，从输入流读取各字段的信息
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upflow = dataInput.readLong();
        downflow = dataInput.readLong();
        sumflow = dataInput.readLong();
    }
}
