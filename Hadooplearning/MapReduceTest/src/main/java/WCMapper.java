import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text keyOut = new Text();       //输出key
        IntWritable valueOut = new IntWritable();   //输出value
        String arr[] = value.toString().split(" "); //把读出来的value分割
        for(String s:arr){
            keyOut.set(s);  //写入key
            valueOut.set(1);    //写入value
            context.write(keyOut, valueOut);    //用context输出
        }
    }
}
