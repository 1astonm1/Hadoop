import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WCMapReduce {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);    //实例化作业

        //设置job的各种属性
        job.setJarByClass(WCMapReduce.class);   //设置jar 类
        job.setJobName("WordCount");            //设置作业名称
        job.setInputFormatClass(TextInputFormat.class); //设置输入类型

        FileInputFormat.addInputPath(job, new Path(args[0]));   //添加输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //设置输出路径

        job.setReducerClass(WCReducer.class);    //设置reduce类
        job.setNumReduceTasks(1);               //设置reduce数量
        job.setOutputKeyClass(Text.class);      //设置输出key类型
        job.setOutputValueClass(IntWritable.class); //设置输出value类型

        job.setMapperClass(WCMapper.class);     //设置map类
        job.setMapOutputKeyClass(Text.class);   //设置map输出key类型
        job.setMapOutputValueClass(IntWritable.class);  //设置map输出value类型

        job.waitForCompletion(true);

    }
}
