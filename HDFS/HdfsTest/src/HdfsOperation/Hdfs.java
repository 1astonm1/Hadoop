package HdfsOperation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class Hdfs {

    //读取
    public void readFile(String dir, String fileName) throws Exception {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        URL url = new URL("Hdfs://192.168.2.129:9000"+dir+fileName);

        URLConnection conn = url.openConnection();
        InputStream is = conn.getInputStream();
        byte[] buf = new byte[is.available()];
        is.read(buf);
        is.close();
        String str = new String(buf);
        System.out.println(str);

    }

    //通过调用api读取文件
    public void readFileByAPI() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fd.defaultFS", "Hdfs://192.168.2.129:9000/");
        FileSystem fs = FileSystem.get(conf);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Path p = new Path("/user/hadoop/test.txt");
        FSDataInputStream fsDataInputStream = fs.open(p);
        IOUtils.copyBytes(fsDataInputStream,byteArrayOutputStream,1024);
        System.out.println(byteArrayOutputStream.toByteArray());
    }

}
