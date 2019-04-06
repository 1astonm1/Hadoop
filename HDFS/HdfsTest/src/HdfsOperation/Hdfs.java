package HdfsOperation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class Hdfs {

    //读取
    //
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
    public void readFileByAPI(String dir, String fileName) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.129:9000/");
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path(dir+fileName);
        FSDataInputStream fsDataInputStream = fs.open(p);
        byte[] buf = new byte[1024];
        int len = -1;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while ((len = fsDataInputStream.read(buf)) != -1) {
            byteArrayOutputStream.write(buf, 0, len);
        }
        fsDataInputStream.close();
        byteArrayOutputStream.close();
        System.out.println(new String(byteArrayOutputStream.toByteArray()));
    }


    public void readFileByAPI2(String dir, String fileName) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.129:9000/");
        FileSystem fs = FileSystem.get(conf) ;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        Path p = new Path(dir+fileName);
        FSDataInputStream fis = fs.open(p);
        IOUtils.copyBytes(fis, baos, 1024);
        System.out.println(new String(baos.toByteArray()));
    }

    //在hdfs中创建文件夹
    public void mkdir(String dir) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.129:9000/");
        FileSystem fs = FileSystem.get(conf) ;
        fs.mkdirs(new Path(dir));
    }

    //上传文件
    public void putFile(String dir, String fileName) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.129:9000/");
        FileSystem fs = FileSystem.get(conf) ;
        FSDataOutputStream out = fs.create(new Path(dir+fileName));
        out.write("helloworld".getBytes());
        out.close();
    }
    //删除文件
    public void removeFile() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.129:9000/");
        FileSystem fs = FileSystem.get(conf) ;
        Path p = new Path("/user/hadoop/myhadoop");
        fs.delete(p, true);
    }

}
