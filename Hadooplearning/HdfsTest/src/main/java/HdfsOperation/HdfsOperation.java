package HdfsOperation;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class HdfsOperation {

    //读取
    //通过java自带函数实现
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
    //
    public void readFileByAPI(String dir, String fileName) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.129:9000/");     //写入hdfs参数（如果不写会用本地文件系统）
        //指明名称节点位置
        FileSystem fs = FileSystem.get(conf);       //文件系统实例化
        Path p = new Path(dir+fileName);       //hadoop中的路径类
        FSDataInputStream fsDataInputStream = fs.open(p);   //获得输入流
        byte[] buf = new byte[1024];        //定义缓冲区
        int len = -1;       //定义长度
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();  //用来写入读出来的数据
        while ((len = fsDataInputStream.read(buf)) != -1) {     //读到全部读完为止
            byteArrayOutputStream.write(buf, 0, len);   //写入读出来的数据
        }
        fsDataInputStream.close();
        byteArrayOutputStream.close();
        System.out.println(new String(byteArrayOutputStream.toByteArray()));
    }


    public void readFileByAPI2(String dir, String fileName) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.129:9000/");   //写hdfs配置
        FileSystem fs = FileSystem.get(conf) ;      //文件系统初始化
        ByteArrayOutputStream baos = new ByteArrayOutputStream();   //用来写入读出来的数据

        Path p = new Path(dir+fileName);    //配置地址
        FSDataInputStream fis = fs.open(p);     //获得输入流
        IOUtils.copyBytes(fis, baos, 1024);     //使用hadoop自带函数来实现上面的循环
        System.out.println(new String(baos.toByteArray()));
    }

    //在hdfs中创建文件夹
    public void mkdir(String dir) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.129:9000/");
        FileSystem fs = FileSystem.get(conf) ;
        fs.mkdirs(new Path(dir));   //新建文件夹命令
    }

    //上传文件
    public void putFile(String dir, String fileName) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.129:9000/");
        FileSystem fs = FileSystem.get(conf) ;
        FSDataOutputStream out = fs.create(new Path(dir+fileName));  //创建文件输出流
        out.write("helloworld".getBytes());     //写文件
        out.close();
    }
    //删除文件
    public void removeFile(String dir) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.129:9000/");
        FileSystem fs = FileSystem.get(conf) ;
        Path p = new Path(dir);
        fs.delete(p, true);
    }
}
