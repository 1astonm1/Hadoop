package hdfs;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.hsqldb.error.Error;
import org.junit.Test;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class hdfs {

    //读取文件
    public static void readFile(String dir, String fileName) throws Exception {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        URL url = new URL("hdfs://192.168.2.129:9000"+dir+fileName);

        URLConnection conn = url.openConnection();
        InputStream is = conn.getInputStream();
        byte[] buf = new byte[is.available()];
        is.read(buf);
        is.close();
        String str = new String(buf);
        System.out.println(str);

    }

    //通过调用api读取文件

    public void readFileByAPI(){

    }


    public static void main(String[] args) throws Exception {
        readFile("/user/hadoop/", "test.txt");
    }

}