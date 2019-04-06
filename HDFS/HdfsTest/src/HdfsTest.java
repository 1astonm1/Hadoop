import HdfsOperation.Hdfs;
public class HdfsTest {
    public static void main(String[] args) throws Exception {
        Hdfs h1 = new Hdfs();
        h1.readFileByAPI("/user/hadoop/","test.txt");
        h1.readFileByAPI2("/user/hadoop/","test.txt");
        h1.mkdir("/user/hadoop/myhadoop/");
        h1.putFile("/user/hadoop/myhadoop/", "a.txt");
        h1.readFile("/user/hadoop/myhadoop/", "a.txt");
        h1.removeFile("/user/hadoop/myhadoop/");
    }
}
