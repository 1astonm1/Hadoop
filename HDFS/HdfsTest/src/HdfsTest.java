import HdfsOperation.Hdfs;
public class HdfsTest {
    public static void main(String[] args) throws Exception {
        Hdfs h1 = new Hdfs();
        h1.readFileByAPI();
        h1.readFileByAPI2();
        h1.mkdir();
        h1.putFile();
        h1.readFile("/user/hadoop/myhadoop/", "a.txt");
        h1.removeFile();
    }
}
