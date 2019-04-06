import HdfsOperation.Hdfs;
public class HdfsTest {
    public static void main(String[] args) throws Exception {
        Hdfs h1 = new Hdfs();
        h1.readFile("/user/hadoop/", "test.txt");

    }
}
