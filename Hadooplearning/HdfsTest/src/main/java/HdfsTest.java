import HdfsOperation.HdfsOperation;
public class HdfsTest {
    public static void main(String[] args) throws Exception {
        HdfsOperation h1 = new HdfsOperation();
        h1.readFileByAPI2("/user/hadoop/","test.txt");
        h1.mkdir("/user/hadoop/myhadoop/");
        h1.putFile("/user/hadoop/myhadoop/", "a.txt");
        h1.readFileByAPI("/user/hadoop/myhadoop/", "a.txt");
        h1.removeFile("/user/hadoop/myhadoop/");

    }
}
