package hadoopLearning;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;

public class Test
{
    public static void main(String[] args) throws Exception {
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(new File("access/input/hello.txt")));
        byte[] bytes = new byte[in.available()];
        in.read(bytes);
        System.out.println(new String(bytes));
    }
}
