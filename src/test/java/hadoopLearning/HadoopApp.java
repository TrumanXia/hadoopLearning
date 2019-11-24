package hadoopLearning;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HadoopApp
{
    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), configuration, "hadoop");
    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
    }

    @Test
    public void mkdir() throws Exception {
        boolean result = fileSystem.mkdirs(new Path("/goo3d"));
        System.out.println(result);
    }

    @Test
    public void read() throws Exception {
        FSDataInputStream stream = fileSystem.open(new Path("/cdh_version.properties"));
        // byte[] bytes = new byte[1024];
        // while (stream.read(bytes) != -1) {
        // System.out.println(new String(bytes));
        // }
        // 或者
        IOUtils.copyBytes(stream, System.err, 1024);
    }

    @Test
    public void write() throws Exception {
        FSDataOutputStream out = fileSystem.create(new Path("/b.txt"));
        out.writeUTF("hello world");
        out.flush();
    }

    @Test
    public void getDefaultReplicationSet() {
        System.out.println(configuration.get("dfs.replication"));
    }

    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/b.txt");
        Path newPath = new Path("/c.txt");

        fileSystem.rename(oldPath, newPath);
    }

    @Test
    public void copyFromLocalFile() throws Exception {
        fileSystem.copyFromLocalFile(new Path("G:\\Temp\\vv.txt"), new Path("/wc"));
    }

    @Test
    public void uploadWithProgress() throws Exception {
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(
                new File("D:\\文件\\新手入门大数据  Hadoop基础与电商行为日志分析\\第3章 分布式文件系统HDFS\\3-26 HDFS API编程之列出文件夹下的所有内容.ev4")));

        FSDataOutputStream out = fileSystem.create(new Path("/xyg/ship.ev"), new Progressable()
        {
            public void progress() {
                System.out.print("=");
            }
        });

        IOUtils.copyBytes(in, out, 1024);
    }

    // 文件传来传去就可能出现问题
    @Test
    public void copyToLocal() throws Exception {
        // fileSystem.copyToLocalFile(new Path("/xyg/ship.ev"), new
        // Path("G:\\Temp\\ship.ev"));
//        fileSystem.copyToLocalFile(null, null);
        FSDataInputStream in = fileSystem.open(new Path("/data/trackinfo_20130721.data"));
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(new File("data/input/trackinfo_20130721.data")));
        IOUtils.copyBytes(in, out, 1024);
        out.flush();
    }

    @Test
    public void listAllFiles() throws Exception {
        FileStatus[] status = fileSystem.listStatus(new Path("/"));

        for (FileStatus fileStatus : status) {
            System.out.println(fileStatus.getPermission() + " " + fileStatus.getReplication() + " "
                    + fileStatus.getOwner() + " " + fileStatus.getGroup() + " " + fileStatus.getLen() + " "
                    + fileStatus.getAccessTime() + " " + fileStatus.getPath());
        }
    }

    @Test
    public void listFileBlock() throws Exception{
        FileStatus status = fileSystem.getFileStatus(new Path("/xyg/c.txt"));
        BlockLocation[] locations = fileSystem.getFileBlockLocations(status, 0, status.getLen());
        
        for (BlockLocation location : locations) {
            for (String host : location.getNames()) {
                System.out.println(host);
            }
        }
    }
    
    @Test
    public void printMapToFile() throws FileNotFoundException {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("1", "ss");
        map.put("2", "ss");
        
        System.err.println(map);
    }
    
    
}
