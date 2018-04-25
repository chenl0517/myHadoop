package myHadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

public class HdfsDao {
	//HDFS访问地址  
    private static final String HDFS = "hdfs://192.168.109.200:9000/";  
  
    public HdfsDao(Configuration conf) {  
        this(HDFS, conf);  
    }  
  
    public HdfsDao(String hdfs, Configuration conf) {  
        this.hdfsPath = hdfs;  
        this.conf = conf;  
    }  
  
    //hdfs路径  
    private String hdfsPath;  
    //Hadoop系统配置  
    private Configuration conf;  
  
    //启动函数  
    public static void main(String[] args) throws IOException {  
        JobConf conf = config();  
        HdfsDao hdfs = new HdfsDao(conf);  
//        hdfs.mkdirs("/tmp/new/two");  
//        hdfs.ls("/tmp/new");  
        hdfs.createFile("/tmp/new/text.log", "Hello world!!");  
        hdfs.cat("/tmp/new/text.log");  
          
    }          
      
    //加载Hadoop配置文件  
    public static JobConf config(){  
//      return (JobConf) new Configuration();    
        JobConf conf = new JobConf(HdfsDao.class);  
        conf.setJobName("HdfsDAO");  
        conf.addResource("classpath:/hadoop/core-site.xml");  
        conf.addResource("classpath:/hadoop/hdfs-site.xml");  
        conf.addResource("classpath:/hadoop/mapred-site.xml");  
        return conf;  
    }  
      
    public void ls(String folder) throws IOException {  
        Path path = new Path(folder);  
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);  
        FileStatus[] list = fs.listStatus(path);  
        System.out.println("ls: " + folder);  
        System.out.println("==========================================================");  
        for (FileStatus f : list) {  
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isFile(), f.getLen());  
        }  
        System.out.println("==========================================================");  
        fs.close();  
    }  
      
    public void mkdirs(String folder) throws IOException {  
        Path path = new Path(folder);  
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);  
        if (!fs.exists(path)) {  
            fs.mkdirs(path);  
            System.out.println("Create: " + folder);  
        }  
        fs.close();  
    }  
      
    //删除文件  
    public void rmr(String folder) throws IOException {  
        Path path = new Path(folder);  
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);  
        fs.deleteOnExit(path);  
        System.out.println("Delete: " + folder);  
        fs.close();  
    }  
      
    public void copyFile(String local, String remote) throws IOException {  
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);  
        fs.copyFromLocalFile(new Path(local), new Path(remote));  
        System.out.println("copy from: " + local + " to " + remote);  
        fs.close();  
    }  
      
    public void cat(String remoteFile) throws IOException {  
        Path path = new Path(remoteFile);  
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);  
        FSDataInputStream fsdis = null;  
        System.out.println("cat: " + remoteFile);  
        try {    
            if(!fs.exists(path))  
                return;  
            fsdis =fs.open(path);  
            IOUtils.copyBytes(fsdis, System.out, 4096, false);    
          } finally {    
            IOUtils.closeStream(fsdis);  
            fs.close();  
          }  
    }  
      
    public void createFile(String file, String content) throws IOException {  
        Path path = new Path(file);  
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);  
        byte[] buff = content.getBytes();  
//        FSDataOutputStream os = null;  
        FSDataOutputStream os = null;  
          
        try {  
            if(!fs.exists(path))  
                fs.createNewFile(path);  
            os = fs.create(path);  
            os.write(buff, 0, buff.length);  
            System.out.println("Create: " + file);  
        } finally {  
            if (os != null)  
                os.close();  
        }  
        fs.close();  
    }  
      
}
