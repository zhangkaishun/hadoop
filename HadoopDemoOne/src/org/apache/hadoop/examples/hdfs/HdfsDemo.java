package org.apache.hadoop.examples.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HdfsDemo {

	private FileSystem fs = null;

	@Before
	public void init() {
		try {
			fs = FileSystem.get(new URI("hdfs://namenode:9000"),
					new Configuration(), "zhangkaishun");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 下载
	 */
	@Test
	public void download1() {
		try {
			fs.copyToLocalFile(new Path("/input/sf.txt"), new Path(
					"F:/hadoophdfs/download/sf.txt"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 下载
	 */
	@Test
	public void download2() {
		try {
			InputStream in = fs.open(new Path("/input/sf.txt"));
			OutputStream out = new FileOutputStream(new File(
					"F:/hadoophdfs/download2/sf.txt"));
			IOUtils.copyBytes(in, out, 2048, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 上传
	 */
	@Test
	public void upload() {
		try {
			fs.copyFromLocalFile(new Path("F:/hadoophdfs/sf.txt"), new Path(
					"/input/sf.txt"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 上传
	 */
	@Test
	public void upload2() throws IllegalArgumentException, IOException {
		OutputStream out = fs.create(new Path("/in/words"));
		InputStream in = new FileInputStream(new File(
				"F:/hadoophdfs/upload2.txt"));
		IOUtils.copyBytes(in, out, 2048, true);
	}

	/**
	 * 删除
	 */
	@Test
	public void delete() throws IllegalArgumentException, IOException {
		fs.delete(new Path("/in"), true);// 第二个参数代表是否循环删除
	}

	/**
	 * 创建目录
	 */
	@Test
	public void mkdirs() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/newdir"));
	}

	/**
	 * 创建文件
	 */
	@Test
	public void createNewFile() throws IllegalArgumentException, IOException {
		fs.createNewFile(new Path("/newdir/newfile.txt"));// 即使上级目录不存在
	}

	/**
	 * 列出某目录下所有文件
	 */
	@Test
	public void listFiles() throws FileNotFoundException,
			IllegalArgumentException, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(
				"/input"), false);// 第二个参数代表是否循环
		while (listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			System.out.println(next.getPath());
		}
	}
	/**
	 * 重命名
	 */
	@Test
	public void rename() throws IllegalArgumentException, IOException{
		fs.rename(new Path("/newdiw/newfile.txt"),new Path("/newdiw/newfileRename.txt"));
	}
	
	/**
	 * 移动文件 使用rename方法，改变路径位置
	 */
	@Test
	public void move() throws IllegalArgumentException, IOException{
		fs.rename(new Path("/newdiw/newfileRename.txt"),new Path("/newdir/newfileRename.txt"));
	}

}
