package com.zhileiedu.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: wzl
 * @Date: 2020/2/19 14:42
 */
public class HdfsClient  {
	private String CONNECTURI = "hdfs://hadoop01:9000";
	private Configuration configuration;
	private FileSystem fs;

	@Before
	public void connectHaoop() throws URISyntaxException, IOException, InterruptedException {
		configuration = new Configuration();
		fs = FileSystem.get(new URI(CONNECTURI), configuration, "root");
	}

	@Test
	public void testMkDir() throws IOException, URISyntaxException, InterruptedException {
		// 创建目录
		fs.mkdirs(new Path("/yinchao"));
		fs.close();
	}

	@Test
	public void uploadFile() throws IOException { // 文件上传
		configuration.set("dfs.replication", "2"); // 设置副本数
		fs.copyFromLocalFile(new Path("D:\\工作软件\\linux系统\\ubuntu-16.04-server-amd64.iso"), new Path("/linux.exe"));
		fs.close();
		System.out.println("上传成功");
	}

	@Test
	public void downloadFileFromHdfs() throws IOException { // 文件下载
		Path dfsPath = new Path("/yinchao/one.exe");
		Path localPath = new Path("d:/hdfs.exe");
		fs.copyToLocalFile(false, dfsPath, localPath, true);
		fs.close();
	}

	@Test
	public void removeDfsDir() throws IOException { // 删除目录
		boolean isDeleted = fs.delete(new Path("/lege"), true);
		System.out.println(isDeleted);
	}

	@Test
	public void renameFile() throws IOException { // 重命名
		boolean rename = fs.rename(new Path("/yinchao/one.exe"), new Path("/yinchao/two.exe"));
		System.out.println(rename);
	}

	// 查看文件详情
	@Test
	public void testListFile() throws IOException {
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		while (files.hasNext()) {
			LocatedFileStatus status = files.next();
			System.out.println("文件名=" + status.getPath().getName());
			System.out.println("文件长度=" + status.getLen());
			System.out.println("权限=" + status.getPermission());
			System.out.println("分组=" + status.getGroup());

			// 块数据
			BlockLocation[] blockLocations = status.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				// 获取块存储的主节点
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
		}
		fs.close();
	}

	@Test
	public void isFileOrIsDiretor() throws IOException {
		FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : fileStatuses) {
			if (fileStatus.isDirectory()) {
				System.out.println("目录=" + fileStatus.getPath().getName());
			} else if (fileStatus.isFile()) {
				System.out.println("文件=" + fileStatus.getPath().getName());
			}
		}
		// 关闭咨询
		fs.close();
	}


	/**
	 * 自己用IO流操作hdfs
	 */
	@Test
	public void putFile() throws IOException {
		// 创建输入流
		FileInputStream fileInputStream = new FileInputStream("d:/one.txt");
		// 获取输出流
		FSDataOutputStream fsDataOutputStream = fs.create(new Path("/banhua.txt"));
		// 流对拷
		IOUtils.copyBytes(fileInputStream, fsDataOutputStream, configuration);

		IOUtils.closeStream(fileInputStream);
		IOUtils.closeStream(fsDataOutputStream);
		fs.close();
		System.out.println("success");
	}


	// 基于IO操作的文件下载
	@Test
	public void getFileFromHDFS() throws IOException {
		// 获取输入流
		FSDataInputStream open = fs.open(new Path("/banhua.txt"));
		// 获取输入流
		FileOutputStream fileOutputStream = new FileOutputStream(new File("d:/abc.txt"));
		// 执行拷备
		IOUtils.copyBytes(open, fileOutputStream, configuration);

		// 执行关闭
		IOUtils.closeStream(open);
		IOUtils.closeStream(fileOutputStream);
		fs.close();

	}


	// 1．需求：分块读取HDFS上的大文 下载第一块 128M
	@Test
	public void readFileSeek1() throws IOException {
		FSDataInputStream fsDataInputStream = fs.open(new Path("/hadoop-2.7.7.tar.gz"));
		FileOutputStream fileOutputStream = new FileOutputStream(new File("d:/hadoop-2.7.2.tar.gz.part1"));

		byte[] buffer = new byte[1024];

		for (int i = 0; i < 1024 * 128; i++) {
			fsDataInputStream.read(buffer);
			fileOutputStream.write(buffer);
		}

		IOUtils.closeStream(fsDataInputStream);
		IOUtils.closeStream(fileOutputStream);
		fs.close();
	}

	// 下载第二块
	@Test
	public void readFileSeek2() throws IOException {
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.7.tar.gz"));
		// 直接切换到128的位置
		fis.seek(1024 * 1024 * 128);

		// 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("d:/hadoop-2.7.2.tar.gz.part2"));

		// 5 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);

		// 6 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}

}
