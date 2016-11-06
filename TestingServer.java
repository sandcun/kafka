package sand.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class TestingServer  {
	private static String hostprot = "192.168.140.131:2181";
	private static int timeOut = 6000;
	public static String fartherPath = "/chroot";
	public static String leaderPath = fartherPath+"/leader";
	private String myPath = null;
	private ZooKeeper zk = null;

	public void init() throws IOException, KeeperException, InterruptedException{
		zk = new ZooKeeper(hostprot,timeOut,new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				//System.out.println("State : " + event.getState());
			}
			
		});
		String path = zk.create(TestingServer.leaderPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		setPath(path);

	}
	
	public ZooKeeper getZk(){
		return zk;
	}
	
	private void setPath(String path){
		myPath = path;
		//System.out.println("myPath :" + myPath);
	}
	
	/*
	public String getPath(){
		return myPath;
	}
	*/
	
	public int getSeq(){
		
		//System.out.println(myPath.substring(TestingServer.leaderPath.length()));
		return new Integer(myPath.substring(TestingServer.leaderPath.length())).intValue();
	}
	
	
	/**
	 * 
	 * @param zk zookeeper����
	 * @param mySeq ��ǰ�ڵ����
	 * @return �����ǰ�ڵ�Ϊ��С�ڵ��򷵻�Null,���򷵻�ǰ��ڵ�
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static String getPreNode(ZooKeeper zk, int mySeq) throws KeeperException, InterruptedException{
		//��ȡ���ڵ��µ������ӽڵ�
		List<String> list = zk.getChildren(fartherPath, null);
		//ȡǰ��ڵ�
		Iterator<String> it = list.iterator();
		String path=null;
		String res = null;
		int seqMax = 0;
		while(it.hasNext()){
			path = it.next();

			//System.out.println("path substring "+path.substring(6));
			int seq = new Integer(path.substring(6)).intValue();
			//С��mySeq�����ֵ
			if(seq > seqMax && seq < mySeq){
				seqMax = seq;
				res = fartherPath+"/"+path;
			}
		}
		//System.out.println("res:" + res);
		return res;
	}

}
