package sand.selector;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import sand.latch.LeaderLatch;
import sand.util.TestingServer;

/**
 * �ص�����
 * @author Ivan
 *
 */
public class LeaderSelector {

	private static TestingServer ts = null;
    private LeaderSelectorListener listener;
   
    /**
     * ����listener
     * @param listener
     */
    public void setListener(LeaderSelectorListener listener){
        this.listener=listener;
    }
    
    /**
     * ����click�¼�������listener
     */
    public void click() {
        listener.takeLeadership(this);
    }
    
	public boolean start() throws IOException, KeeperException, InterruptedException {
		ts = new TestingServer();
		ts.init();
		ZooKeeper zk = ts.getZk();
		//String myPath = ts.getPath();
		int mySeq = ts.getSeq();
		//��ȡǰ��ڵ�
		String preNode = ts.getPreNode(zk,mySeq);
		//System.out.println("preNode :" + preNode);
		//��С�ڵ�û��ǰ��ڵ㣬����Ϊleader
		if(preNode == null){
			System.out.println("I am leader, my LeaderSeq = "+mySeq);
			//�����¼�
			click();
			return true;
		}else{
			//�����ڵ��Ϊfollower�ڵ�
			System.out.println("I am follower, my LeaderSeq = "+mySeq);
			//����watcher��watchǰ��ڵ�
			System.out.println("I am watching : " + preNode);
			this.setWatcher(zk, mySeq, preNode);
		}

		return false;
	}
	
	private void setWatcher(final ZooKeeper zk,final int mySeq,String preNode) throws KeeperException, InterruptedException{
		Stat stat = zk.exists(preNode, new Watcher() {
			@Override
			public void process(WatchedEvent event){
				
				System.out.println(event.getPath() + "|" + event.getType().name());
				try{
					//ǰ��ڵ�ҵ���������ѡ��ǰ��ڵ�
					if(event.getType().name().equals("NodeDeleted")){
						String preNode = ts.getPreNode(zk,mySeq);
						//��ǰ��ڵ㣬��ǰΪ��С�ڵ㣬����Ϊleader
						if(preNode == null){
							System.out.println("I am leader and seq = "+mySeq);
							//�����¼�
							click();
						}else{
							//����watchǰ��ڵ�
							zk.exists(preNode, this);
							System.out.println("PreNode is down , change preNode as : " + preNode);								
						}
					}
					
				}catch(KeeperException | InterruptedException e){
					//
				}
			}
		}
		);
	}	

}
