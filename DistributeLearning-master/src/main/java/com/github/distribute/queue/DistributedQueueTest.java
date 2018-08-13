package com.github.distribute.queue;

import java.io.Serializable;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;

public class DistributedQueueTest {

	public static void main(String[] args) {
		ZkClient zkClient = new ZkClient("127.0.0.1", 5000, 5000, new SerializableSerializer());
		DistributedSimpleQueue<SendObject> queue = new DistributedSimpleQueue<SendObject>(zkClient, "/Queue");
		//5�������̳߳�������
		for(int i=0;i<5;i++){
			new Thread(new ProducerThread(queue)).start();	
		}
		
		//5�������̳߳�������
		for(int i=0;i<5;i++){
			new Thread(new ConsumerThread(queue)).start();
		}
		
		
	}

}

	class ConsumerThread implements Runnable {
		private DistributedSimpleQueue<SendObject> queue;
	
		public ConsumerThread(DistributedSimpleQueue<SendObject> queue) {
			this.queue = queue;
		}
	
		public void run() {
			for (int i = 0; i < 10000; i++) {
				try {
					Thread.sleep((int) (Math.random() * 5000));// ���˯��һ��
					SendObject sendObject = (SendObject) queue.pollHuang();
					System.out.println("����һ����Ϣ�ɹ���" + sendObject);
				} catch (Exception e) {
				}
			}
		}
	}
	
	class ProducerThread implements Runnable {
	
		private DistributedSimpleQueue<SendObject> queue;
	
		public ProducerThread(DistributedSimpleQueue<SendObject> queue) {
			this.queue = queue;
		}
	
		public void run() {
			for (int i = 0; i < 10000; i++) {
				try {
					Thread.sleep((int) (Math.random() * 5000));// ���˯��һ��
					SendObject sendObject = new SendObject(String.valueOf(i), "content" + i);
					if(queue.offer(sendObject)){
						System.out.println("����һ����Ϣ�ɹ���" + sendObject);	
					}else{
						System.out.println("too many product");
					}
					
				} catch (Exception e) {
				}
			}
		}
	
	}
	
	class SendObject implements Serializable {
	
		private static final long serialVersionUID = 1L;
	
		public SendObject(String id, String content) {
			this.id = id;
			this.content = content;
		}
	
		private String id;
	
		private String content;
	
		public String getId() {
			return id;
		}
	
		public void setId(String id) {
			this.id = id;
		}
	
		public String getContent() {
			return content;
		}
	
		public void setContent(String content) {
			this.content = content;
		}
	
		@Override
		public String toString() {
			return "SendObject [id=" + id + ", content=" + content + "]";
		}
	
	}
