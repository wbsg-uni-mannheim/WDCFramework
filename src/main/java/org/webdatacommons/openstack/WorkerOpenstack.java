package org.webdatacommons.openstack;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.jets3t.service.model.S3Object;
import org.webdatacommons.framework.io.CSVStatHandler;
import org.webdatacommons.framework.io.StatHandler;
import org.webdatacommons.framework.processor.FileProcessor;
import org.webdatacommons.framework.processor.ProcessingNode;

import com.rabbitmq.client.AMQP.Basic;
import com.rabbitmq.client.AMQP.Basic.Deliver;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Worker implementation of the {@link ProcessingNode}, which builds up for each
 * core on the system one thread. Each thread connects to the RabitMQ queuing service and
 * requests a file. The file is retrieved, and processed by a Processor which
 * than stores the data back to the blocking storage of the instance. The queue is signalized that the file is
 * done. 
 * @author Anna Primpeli
 */
public class WorkerOpenstack extends ProcessingNode {
	// the logger
	private static Logger log = Logger.getLogger(WorkerOpenstack.class);

	//information about the queue
	private final String queueIP = getOrCry("queueIP");
	private final String queuePort = getOrCry("queuePort");
	private final String queueUserName = getOrCry("queueUsername");
	private final String queuePassword = getOrCry("queuePassword");
	private final String queueVHost = getOrCry("queueVHost");
	private final String queueName = getOrCry("queueName"); 
	
	// the bucket where to get the data from
	private final String dataBucket = getOrCry("dataBucket");
	// the name of the processor class
	private final String processorClass = getOrCry("processorClass");
	// maximum limit of retries for a queue
	private final int retryLimit = Integer.parseInt(getOrCry("jobRetryLimit"));
	// handler for statistics
	private StatHandler dataStatHandler = null;
	// handler for errors
	private StatHandler errorStatHandler = null;

	// the actual worker thread.
	public static class WorkerThread extends Thread {
		private Timer timer = new Timer();
		int timeLimit = 0;

		public WorkerThread() {
		}

		public WorkerThread(int timeLimitMsec) {
			this.timeLimit = timeLimitMsec;
		}

		public void run() {
			WorkerOpenstack worker = new WorkerOpenstack();
			if (timeLimit < 1) {
				timeLimit = Integer.parseInt(worker.getOrCry("jobTimeLimit")) * 1000;
			}
			while (true) {
				// cancel all old timers
				boolean success = false;
				timer = new Timer();
				// set a new timer killing us after the specified time limit
				final WorkerThread t = this;
				timer.schedule(new TimerTask() {
					@Override
					public void run() {
						log.warn("Killing worker thread, timeout expired.");
						t.interrupt();
					}
				}, timeLimit);

				// start the worker - and let it work
				try {
					success = worker.getTaskAndProcess();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 

				// on failures sleep a bit
				timer.cancel();
				if (!success) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						log.warn("Interrupted", e);
					}
				}
			}
		}
	}

	public boolean getTaskAndProcess() throws IOException, TimeoutException {
		File tempInputFile = null;
		File unpackedFile = null;
		File tempOutputFile = null;
		String inputFileKey = "";
		CSVStatHandler threehundredHandler = null;

		//connect to the queue server
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(queueIP);
		factory.setPort(Integer.parseInt(queuePort));
		factory.setUsername(queueUserName);
		factory.setPassword(queuePassword);
		factory.setVirtualHost(queueVHost);	
				  
		Connection connection = factory.newConnection();

		final Channel channel = connection.createChannel();


		try {
			// receive task message from queue
			channel.basicQos(1);
			System.out.println("The queue contains:"+channel.messageCount(queueName));
			if (channel.messageCount(queueName) < 1 ){
				System.out.println("Queue is empty");
				log.warn("Queue is empty");
				return false;
			}
			
			GetResponse response = channel.basicGet(queueName, false);
			String message = "no message";
			if(response == null){
				System.out.println("No response from the queue");
				return false;
			}else{
				byte[] body = response.getBody();
				message = new String(body, "UTF-8");
				
			}
						
			/**
			 * messages which went back to the queue more than the amount of
			 * times defined in the configuration entry "jobRetryLimit" are
			 * discarded, probably contain nasty data we cannot parse.
			 * Leave it for now for OpenStack TODO
			 */

			/**
			 * retrieve data file from s3, and unpack it using gzip
			 */
			inputFileKey = message;
			log.info("Now working on " + inputFileKey);
			System.out.println("Now working on " + inputFileKey);
			/**
			 * get file with http request
			 */
			ReadableByteChannel gzippedWatFileBC = Channels.newChannel(new URL(getOrCry("commonCrawlPrefix")+inputFileKey).openStream());

			FileProcessor processor = (FileProcessor) Class.forName(
					processorClass).newInstance();
			Map<String, String> stats = processor.process(gzippedWatFileBC,
					inputFileKey);
		
			log.debug("Finished processing file " + inputFileKey);

			//acknowledge that you received the message so that it can be deleted from the queue
		    channel.basicAck(response.getEnvelope().getDeliveryTag(), true);
			channel.close();
			connection.close();
			return true;

		} catch (Exception e) {
			log.warn("Unable to finish processing ("
					+ e.getClass().getSimpleName() + ": " + e.getMessage()
					+ ")");
			log.debug("Stacktrace", e.fillInStackTrace());

			

		} finally {

			if (tempInputFile != null && tempInputFile.exists()) {
				tempInputFile.delete();
			}
			if (unpackedFile != null && unpackedFile.exists()) {
				unpackedFile.delete();
			}
			if (tempOutputFile != null && tempOutputFile.exists()) {
				tempOutputFile.delete();
			}

			if (threehundredHandler != null) {
				File statFile = threehundredHandler.getFile();
				if (statFile != null && statFile.exists()) {
					statFile.delete();
				}
			}
			
		}
		return false;
	}

	public void storeStreamToFile(InputStream in, File outFile)
			throws IOException {
		OutputStream out = new FileOutputStream(outFile);
		byte[] buffer = new byte[1024];
		int len;
		while ((len = in.read(buffer)) != -1) {
			out.write(buffer, 0, len);
		}
		out.close();
	}


	private static String getStackTrace(Throwable aThrowable) {
		final Writer result = new StringWriter();
		final PrintWriter printWriter = new PrintWriter(result);
		aThrowable.printStackTrace(printWriter);
		return result.toString();
	}

	public static class ThreadGuard extends Thread {
		private List<Thread> threads = new ArrayList<Thread>();
		// can set thread limit to one for debugging
		private int threadLimit = Runtime.getRuntime().availableProcessors();
		//private int threadLimit = 1;
		
		private int threadSerial = 0;
		private int waitTimeSeconds = 1;

		private Class<? extends Thread> threadClass;

		public ThreadGuard(Class<? extends Thread> threadClass) {
			this.threadClass = threadClass;
		}

		public void run() {
			while (true) {
				List<Thread> threadsCopy = new ArrayList<Thread>(threads);
				for (Thread t : threadsCopy) {
					if (!t.isAlive()) {
						log.warn("Thread " + t.getName() + " died.");
						threads.remove(t);
					}
				}
				while (threads.size() < threadLimit) {
					Thread newThread;
					try {
						newThread = threadClass.newInstance();
						newThread.setName("#" + threadSerial);
						threads.add(newThread);
						newThread.start();
						log.info("Started new WorkerThread, "
								+ newThread.getName());
						threadSerial++;
					} catch (Exception e) {
						log.warn("Failed to start new Thread of class "
								+ threadClass);
					}

				}
				try {
					Thread.sleep(waitTimeSeconds * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
		new ThreadGuard(WorkerThread.class).start();
	}

}
