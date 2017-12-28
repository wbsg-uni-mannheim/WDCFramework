package org.webdatacommons.framework;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetAddress;
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

import org.apache.log4j.Logger;
import org.jets3t.service.model.S3Object;
import org.webdatacommons.framework.io.AmazonStatHandler;
import org.webdatacommons.framework.io.CSVStatHandler;
import org.webdatacommons.framework.io.StatHandler;
import org.webdatacommons.framework.processor.FileProcessor;
import org.webdatacommons.framework.processor.ProcessingNode;

import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

/**
 * Worker implementation of the {@link ProcessingNode}, which builds up for each
 * core on the system one thread. Each thread connects to the SQS (AWS) and
 * requests a file. The file is retrieved, and processed by a Processor which
 * than stores the data back to S3. S3. The queue is signalized that the file is
 * done. In addition statistics about the processed files are store in SimpleDB
 * of AWS.
 * 
 * @author Robert Meusel
 * 
 */
public class Worker extends ProcessingNode {
	// the logger
	private static Logger log = Logger.getLogger(Worker.class);

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
			Worker worker = new Worker();
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
				success = worker.getTaskAndProcess();

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

	public boolean getTaskAndProcess() {
		File tempInputFile = null;
		File unpackedFile = null;
		File tempOutputFile = null;
		String inputFileKey = "";
		CSVStatHandler threehundredHandler = null;
		boolean messageIsDeleted = false;

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
				getQueueUrl()).withAttributeNames("ApproximateReceiveCount");

		try {
			// receive task message from queue
			receiveMessageRequest.setMaxNumberOfMessages(1);
			ReceiveMessageResult queueRes = getQueue().receiveMessage(
					receiveMessageRequest);
			if (queueRes.getMessages().size() < 1) {
				log.warn("Queue is empty");
				return false;
			}
			// get the message
			Message jobMessage = queueRes.getMessages().get(0);

			/**
			 * messages which went back to the queue more than the amount of
			 * times defined in the configuration entry "jobRetryLimit" are
			 * discarded, probably contain nasty data we cannot parse.
			 */

			if (Integer.parseInt(jobMessage.getAttributes().get(
					"ApproximateReceiveCount")) > retryLimit) {
				log.warn("Discarding message " + jobMessage.getBody());
				getQueue().deleteMessage(
						new DeleteMessageRequest(getQueueUrl(), jobMessage
								.getReceiptHandle()));

				// store this information in sdb about the message discard
				Map<String, String> statData = new HashMap<String, String>();
				statData.put("message", "Message Discarded");

				try {
					statData.put("node", InetAddress.getLocalHost()
							.getHostName());
				} catch (UnknownHostException e1) {
					// ignore
				}
				statData.put("file", jobMessage.getBody());
				statData.put("datetime", Calendar.getInstance().getTime()
						.toString());

				getErrorStatHandler().addStats(UUID.randomUUID().toString(),
						statData);
				getErrorStatHandler().flush();

				return false;
			}

			/**
			 * retrieve data file from s3, and unpack it using gzip
			 */
			inputFileKey = jobMessage.getBody();
			log.info("Now working on " + inputFileKey);

			/**
			 * get file from s3 and process with zipped arc.
			 */
			S3Object inputObject = getStorage().getObject(dataBucket,
					inputFileKey);
			ReadableByteChannel gzippedWatFileBC = Channels
					.newChannel(inputObject.getDataInputStream());

			FileProcessor processor = (FileProcessor) Class.forName(
					processorClass).newInstance();
			Map<String, String> stats = processor.process(gzippedWatFileBC,
					inputFileKey);

			/**
			 * force statistics being persisted
			 */
			getDataStatHandler().addStats(inputFileKey, stats);
			getDataStatHandler().flush();

			/**
			 * remove message from queue. If an Exception is thrown or the node
			 * dies before finishing its task, this does not occur and the
			 * message is re-queued for another node
			 */
			getQueue().deleteMessage(
					new DeleteMessageRequest(getQueueUrl(), jobMessage
							.getReceiptHandle()));
			messageIsDeleted = true;
			log.debug("Finished processing file " + inputFileKey);

			return true;

		} catch (Exception e) {
			log.warn("Unable to finish processing ("
					+ e.getClass().getSimpleName() + ": " + e.getMessage()
					+ ")");
			log.debug("Stracktrace", e.fillInStackTrace());

			// put error information into sdb for later analyis
			Map<String, String> statData = new HashMap<String, String>();
			statData.put("exception", e.getClass().getSimpleName());
			String message = e.getMessage();
			if (message == null) {
				message = e.getClass().getName();
			}
			statData.put("message", message);
			String st = getStackTrace(e);
			statData.put("stacktrace",
					st.substring(0, Math.min(1024, st.length())));

			try {
				statData.put("node", InetAddress.getLocalHost().getHostName());
			} catch (UnknownHostException e1) {
				// ignore
			}
			statData.put("file", inputFileKey);
			statData.put("datetime", Calendar.getInstance().getTime()
					.toString());

			getErrorStatHandler().addStats(UUID.randomUUID().toString(),
					statData);
			getErrorStatHandler().flush();

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
			if (!messageIsDeleted) {
				receiveMessageRequest.setVisibilityTimeout(0);
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

	public void setDataStatHandler(StatHandler h) {
		dataStatHandler = h;
	}

	public void setErrorStatHandler(StatHandler h) {
		errorStatHandler = h;
	}

	public StatHandler getDataStatHandler() {
		if (dataStatHandler == null) {
			dataStatHandler = new AmazonStatHandler(getDbClient(),
					getOrCry("sdbdatadomain"));
		}
		return dataStatHandler;
	}

	public StatHandler getErrorStatHandler() {
		if (errorStatHandler == null) {
			errorStatHandler = new AmazonStatHandler(getDbClient(),
					getOrCry("sdberrordomain"));
		}
		return errorStatHandler;
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
