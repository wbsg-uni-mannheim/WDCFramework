package org.webdatacommons.structureddata.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;

import org.apache.tika.io.IOUtils;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.shared.FlexBuffer;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.webdatacommons.framework.Worker;
import org.webdatacommons.framework.Worker.ThreadGuard;
import org.webdatacommons.framework.Worker.WorkerThread;
import org.webdatacommons.framework.io.CSVStatHandler;
import org.webdatacommons.framework.io.StatHandler;
import org.webdatacommons.structureddata.extractor.RDFExtractor;
import org.webdatacommons.structureddata.processor.ArcProcessor;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class WorkerTest {

	@Ignore
	@Test
	public void allIsWellTest() throws ServiceException, IOException {
		// rig queue
		AmazonSQS sqs = mock(AmazonSQS.class);

		ReceiveMessageResult mres = new ReceiveMessageResult();
		Message m = new Message();
		m.getAttributes().put("ApproximateReceiveCount", "1");
		String filename = "someFile";
		m.setBody(filename);
		mres.getMessages().add(m);

		when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(
				mres);

		// rig storage
		RestS3Service s3 = mock(RestS3Service.class);
		S3Object object = mock(S3Object.class);

		// dummy file
		File inputFile = File.createTempFile("temp-arc-file", ".arc.gz");
		inputFile.deleteOnExit();

		/*
		 * a file we know has triples as input File inputFile = new
		 * File(.toString());
		 */

		InputStream fileData = this.getClass().getResourceAsStream(
				"/exampledata/-960461054.webpage");
		final ArcFileItem item = new ArcFileItem();
		item.setArcFileName(inputFile.getName());
		item.setArcFilePos(0);
		item.setMimeType("text/html");
		item.setUri("http://example.com/");
		item.setContent(new FlexBuffer(IOUtils.toByteArray(fileData), false));

		when(object.getDataInputStream()).thenReturn(
				new FileInputStream(inputFile));
		when(object.getDataInputFile()).thenReturn(inputFile);
		when(s3.getObject(any(String.class), any(String.class))).thenReturn(
				object);

		// rig extractor, we call the extractor with some dummy data, it should
		// provide some output to the workers output file.
		ArcProcessor p = mock(ArcProcessor.class);

		final Map<String, String> exRes = new HashMap<String, String>();
		exRes.put(ArcProcessor.PAGES_GUESSED_TRIPLES, "42");
		when(
				p.processArcData(any(ReadableByteChannel.class),
						any(String.class), any(RDFExtractor.class),
						any(StatHandler.class), false)).thenAnswer(
				new Answer<Map<String, String>>() {
					@Override
					public Map<String, String> answer(
							InvocationOnMock invocation) throws Throwable {
						RDFExtractor ex = (RDFExtractor) invocation
								.getArguments()[2];
						ex.extract(item);
						return exRes;
					}

				});

		StatHandler ds = mock(StatHandler.class);
		CSVStatHandler ps = mock(CSVStatHandler.class);
		when(ps.getFile()).thenReturn(File.createTempFile("foo", "bar"));
		StatHandler es = mock(StatHandler.class);

		Worker wm = new Worker();

		// overload the amazon services with our implementations
		wm.setQueue(sqs);
		wm.setQueueUrl("queue:test");

		wm.setStorage(s3);
		// FIXME wm.setProcessor(p);
		wm.setDataStatHandler(ds);
		// FIXME wm.setPageStatHandler(ps);
		wm.setErrorStatHandler(es);

		// run it!

		boolean res = wm.getTaskAndProcess();
		assertTrue(res);

		// now verify:

		// we got the message from the queue
		verify(sqs, times(1)).receiveMessage(any(ReceiveMessageRequest.class));

		// we tried to load the right object from s3
		verify(s3, times(1)).getObject(any(String.class), eq(filename));

		// the extractor was called with the correct file name
		verify(p, times(1)).processArcData(any(ReadableByteChannel.class),
				eq(filename), any(RDFExtractor.class), any(StatHandler.class), false);

		// the extractions statistics were stored
		verify(ds, times(1)).addStats(eq(filename), eq(exRes));
		verify(ds, times(1)).flush();

		// verify the data and stats were uploaded to s3
		verify(s3, times(2)).putObject(any(String.class), any(S3Object.class));

		// verify the message was taken out of the queue
		verify(sqs, times(1)).deleteMessage(any(DeleteMessageRequest.class));

		// verify no error has occurred
		verifyZeroInteractions(es);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void queueFailureTest() throws ServiceException, IOException {
		// rig queue
		AmazonSQS sqs = mock(AmazonSQS.class);
		when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(
				AmazonClientException.class);

		// rig storage
		RestS3Service s3 = mock(RestS3Service.class);

		StatHandler es = mock(StatHandler.class);

		Worker wm = new Worker();

		// replace the amazon services with our mock implementations
		wm.setQueue(sqs);
		wm.setQueueUrl("queue:test");
		wm.setStorage(s3);
		wm.setErrorStatHandler(es);

		// run it!

		boolean res = wm.getTaskAndProcess();
		assertFalse(res);

		verifyZeroInteractions(s3);

		// important: the message was not removed from the queue
		verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));

		// verify we wrote the exception to sqs
		verify(es, times(1)).addStats(any(String.class), any(Map.class));

	}

	@Test
	public void queueEmptyTest() throws ServiceException, IOException {
		// rig queue
		AmazonSQS sqs = mock(AmazonSQS.class);

		// empty result
		ReceiveMessageResult mres = new ReceiveMessageResult();
		when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(
				mres);

		// rig storage
		RestS3Service s3 = mock(RestS3Service.class);

		Worker wm = new Worker();

		// overload the amazon services with our implementations
		wm.setQueue(sqs);
		wm.setQueueUrl("queue:test");

		wm.setStorage(s3);

		// run it!

		boolean res = wm.getTaskAndProcess();
		assertFalse(res);

		verifyZeroInteractions(s3);

		// important: the message was not removed from the queue
		verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void storageFailureTest() throws ServiceException, IOException {
		// rig queue
		AmazonSQS sqs = mock(AmazonSQS.class);

		ReceiveMessageResult mres = new ReceiveMessageResult();
		Message m = new Message();
		m.getAttributes().put("ApproximateReceiveCount", "1");
		String filename = "someFile";
		m.setBody(filename);
		mres.getMessages().add(m);

		when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(
				mres);

		// rig storage
		RestS3Service s3 = mock(RestS3Service.class);
		when(s3.getObject(any(String.class), any(String.class))).thenThrow(
				AmazonServiceException.class);

		StatHandler es = mock(StatHandler.class);

		Worker wm = new Worker();

		// overload the amazon services with our implementations
		wm.setQueue(sqs);
		wm.setQueueUrl("queue:test");
		wm.setStorage(s3);
		wm.setErrorStatHandler(es);

		// run it!

		boolean res = wm.getTaskAndProcess();
		assertFalse(res);

		// important: the message was not taken from the queue
		verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));

		verify(es, times(1)).addStats(any(String.class), any(Map.class));
	}

	@Test
	public void timeLimitThreadTest() throws ServiceException, IOException,
			InterruptedException {
		WorkerThread t = new WorkerThread(1000);
		t.start();
		for (int i = 0; i < 20; i++) {
			assertTrue(t.isAlive());
			Thread.sleep(100);
			// t.interrupt();
		}

	}

	public static class TestThread extends Thread {
		public void run() {
			try {

				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Test
	public void threadGuardTest() {
		ThreadGuard t = new ThreadGuard(TestThread.class);
		t.start();
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
