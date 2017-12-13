package org.webdatacommons.framework.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.acl.gs.AllUsersGrantee;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.webdatacommons.framework.io.CSVExport;
import org.webdatacommons.framework.processor.ProcessingNode;
import org.webdatacommons.structureddata.extractor.RDFExtractor;
import org.webdatacommons.structureddata.processor.WarcProcessor;
import org.webdatacommons.structureddata.util.DataCollector;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ListDomainsRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;
import com.martiansoftware.jsap.UnspecifiedParameterException;

import au.id.jericho.lib.html.Segment;
import de.dwslab.dwslib.util.io.InputUtil;

public class Master extends ProcessingNode {

	/**
	 * This is the current code to gather n-quad files from S3 (which are a
	 * result of the extraction using the {@link WarcProcessor}. In case you
	 * need an own collector for your data, which does not only simply download
	 * the data to your local file system, you need to adapt the
	 * {@link DataThread#run()} method do serve your needs.
	 * 
	 * @author Robert Meusel (robert@informatik.uni-mannheim.de)
	 * 
	 */
	private class DataThread extends Observable implements Runnable {
		private S3Object object;
		private String name;
		private File file;
		private int i;
		private File dataDir;
		private int sizeLimitMb;
		private int length;
		private String resultBucket;

		private DataThread(S3Object object, int i, File dataDir,
				int sizeLimitMb, int length, String resultBucket) {
			this.object = object;
			this.name = "s3 " + object.getKey();
			this.i = i;
			this.dataDir = dataDir;
			this.sizeLimitMb = sizeLimitMb;
			this.length = length;
			this.resultBucket = resultBucket;
		}

		private DataThread(File object, int i, File dataDir, int sizeLimitMb,
				int length, String resultBucket) {
			this.file = object;
			this.name = "local " + object.getName();
			this.i = i;
			this.dataDir = dataDir;
			this.sizeLimitMb = sizeLimitMb;
			this.length = length;
			this.resultBucket = resultBucket;
		}

		@Override
		public void run() {
			try {

				log.info("Retrieving " + name + ", (" + i + "/" + length + ") ");

				// data file
				if (name.endsWith(".nq.gz")) {

					BufferedReader retrievedDataReader = null;
					if (file != null) {
						retrievedDataReader = InputUtil.getBufferedReader(file);
					} else {
						// now really download the file
						S3Object dataObject = getStorage().getObject(
								resultBucket, object.getKey());

						retrievedDataReader = new BufferedReader(
								new InputStreamReader(new GZIPInputStream(
										dataObject.getDataInputStream())));
					}
					String line;
					while (retrievedDataReader.ready()) {
						line = retrievedDataReader.readLine();
						Line l = parseLine(line);
						if (l == null) {
							continue;
						}
						if (!RDFExtractor.EXTRACTORS.contains(l.extractor)) {
							log.debug(l.quad + "/" + l.extractor
									+ " is strange...");
							continue;
						}
						OutputStream out = getOutput(l.extractor, dataDir,
								sizeLimitMb);
						out.write(new String(l.quad + "\n").getBytes("UTF-8"));
					}
					retrievedDataReader.close();
				}
			} catch (Exception e) {
				log.debug("Error in " + name, e);
			} finally {
				setChanged();
				notifyObservers();
			}
		}

	}

	// TODO move this to de.dwslab.dwslib.framework.Processor
	private class DataThreadHandler extends Thread implements Observer {
		private File dataDir;
		private int sizeLimitMb;
		private int threads = 0;
		private int threadLimit = 1;
		// if this is null, the S3 version is used
		private File localRawDataFolder = null;

		private DataThreadHandler(File dataDir, int sizeLimitMb) {
			this.dataDir = dataDir;
			this.sizeLimitMb = sizeLimitMb;
		}

		private DataThreadHandler(File dataDir, File localRawDataFolder,
				int sizeLimitMb, int threadLimit) {
			this.dataDir = dataDir;
			this.sizeLimitMb = sizeLimitMb;
			this.localRawDataFolder = localRawDataFolder;
			if (threadLimit > -1) {
				this.threadLimit = threadLimit;
			} else {
				this.threadLimit = Runtime.getRuntime().availableProcessors();
			}
		}

		@Override
		public void run() {
			dataDir.mkdirs();
			String resultBucket = getOrCry("resultBucket");
			try {
				if (localRawDataFolder != null) {

					File[] objects = localRawDataFolder.listFiles();
					int i = 0;
					for (File object : objects) {
						// check if there are already as many threads as cpu
						// cores
						while (threads > threadLimit) {
							Thread.sleep(50);
						}
						i++;
						// create a thread that handles this object
						DataThread dt = new DataThread(object, i, dataDir,
								sizeLimitMb, objects.length, resultBucket);
						dt.addObserver(this);
						Thread t = new Thread(dt);

						t.start();
						threads++;
					}

				} else {
					//Changed to work for the test bucket
					S3Object[] objects = getStorage().listObjects(resultBucket,
							"data/", null);

					int i = 0;

					for (S3Object object : objects) {
						// check if there are already as many threads as cpu
						// cores
						while (threads > threadLimit) {
							Thread.sleep(50);
						}
						i++;
						// create a thread that handles this object
						DataThread dt = new DataThread(object, i, dataDir,
								sizeLimitMb, objects.length, resultBucket);
						dt.addObserver(this);
						Thread t = new Thread(dt);

						t.start();
						threads++;
					}
				}

				// wait till all threads are finished
				while (threads > 0) {
					Thread.sleep(1000);
				}

				for (OutputStream os : outputWriters.values()) {
					if (os != null) {
						os.write("\n".getBytes());
						os.close();
					}
				}

			} catch (Exception e) {
				log.warn("Error: ", e);
			}
		}

		@Override
		public void update(Observable arg0, Object arg1) {
			threads--;
		}
	}

	private class DateSizeRecord {
		Date recordTime;
		Long queueSize;

		public DateSizeRecord(Date time, Long size) {
			this.recordTime = time;
			this.queueSize = size;
		}
	}

	public static class Line {
		public String quad;
		public String extractor;
	}

	Map<String, OutputStream> outputWriters = new HashMap<String, OutputStream>();

	Map<String, File> outputFiles = new HashMap<String, File>();

	/**
	 * NOTICE: This is a startup shell script for EC2 instances. It installs
	 * Java 8, downloads the Extractor JAR from S3 and launches it.
	 * 
	 * This is designed to work on the Ubuntu 14.04 LTS AMI ami-018c9568
	 */
	private final String startupScript = "#!/bin/bash \n echo 1 > /proc/sys/vm/overcommit_memory \n"
			+ " apt-get install -y python-software-properties debconf-utils \n"
			+ " add-apt-repository -y ppa:webupd8team/java \n "
			+ "apt-get update \n "
			+ "echo \"oracle-java8-installer shared/accepted-oracle-license-v1-1 select true\" | sudo debconf-set-selections \n "
			+ "sudo apt-get install -y oracle-java8-installer htop \n "
			+ "wget -O /tmp/start.jar \""
			+ getJarUrl()
			+ "\" \n java -Xmx"
			+ getOrCry("javamemory").trim()
			+ " -jar /tmp/start.jar > /tmp/start.log 2> /tmp/start_errors.log & \n";
	/**
	 * only run java
	 */
//	private final String startupScript = "#!/bin/bash \n echo 1 > /proc/sys/vm/overcommit_memory \n"
//			+ "wget -O /tmp/start.jar \""
//			+ getJarUrl()
//			+ "\" \n java -Xmx"
//			+ getOrCry("javamemory").trim()
//			+ " -jar /tmp/start.jar > /tmp/start.log & \n";

	// private final String startupScript =
	// "#!/bin/bash \n echo 1 > /proc/sys/vm/overcommit_memory \n aptitude update \n aptitude -y install java7-jdk htop \n wget -O /tmp/start.jar \""
	// + getJarUrl()
	// + "\" \n java -Xmx"
	// + getOrCry("javamemory").trim()
	// + " -jar /tmp/start.jar > /tmp/start.log & \n";

	private static Logger log = Logger.getLogger(Master.class);

	private static Set<String> getSdbAttributes(AmazonSimpleDBClient client,
			String domainName, int sampleSize) {
		if (!client.listDomains().getDomainNames().contains(domainName)) {
			throw new IllegalArgumentException("SimpleDB domain '" + domainName
					+ "' not accessible from given client instance");
		}

		int domainCount = client.domainMetadata(
				new DomainMetadataRequest(domainName)).getItemCount();
		if (domainCount < sampleSize) {
			throw new IllegalArgumentException("SimpleDB domain '" + domainName
					+ "' does not have enough entries for accurate sampling.");
		}

		int avgSkipCount = domainCount / sampleSize;
		int processedCount = 0;
		String nextToken = null;
		Set<String> attributeNames = new HashSet<String>();
		Random r = new Random();
		do {
			int nextSkipCount = r.nextInt(avgSkipCount * 2) + 1;

			SelectResult countResponse = client.select(new SelectRequest(
					"select count(*) from `" + domainName + "` limit "
							+ nextSkipCount).withNextToken(nextToken));

			nextToken = countResponse.getNextToken();

			processedCount += Integer.parseInt(countResponse.getItems().get(0)
					.getAttributes().get(0).getValue());

			SelectResult getResponse = client.select(new SelectRequest(
					"select * from `" + domainName + "` limit 1")
					.withNextToken(nextToken));

			nextToken = getResponse.getNextToken();

			processedCount++;

			if (getResponse.getItems().size() > 0) {
				for (Attribute a : getResponse.getItems().get(0)
						.getAttributes()) {
					attributeNames.add(a.getName());
				}
			}
		} while (domainCount > processedCount);
		return attributeNames;
	}

	// command line parameters, different actions
	public static void main(String[] args) throws JSAPException {
		// command line parser
		JSAP jsap = new JSAP();
		UnflaggedOption actionParam = new UnflaggedOption("action")
				.setStringParser(JSAP.STRING_PARSER).setRequired(true)
				.setGreedy(false);
		
		actionParam
				.setHelp("Action to perform, can be 'queue', 'clearqueue', 'cleardata', 'crawlstats', 'start', 'shutdown', 'deploy', 'retrievedata', 'retrievestats' and 'monitor'");
		jsap.registerParameter(actionParam);

		JSAPResult config = jsap.parse(args);
		String action = config.getString("action");

		// Prefix path of objects to be queued from bucket
		if ("queue".equals(action)) {
			FlaggedOption prefix = new FlaggedOption("prefix")
					.setStringParser(JSAP.STRING_PARSER).setRequired(false)
					.setLongFlag("bucket-prefix").setShortFlag('p');

			prefix.setHelp("Prefix path of objects to be queued from bucket");
			jsap.registerParameter(prefix);

			FlaggedOption limit = new FlaggedOption("limit")
					.setStringParser(JSAP.LONG_PARSER).setRequired(false)
					.setLongFlag("file-number-limit").setShortFlag('l');

			limit.setHelp("Limits number of objects to be queued from bucket");

			jsap.registerParameter(limit);

			FlaggedOption prefixFile = new FlaggedOption("prefixFile")
					.setStringParser(JSAP.STRING_PARSER).setRequired(false)
					.setLongFlag("bucket-prefix-file").setShortFlag('f');

			prefixFile
					.setHelp("File including line based prefix paths of objects to be queued from bucket");
			jsap.registerParameter(prefixFile);

			JSAPResult queueResult = jsap.parse(args);
			// if parsing was not successful print usage of commands and exit
			if (!queueResult.success()) {
				printUsageAndExit(jsap, queueResult);
			}
			Long limitValue = null;
			try {
				limitValue = queueResult.getLong("limit");
			} catch (UnspecifiedParameterException e) {
				// do nothing
			}
			String filePath = null;
			try {
				filePath = queueResult.getString("prefixFile");
			} catch (UnspecifiedParameterException e) {
				// do nothing
			}

			new Master().queue(queueResult.getString("prefix"), limitValue,
					filePath);
//			ArrayList<String> segments = new ArrayList<String>();
//			try {
//				segments = CommonCrawlSegments.getSegments("C:\\Users\\User\\workspace\\ExtractionFramework_WDC\\src\\main\\resources\\segments_2016.txt");
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			for (String s:segments)
//				new Master().queue("CC-MAIN-2016-44/segments/"+s+"/warc", null,
//						null);
			
			System.exit(0);
		}

		if ("clearqueue".equals(action)) {
			new Master().clearQueue();
			System.exit(0);
		}

		if ("cleardata".equals(action)) {
			Switch s3Deletion = new Switch("includeS3Storage").setLongFlag(
					"includeS3Storage").setShortFlag('r');
			s3Deletion
					.setHelp("Clear all data including extraction data from s3 storage.");
			jsap.registerParameter(s3Deletion);
			JSAPResult queueResult = jsap.parse(args);
			if (!queueResult.success()) {
				printUsageAndExit(jsap, queueResult);
			}
			boolean includeS3Storage = queueResult
					.getBoolean("includeS3Storage");
			new Master().clearData(includeS3Storage);
			System.exit(0);
		}
		if ("deletedata".equals(action)){
			new Master().deleteData("wdc-2016-data/deploy_data");
			System.exit(0);
		}
		if ("monitor".equals(action)) {

			Switch autoShutdown = new Switch("autoShutdown").setLongFlag(
					"autoShutdown").setShortFlag('a');
			autoShutdown
					.setHelp("Indicates if shutdown method is called if not messages are left to process and ec2 system is still running.");
			jsap.registerParameter(autoShutdown);
			JSAPResult queueResult = jsap.parse(args);
			if (!queueResult.success()) {
				printUsageAndExit(jsap, queueResult);
			}
			boolean auto = queueResult.getBoolean("autoShutdown");
			new Master().monitorQueue(auto);
			System.exit(0);
		}

		if ("crawlstats".equals(action)) {
			FlaggedOption prefixP = new FlaggedOption("prefix")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("bucket-prefix").setShortFlag('p');
			prefixP.setHelp("Prefix path of objects in bucket to calculate statistics for");
			jsap.registerParameter(prefixP);

			FlaggedOption outputP = new FlaggedOption("output")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("output-file").setShortFlag('o');
			outputP.setHelp("Path for CSV output file");
			jsap.registerParameter(outputP);

			JSAPResult statsResult = jsap.parse(args);
			if (!statsResult.success()) {
				printUsageAndExit(jsap, statsResult);
			}

			String prefix = statsResult.getString("prefix");
			String output = statsResult.getString("output");

			System.out
					.println("Calculating object count and size statistics for prefix "
							+ prefix + ", saving results to " + output);
			new Master().crawlStats(prefix, new File(output));
			System.out.println("Done.");
			System.exit(0);
		}

		if ("deploy".equals(action)) {
			FlaggedOption jarfileP = new FlaggedOption("jarfile")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("jarfile").setShortFlag('j');
			jarfileP.setHelp("Jarfile to be executed on the worker instances");
			jsap.registerParameter(jarfileP);

			JSAPResult statsResult = jsap.parse(args);
			if (!statsResult.success()) {
				printUsageAndExit(jsap, statsResult);
			}
			File jarfile = new File(statsResult.getString("jarfile"));
			if (!jarfile.exists() || !jarfile.canRead()) {
				log.warn("Unable to access JAR file at " + jarfile);
				System.exit(-1);
			}

			System.out.println("Deploying JAR file at " + jarfile);
			new Master().deploy(jarfile);
			System.exit(0);
		}

		if ("start".equals(action)) {
			FlaggedOption amountP = new FlaggedOption("amount")
					.setStringParser(JSAP.INTEGER_PARSER).setRequired(true)
					.setLongFlag("worker-amount").setShortFlag('a');
			amountP.setHelp("Amount of worker instances to start in EC2");
			jsap.registerParameter(amountP);

			FlaggedOption priceP = new FlaggedOption("pricelimit")
					.setStringParser(JSAP.DOUBLE_PARSER).setRequired(true)
					.setLongFlag("pricelimit").setShortFlag('p');
			priceP.setHelp("Price limit for instances in US$");
			jsap.registerParameter(priceP);

			JSAPResult startParams = jsap.parse(args);
			if (!startParams.success()) {
				printUsageAndExit(jsap, startParams);
			}

			int amount = startParams.getInt("amount");
			new Master().createInstances(amount,
					startParams.getDouble("pricelimit"));
			System.out.println("done.");
			System.exit(0);
		}

		if ("shutdown".equals(action)) {
			System.out
					.print("Cancelling spot request and shutting down all worker instances in EC2...");
			new Master().shutdownInstances();
			System.out.println("done.");
			System.exit(0);
		}

		if ("retrievedata".equals(action)) {
			FlaggedOption destinationDir = new FlaggedOption("destination")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("destination").setShortFlag('d');
			destinationDir.setHelp("Directory to write the extracted data to");
			jsap.registerParameter(destinationDir);

			FlaggedOption localDestinationDir = new FlaggedOption("localsource")
					.setStringParser(JSAP.STRING_PARSER).setRequired(false)
					.setLongFlag("localsource").setShortFlag('l');
			destinationDir.setHelp("Local Directory to retrieve data from.");
			jsap.registerParameter(localDestinationDir);

			FlaggedOption multiThreadMode = new FlaggedOption("multiThreadMode")
					.setStringParser(JSAP.INTEGER_PARSER).setRequired(false)
					.setLongFlag("multiThreadMode").setShortFlag('m');
			destinationDir.setHelp("Number of threads to use in parallel.");
			jsap.registerParameter(multiThreadMode);

			JSAPResult destinationConfig = jsap.parse(args);
			if (!destinationConfig.success()) {
				printUsageAndExit(jsap, destinationConfig);
			}
			File destinationDirectory = new File(
					destinationConfig.getString("destination"));
			int threads = 0;
			try {
				threads = destinationConfig.getInt("multiThreadMode");
				System.out.println("Running with " + threads + " Threads.");
			} catch (NullPointerException e) {
				System.out.println("Running in single thread mode.");
			}

			String localDir = null;
			try {
				localDir = destinationConfig.getString("localsource");
				System.out
						.println("Getting extracted triples from local dir to local disk...");
			} catch (NullPointerException e) {
				// nullpointer
				System.out
						.println("Getting extracted triples from cloud to local disk...");
			}

			Master m = new Master();
			m.retrieveData(destinationDirectory, localDir, 100, threads);

			System.out.println("done.");
			System.exit(0);
		}

		if ("retrievestats".equals(action)) {

			FlaggedOption destinationDir = new FlaggedOption("destination")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("destination").setShortFlag('d');
			destinationDir
					.setHelp("Directory to write the extracted statistics to");
			jsap.registerParameter(destinationDir);

			FlaggedOption mountDestinationDir = new FlaggedOption("mount")
					.setStringParser(JSAP.STRING_PARSER).setRequired(false)
					.setLongFlag("mountdestination");
			mountDestinationDir
					.setHelp("Mounted Directory to write the extracted statistics to after writing them local. Local copy is removed.");
			jsap.registerParameter(mountDestinationDir);

			JSAPResult destinationConfig = jsap.parse(args);
			if (!destinationConfig.success()) {
				printUsageAndExit(jsap, destinationConfig);
			}
			File destinationDirectory = new File(
					destinationConfig.getString("destination"));

			File mountDestinationDirectory = null;
			try {
				mountDestinationDirectory = new File(
						destinationConfig.getString("mount"));
			} catch (NullPointerException e) {
				// do nothing
			}

			System.out
					.println("Getting statistics from cloud to local disk...");
			Master m = new Master();
			m.retrieveStats(destinationDirectory, mountDestinationDirectory,
					100);

			System.out.println("done.");
			System.exit(0);
		}

		printUsageAndExit(jsap, config);
	}

	private void deleteData(String bucketName) {
			
		String bucket_name = bucketName;

        System.out.println("Deleting S3 bucket: " + bucket_name);
        final AmazonS3 s3 = new AmazonS3Client(getAwsCredentials());
        

        try {
            System.out.println(" - removing objects from bucket");
            ObjectListing object_listing = s3.listObjects(bucket_name);
            while (true) {
                for (Iterator<?> iterator =
                        object_listing.getObjectSummaries().iterator();
                        iterator.hasNext();) {
                    S3ObjectSummary summary = (S3ObjectSummary)iterator.next();
                    s3.deleteObject(bucket_name, summary.getKey());
                }

                // more object_listing to retrieve?
                if (object_listing.isTruncated()) {
                    object_listing = s3.listNextBatchOfObjects(object_listing);
                } else {
                    break;
                }
            };

            System.out.println(" - removing versions from bucket");
            VersionListing version_listing = s3.listVersions(
                    new ListVersionsRequest().withBucketName(bucket_name));
            while (true) {
                for (Iterator<?> iterator =
                        version_listing.getVersionSummaries().iterator();
                        iterator.hasNext();) {
                    S3VersionSummary vs = (S3VersionSummary)iterator.next();
                    s3.deleteVersion(
          
                  bucket_name, vs.getKey(), vs.getVersionId());
                }

                if (version_listing.isTruncated()) {
                    version_listing = s3.listNextBatchOfVersions(
                            version_listing);
                } else {
                    break;
                }
            }

            System.out.println(" OK, bucket ready to delete!");
            s3.deleteBucket(bucket_name);
        } catch (AmazonServiceException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        System.out.println("Done!");
		
	}

	public static final Line parseLine(String line) {
		StringTokenizer t = new StringTokenizer(line, " ");
		// second to last element is extractor name
		// last element is "."

		Line l = new Line();
		while (t.hasMoreTokens()) {
			String entry = t.nextToken();
			if (entry.startsWith("<ex:")) {
				l.extractor = entry.replace("<ex:", "").replace(">", "");
				l.quad = line.replace(entry, "");
				return l;
			}
		}
		log.warn("Unable to parse " + line);
		return null;
	}

	private static void printUsageAndExit(JSAP jsap, JSAPResult result) {
		@SuppressWarnings("rawtypes")
		Iterator it = result.getErrorMessageIterator();
		while (it.hasNext()) {
			System.err.println("Error: " + it.next());
		}

		System.err.println("Usage: " + Master.class.getName() + " "
				+ jsap.getUsage());
		System.err.println(jsap.getHelp());
		System.err
				.println("General Usage: \n1) Create a CC extractor JAR file (mvn install)\n2) Use 'deploy' command to upload the JAR to S3\n3) Use 'queue' command to fill the extraction queue with CC file names\n4) Use 'start' command to launch EC2 extraction instances\n5) Wait until everything is finished using the 'monitor' command\n6) Use 'shutdown' command to kill worker nodes\n7) Collect result data and statistics with the 'retrievedata' and 'retrievestats' commands");

		System.exit(1);
	}

	public void clearData(boolean includeS3Storage) {
		ExecutorService ex = Executors.newFixedThreadPool(100);
		final AmazonSimpleDBClient client = getDbClient();
		String nextToken = null;
		long domainCount = 0;
		List<String> domains = new ArrayList<String>();
		do {
			ListDomainsResult res = client.listDomains(new ListDomainsRequest()
					.withNextToken(nextToken));
			nextToken = res.getNextToken();
			domains.addAll(res.getDomainNames());
			domainCount += domains.size();

		} while (nextToken != null);

		log.info(domainCount + " domains");

		for (final String domain : domains) {
			ex.submit(new Thread() {
				public void run() {
					client.deleteDomain(new DeleteDomainRequest(domain));
					log.info("Deleted " + domain);
				}
			});

		}

		ex.shutdown();
		try {
			ex.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (includeS3Storage) {
			log.info("Cleaning all data including s3 storage.");
			String resultBucket = getOrCry("resultBucket");
			try {
				for (S3Object object : getStorage().listObjects(resultBucket)) {
					if (object.getKey() != null
							&& (object.getKey().startsWith("data/") || object
									.getKey().startsWith("stats/"))) {
						log.info("Removing s3 object: " + object.getKey()
								+ " in bucket " + object.getBucketName());
						getStorage()
								.deleteObject(resultBucket, object.getKey());
					}
				}
			} catch (S3ServiceException e) {
				log.warn(e, e.fillInStackTrace());
			} catch (ServiceException e) {
				log.warn(e, e.fillInStackTrace());
			}
		}

		/*
		 * getDbClient().deleteDomain( new
		 * DeleteDomainRequest(getOrCry("sdbdatadomain")));
		 * 
		 * getDbClient().deleteDomain( new
		 * DeleteDomainRequest(getOrCry("sdberrordomain")));
		 * 
		 * String resultBucket = getOrCry("resultBucket");
		 * 
		 * try { for (S3Object object : getStorage().listObjects(resultBucket,
		 * "", null)) { getStorage().deleteObject(resultBucket,
		 * object.getKey()); } } catch (Exception e) {
		 * log.warn("Unable to clear data files", e); }
		 */
		log.info("Deleted statistics and intermediate data");
	}

	public void clearQueue() {
		deleteQueue();
		log.info("Deleted job queue");
	}

	public void crawlStats(String prefix, File statFile) {
		String dataBucket = getOrCry("dataBucket");
		String dataSuffix = "";
		boolean dataSuffixSet = true;
		try {
			dataSuffix = getOrCry("dataSuffix");
		} catch (IllegalArgumentException e) {
			dataSuffixSet = false;
		}
		Map<String, Long> fileCount = new HashMap<String, Long>();
		Map<String, Long> fileSize = new HashMap<String, Long>();

		try {

			for (S3Object object : getStorage().listObjects(dataBucket, prefix,
					null)) {
				if (dataSuffixSet && !object.getKey().endsWith(dataSuffix)) {
					continue;
				}
				List<Permission> permissions = object.getAcl()
						.getPermissionsForGrantee(new AllUsersGrantee());

				if (permissions == null
						|| !permissions.contains(Permission.PERMISSION_READ)) {
					log.warn("Unable to access " + object.getKey());
				}
				Long size = Long.parseLong((String) object
						.getMetadata(S3Object.METADATA_HEADER_CONTENT_LENGTH));
				StringTokenizer st = new StringTokenizer(object.getKey(), "/");
				String statKey = "";
				while (st.hasMoreTokens()) {
					statKey += st.nextToken() + "/";
					if (!st.hasMoreTokens()) {
						// we are at the last token, this is the filename, we no
						// want to see this in our statistics
						break;
					}
					Long oldCount = 0L;
					Long oldSize = 0L;

					if (fileCount.containsKey(statKey)) {
						oldCount = fileCount.get(statKey);
					}
					if (fileSize.containsKey(statKey)) {
						oldSize = fileSize.get(statKey);
					}

					fileCount.put(statKey, oldCount + 1);
					fileSize.put(statKey, oldSize + size);
				}
			}

		} catch (S3ServiceException e) {
			log.warn(e);
		}
		List<String> keys = new ArrayList<String>(fileCount.keySet());
		Collections.sort(keys);
		for (String key : keys) {

			Map<String, Object> statEntry = new HashMap<String, Object>();
			statEntry.put("bucket", dataBucket);
			statEntry.put("key", key);
			statEntry.put("files", fileCount.get(key));
			statEntry.put("size", fileSize.get(key));
			statEntry.put("sizep",
					CSVExport.humanReadableByteCount(fileSize.get(key), false));
			log.info(statEntry);
			CSVExport.writeToFile(statEntry, statFile);
		}
		CSVExport.closeWriter(statFile);

	}

	public void createInstances(int count, double priceLimitDollars) {
		AmazonEC2 ec2 = new AmazonEC2Client(getAwsCredentials());
		ec2.setEndpoint(getOrCry("ec2endpoint"));

		log.info("Requesting " + count + " instances of type "
				+ getOrCry("ec2instancetype") + " with price limit of "
				+ priceLimitDollars + " US$");
		log.debug(startupScript);

		try {
			// our bid
			RequestSpotInstancesRequest runInstancesRequest = new RequestSpotInstancesRequest()
					.withSpotPrice(Double.toString(priceLimitDollars))
					.withInstanceCount(count).withType("persistent");

			// increase volume size
			// BlockDeviceMapping mapping = new BlockDeviceMapping()
			// .withDeviceName("/dev/sda1").withEbs(
			// new EbsBlockDevice().withVolumeSize(Integer
			// .parseInt(getOrCry("ec2disksize"))));

			// what we want
			LaunchSpecification workerSpec = new LaunchSpecification()
					.withInstanceType(getOrCry("ec2instancetype"))
					.withImageId(getOrCry("ec2ami"))
					.withKeyName(getOrCry("ec2keypair"))
					// .withBlockDeviceMappings(mapping)
					.withUserData(
							new String(Base64.encodeBase64(startupScript
									.getBytes())));

			runInstancesRequest.setLaunchSpecification(workerSpec);

			// place the request
			ec2.requestSpotInstances(runInstancesRequest);
			log.info("Request placed, now use 'monitor' to check how many instances are running. Use 'shutdown' to cancel the request and terminate the corresponding instances.");
		} catch (Exception e) {
			log.warn("Failed to start instances - ", e);
		}
	}

	public void deploy(File jarFile) {
		String deployBucket = getOrCry("deployBucket");
		String deployFilename = getOrCry("deployFilename");

		try {
			getStorage().getOrCreateBucket(deployBucket);
			AccessControlList bucketAcl = getStorage().getBucketAcl(
					deployBucket);
			bucketAcl.grantPermission(GroupGrantee.ALL_USERS,
					Permission.PERMISSION_READ);

			S3Object statFileObject = new S3Object(jarFile);
			statFileObject.setKey(deployFilename);
			statFileObject.setAcl(bucketAcl);

			getStorage().putObject(deployBucket, statFileObject);

			log.info("File " + jarFile + " now accessible at " + getJarUrl());
		} catch (Exception e) {
			log.warn("Failed to deploy or set permissions in bucket  "
					+ deployBucket + ", key " + deployFilename, e);
		}
	}

	private void domainToCSV(String domainPrefix, File csvFile) {
		log.info("Storing data from SDB domains starting with " + domainPrefix
				+ " to file " + csvFile);

		int minResults = Integer.parseInt(getOrCry("minResults"));
		Set<String> attributes = null;

		List<String> domains = getDbClient().listDomains().getDomainNames();
		int c = 0;
		for (String domainName : domains) {
			if (domainName.startsWith(domainPrefix)) {
				c++;
				log.info("Exporting from " + domainName + " (" + c + "/"
						+ domains.size() + ")");
				long domainCount = getDbClient().domainMetadata(
						new DomainMetadataRequest(domainName)).getItemCount();
				if (domainCount < minResults) {
					log.info("Ignoring " + domainName + ", less than "
							+ minResults + " entries.");
					continue;
				}
				if (attributes == null) {
					attributes = getSdbAttributes(getDbClient(), domainName,
							minResults);
				}
				long total = 0;
				String select = "select * from `" + domainName + "` limit 2500";
				String nextToken = null;
				SelectResult res;
				do {
					res = getDbClient().select(
							new SelectRequest(select).withNextToken(nextToken)
									.withConsistentRead(false));

					for (Item i : res.getItems()) {
						Map<String, Object> csvEntry = new HashMap<String, Object>();
						csvEntry.put("_key", i.getName());
						for (String attr : attributes) {
							csvEntry.put(attr, "");
						}

						for (Attribute a : i.getAttributes()) {
							csvEntry.put(a.getName(), a.getValue());
						}
						CSVExport.writeToFile(csvEntry, csvFile);
					}
					nextToken = res.getNextToken();
					total += res.getItems().size();
					log.info("Exported " + total + " of " + domainCount);
				} while (nextToken != null);
				log.info("Finished exporting from " + domainName);

			}
		}
		CSVExport.closeWriter(csvFile);
	}

	private String getJarUrl() {
		return "http://s3.amazonaws.com/" + getOrCry("deployBucket") + "/"
				+ getOrCry("deployFilename");
	}

	private OutputStream getOutput(String extractor, File outputDir,
			int sizeLimitMb) throws FileNotFoundException, IOException {
		long sizeLimitBytes = sizeLimitMb * 1024 * 1024;
		if (outputFiles.containsKey(extractor)
				&& outputFiles.get(extractor).length() > sizeLimitBytes) {
			outputWriters.get(extractor).write("\n".getBytes());
			outputWriters.get(extractor).close();
			outputFiles.remove(extractor);
			outputWriters.remove(extractor);
		}

		if (!outputWriters.containsKey(extractor)) {
			int suffix = 0;
			File outputFile;
			do {
				outputFile = new File(outputDir + File.separator + "ccrdf."
						+ extractor + "." + suffix + ".nq.gz");
				suffix++;
			} while (outputFile.exists());

			outputFiles.put(extractor, outputFile);
			OutputStream os = new GZIPOutputStream(new FileOutputStream(
					outputFiles.get(extractor)));
			outputWriters.put(extractor, os);
		}
		return outputWriters.get(extractor);
	}

	public void monitorCPUUsage() {
		AmazonCloudWatchClient cloudClient = new AmazonCloudWatchClient(
				getAwsCredentials());
		GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();
		Calendar cal = Calendar.getInstance();
		request.setEndTime(cal.getTime());
		cal.add(Calendar.MINUTE, -5);
		request.setStartTime(cal.getTime());
		request.setNamespace("AWS/EC2");
		List<String> statistics = new ArrayList<String>();
		statistics.add("Maximium");
		statistics.add("Average");
		request.setStatistics(statistics);
		request.setMetricName("CPUUtilization");
		request.setPeriod(300);
		Dimension dimension = new Dimension();
		dimension.setName("InstanceId");
		dimension.setValue("i-d93fa2a4");
		List<Dimension> dimensions = new ArrayList<Dimension>();
		dimensions.add(dimension);
		request.setDimensions(dimensions);
		GetMetricStatisticsResult result = cloudClient
				.getMetricStatistics(request);
		List<Datapoint> dataPoints = result.getDatapoints();
		for (Datapoint dataPoint : dataPoints) {
			System.out.println(dataPoint.getAverage());
		}

	}

	public void monitorQueue(boolean autoShutDown) {
		System.out
				.println("Monitoring job queue, extraction rate and running instances. AutoShutdown is: "
						+ (autoShutDown ? "on" : "off"));
		System.out.println();

		List<DateSizeRecord> sizeLog = new ArrayList<DateSizeRecord>();
		DecimalFormat twoDForm = new DecimalFormat("#.##");

		AmazonEC2 ec2 = new AmazonEC2Client(getAwsCredentials());
		ec2.setEndpoint(getOrCry("ec2endpoint"));

		long emptyQueueTimerMS = 0;
		long maxEmptyQueueTimeMS = 60000;
		long sleepMS = 1000;

		while (true) {
			try {
				DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest();
				DescribeSpotInstanceRequestsResult describeResult = ec2
						.describeSpotInstanceRequests(describeRequest);
				List<SpotInstanceRequest> describeResponses = describeResult
						.getSpotInstanceRequests();

				int requestedInstances = 0;
				int runningInstances = 0;
				for (SpotInstanceRequest describeResponse : describeResponses) {
					if ("active".equals(describeResponse.getState())) {
						runningInstances++;
						requestedInstances++;
					}
					if ("open".equals(describeResponse.getState())) {
						requestedInstances++;
					}
				}

				// get queue attributes
				GetQueueAttributesResult res = getQueue().getQueueAttributes(
						new GetQueueAttributesRequest(getQueueUrl())
								.withAttributeNames("All"));
				Long queueSize = Long.parseLong(res.getAttributes().get(
						"ApproximateNumberOfMessages"));
				Long inflightSize = Long.parseLong(res.getAttributes().get(
						"ApproximateNumberOfMessagesNotVisible"));

				// in case the queueSize and the inflightSize are 0 for a longer
				// time, the ec2s are automatically shut down.
				if (autoShutDown) {
					if (requestedInstances + runningInstances > 0) {
						if (queueSize + inflightSize > 0) {
							emptyQueueTimerMS = 0;
						} else {
							emptyQueueTimerMS += sleepMS;
						}

						if (emptyQueueTimerMS > maxEmptyQueueTimeMS) {

							System.out
									.println(new Date() + " No more messages to process. Shutting down instances.");
							shutdownInstances();
							try {
								// lets wait a little bit.
								System.out.println("Waiting for shutdown.");
								Thread.sleep(sleepMS * 10);
							} catch (InterruptedException e) {
								// who cares if we get interrupted here
							}
						}
					}
				}

				// add the new value to the tail, now remove too old stuff from
				// the
				// head
				DateSizeRecord nowRecord = new DateSizeRecord(Calendar
						.getInstance().getTime(), queueSize + inflightSize);
				sizeLog.add(nowRecord);

				int windowSizeSec = 120;

				// remove outdated entries
				for (DateSizeRecord rec : new ArrayList<DateSizeRecord>(sizeLog)) {
					if (nowRecord.recordTime.getTime()
							- rec.recordTime.getTime() > windowSizeSec * 1000) {
						sizeLog.remove(rec);
					}
				}
				// now the first entry is the first data point, and the entry
				// just
				// added the last;
				DateSizeRecord compareRecord = sizeLog.get(0);
				double timeDiffSec = (nowRecord.recordTime.getTime() - compareRecord.recordTime
						.getTime()) / 1000;
				long sizeDiff = compareRecord.queueSize - nowRecord.queueSize;

				double rate = sizeDiff / timeDiffSec;

				System.out.print('\r');

				if (rate > 0) {
					System.out.print("Q: " + queueSize + " (" + inflightSize
							+ "), R: " + twoDForm.format(rate * 60)
							+ " m/min, ETA: "
							+ twoDForm.format((queueSize / rate) / 3600)
							+ " h, N: " + runningInstances + "/"
							+ requestedInstances + "          ");
				} else {
					System.out.print("Q: " + queueSize + " (" + inflightSize
							+ "), N: " + runningInstances + "/"
							+ requestedInstances
							+ "                          	");
				}

			} catch (AmazonServiceException e) {
				System.out.print("\r! // ");
			}
			try {
				Thread.sleep(sleepMS);
			} catch (InterruptedException e) {
				// who cares if we get interrupted here
			}
		}
	}

	public void queue(String singlePrefix, Long limit, String filePath) {

		String dataBucket = getOrCry("dataBucket");
		int batchSize = Integer.parseInt(getOrCry("batchsize"));

		Set<String> prefixes = new HashSet<String>();
		if (filePath == null) {
			if (singlePrefix == null || singlePrefix.trim().equals("")) {
				log.warn("No prefix given");
				return;
			}
			prefixes.add(singlePrefix);
			log.info("Queuing all keys from bucket " + dataBucket
					+ " with prefix " + singlePrefix);
		} else {

			try {
				FileReader fis = new FileReader(new File(filePath));
				BufferedReader br = new BufferedReader(fis);
				while (br.ready()) {
					String line = br.readLine();
					if (line != null && line.trim().length() > 0) {
						prefixes.add(line.trim());
					}
				}
				br.close();
			} catch (FileNotFoundException e) {
				log.warn("Could not find file.");
				log.debug(e);
			} catch (IOException e) {
				log.warn("Could not access file.");
				log.debug(e);
			}

			log.info("Queuing all keys from bucket " + dataBucket
					+ " with prefixes included in " + filePath);
		}
		if (prefixes.size() < 1) {
			log.warn("No prefixes included");
			return;
		}
		if (limit != null) {
			log.info("Setting limit of files to: " + limit);
		} else {
			log.info("Selecting all included files.");
		}
		boolean dataSuffixSet = true;
		String dataSuffix = "";
		try {
			dataSuffix = getOrCry("dataSuffix");
		} catch (IllegalArgumentException e) {
			dataSuffixSet = false;
		}

		long globalQueued = 0;
		for (String prefix : prefixes) {
			try {
				prefix = getOrCry("dataPrefix") + "/" + prefix;
				long objectsQueued = 0;
				SendMessageBatchRequest smbr = new SendMessageBatchRequest(
						getQueueUrl());
				smbr.setEntries(new ArrayList<SendMessageBatchRequestEntry>());

				for (S3Object object : getStorage().listObjects(dataBucket,
						prefix, null)) {
					// if limit is set and number of queued objects reached
					// limit,
					// stop queuing
					if (limit != null && globalQueued >= limit) {
						break;
					}
					if (dataSuffixSet && !object.getKey().endsWith(dataSuffix)) {
						continue;
					}
					SendMessageBatchRequestEntry smbre = new SendMessageBatchRequestEntry();
					smbre.setMessageBody(object.getKey());
					smbre.setId("task_" + objectsQueued);
					smbr.getEntries().add(smbre);
					if (smbr.getEntries().size() >= batchSize) {
						getQueue().sendMessageBatch(smbr);
						// having send into queue - reset entries.
						smbr.setEntries(new ArrayList<SendMessageBatchRequestEntry>());
					}
					objectsQueued++;
					globalQueued++;
				}
				// send the rest
				if (smbr.getEntries().size() > 0) {
					getQueue().sendMessageBatch(smbr);
				}
				log.info("Queued " + objectsQueued + " objects for prefix "
						+ prefix);
			} catch (Exception e) {
				log.warn("Failed to queue objects in bucket " + dataBucket
						+ " with prefix " + prefix, e);
			}
		}
		log.info("Queued " + globalQueued + " objects for all given prefixes.");
	}

	/**
	 * 
	 * @param dataDir
	 * @param sizeLimitMb
	 * @param runInMultiThreadMode
	 */
	public void retrieveData(File dataDir, String fileName, int sizeLimitMb,
			int threads) {

		File file = null;
		if (fileName != null && fileName.length() > 0) {
			file = new File(fileName);
			if (!file.isDirectory()) {
				System.out.println("Input file name is not a directory");
				System.exit(0);
			}
		}

		if (file != null) {
			DataCollector dc = new DataCollector(fileName,
					dataDir.getAbsolutePath(), threads);
			dc.process();
		} else {
			Thread t = null;
			t = new DataThreadHandler(dataDir, file, sizeLimitMb, threads);

			t.start();
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public void retrieveStats(File destinationDirectory, File mountedDirectory,
			int sizeLimitMb) {
		if (!destinationDirectory.exists()) {
			destinationDirectory.mkdirs();
		}

		File failureStatFile = new File(destinationDirectory + File.separator
				+ "failed.csv.gz");
		domainToCSV(getOrCry("sdberrordomain"), failureStatFile);

		File dataStatFile = new File(destinationDirectory + File.separator
				+ "data.csv.gz");
		domainToCSV(getOrCry("sdbdatadomain"), dataStatFile);
	}

	public void shutdownInstances() {
		AmazonEC2 ec2 = new AmazonEC2Client(getAwsCredentials());
		ec2.setEndpoint(getOrCry("ec2endpoint"));

		try {
			// cancel spot request, so no new instances will be launched
			DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest();
			DescribeSpotInstanceRequestsResult describeResult = ec2
					.describeSpotInstanceRequests(describeRequest);
			List<SpotInstanceRequest> describeResponses = describeResult
					.getSpotInstanceRequests();
			List<String> spotRequestIds = new ArrayList<String>();
			List<String> instanceIds = new ArrayList<String>();

			for (SpotInstanceRequest describeResponse : describeResponses) {
				spotRequestIds.add(describeResponse.getSpotInstanceRequestId());
				if ("active".equals(describeResponse.getState())) {
					instanceIds.add(describeResponse.getInstanceId());
				}
			}
			ec2.cancelSpotInstanceRequests(new CancelSpotInstanceRequestsRequest()
					.withSpotInstanceRequestIds(spotRequestIds));
			log.info("Cancelled spot request");

			if (instanceIds.size() > 0) {
				ec2.terminateInstances(new TerminateInstancesRequest(
						instanceIds));
				log.info("Shut down " + instanceIds.size() + " instances");
			}

		} catch (Exception e) {
			log.warn("Failed to shutdown instances - ", e);
		}
	}
}