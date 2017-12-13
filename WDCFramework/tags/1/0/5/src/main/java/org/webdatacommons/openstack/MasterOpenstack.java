package org.webdatacommons.openstack;

import static org.jclouds.scriptbuilder.domain.Statements.exec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.compute.predicates.NodePredicates;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.nova.v2_0.NovaApi;
import org.jclouds.openstack.nova.v2_0.domain.BlockDeviceMapping;
import org.jclouds.openstack.nova.v2_0.domain.ServerCreated;
import org.jclouds.openstack.nova.v2_0.features.ServerApi;
import org.jclouds.openstack.nova.v2_0.options.CreateServerOptions;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.jets3t.service.model.S3Object;
import org.webdatacommons.framework.processor.ProcessingNode;
import org.webdatacommons.structureddata.extractor.RDFExtractor;
import org.webdatacommons.structureddata.processor.WarcProcessor;
import org.webdatacommons.structureddata.util.DataCollector;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Closeables;
import com.google.inject.Module;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.Channel;

import org.jclouds.scriptbuilder.ScriptBuilder;
import org.jclouds.scriptbuilder.domain.OsFamily;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.UnflaggedOption;
import com.martiansoftware.jsap.UnspecifiedParameterException;
import com.rabbitmq.client.ConnectionFactory;

import de.dwslab.dwslib.util.io.InputUtil;


/**
 *Based on the initial implementation of the Master.class.
 *Basic functionalities of the framework implemented to work with OpenStack.
 *RabbitMQ is used as a queuing service and NOVA instance for computation.
 *Update the properties file with all the required configurations. 
 * @author Anna Primpeli
 */

public class MasterOpenstack extends ProcessingNode {


	com.rabbitmq.client.Connection connection;
	com.rabbitmq.client.Channel channel;
	ComputeService client;
	NovaApi novaApi;
	
	private class DataThread extends Observable implements Runnable {
		private S3Object object;
		private String name;
		private File file;
		private int i;
		private File dataDir;
		private int sizeLimitMb;
		private int length;
		private String resultBucket;


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
					System.out.println("No S3 implementation possible");
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
	 * NOTICE: This is a startup shell script for OpenStack instances. It installs
	 * Java 8 and launches the jar file that is stored already in the volume block of the instance.
	 * 
	 * This is designed to work on the Ubuntu 14.04 LTS AMI ami-018c9568
	 */
	String script = new ScriptBuilder()
            .addStatement(exec("sudo apt-get install -y python-software-properties debconf-utils"))
            .addStatement(exec("sudo add-apt-repository -y ppa:webupd8team/java"))
            .addStatement(exec("sudo apt-get update"))
            .addStatement(exec("echo \"oracle-java8-installer shared/accepted-oracle-license-v1-1 select true\" | sudo debconf-set-selections"))
            .addStatement(exec("sudo apt-get install -y oracle-java8-installer htop"))
            .addStatement(exec("java -Xmx1g -jar /home/ubuntu/"+getOrCry("jarName")+" > /tmp/start.log 2> /tmp/start_errors.log"))
            .render(OsFamily.UNIX);
	
	private static Logger log = Logger.getLogger(MasterOpenstack.class);


	// command line parameters, different actions
	public static void main(String[] args) throws Exception {
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
			
			FlaggedOption limit = new FlaggedOption("limit")
					.setStringParser(JSAP.LONG_PARSER).setRequired(false)
					.setLongFlag("file-number-limit").setShortFlag('l');

			limit.setHelp("Limits number of objects to be queued from bucket");

			jsap.registerParameter(limit);

			FlaggedOption prefixFile = new FlaggedOption("path")
					.setStringParser(JSAP.STRING_PARSER).setRequired(false)
					.setLongFlag("warc-file-path").setShortFlag('f');

			prefixFile
					.setHelp("File including line based paths of objects to be queued from bucket");
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
				filePath = queueResult.getString("path");
			} catch (UnspecifiedParameterException e) {
				// do nothing
			}

			new MasterOpenstack().queue(limitValue,
					filePath);
			
			System.exit(0);
		}

		if ("clearqueue".equals(action)) {
			new MasterOpenstack().clearQueue();
			System.exit(0);
		}

		if ("start".equals(action)) {
			FlaggedOption amountP = new FlaggedOption("amount")
					.setStringParser(JSAP.INTEGER_PARSER).setRequired(true)
					.setLongFlag("worker-amount").setShortFlag('a');
			amountP.setHelp("Amount of worker instances to start in OpenStack");
			jsap.registerParameter(amountP);

			FlaggedOption imageID = new FlaggedOption("image")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("imageID").setShortFlag('i');
			imageID.setHelp("Image ID for the worker you want to launch. Example for Ubuntu 14: 6c6d2344-e271-42f2-9119-83edbfd73f47");
			jsap.registerParameter(imageID);
			
			
			FlaggedOption flavorID = new FlaggedOption("flavor")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("flavorID").setShortFlag('f');
			flavorID.setHelp("Flavor ID for the worker you want to launch. Example for m1.large: 33324d1a-ed5e-472f-8342-8ba70151f513, for m1.medium 736c2146-d194-43b2-8eab-3d3316ecf8f9");
			jsap.registerParameter(flavorID);
			
			FlaggedOption volume = new FlaggedOption("volume")
					.setStringParser(JSAP.INTEGER_PARSER).setRequired(true)
					.setLongFlag("volumeSize").setShortFlag('v');
			volume.setHelp("How much volume in GB you want to attach to your worker. Fill in an integer.");
			jsap.registerParameter(volume);
			
			JSAPResult startParams = jsap.parse(args);
			if (!startParams.success()) {
				printUsageAndExit(jsap, startParams);
			}

			int amount = startParams.getInt("amount");
			new MasterOpenstack().createInstances(amount, startParams.getString("image"), startParams.getString("flavor"),
					startParams.getInt("volume"));
			System.out.println("done.");
			System.exit(0);
		}

		if ("shutdown".equals(action)) {
			System.out
					.print("To be implemented");
//			new MasterOpenstack().shutdownInstances();
//			System.out.println("done.");
			System.exit(0);
		}

		if ("retrievedata".equals(action)) {
			System.out
			.print("To be implemented");
		}

		if ("retrievestats".equals(action)) {

			System.out
			.print("To be implemented");
		}

		printUsageAndExit(jsap, config);
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

		System.err.println("Usage: " + MasterOpenstack.class.getName() + " "
				+ jsap.getUsage());
		System.err.println(jsap.getHelp());
		System.err
				.println("General Usage: \n1) Create a CC extractor JAR file (mvn install)\n2) Use 'deploy' command to upload the JAR to S3\n3) Use 'queue' command to fill the extraction queue with CC file names\n4) Use 'start' command to launch EC2 extraction instances\n5) Wait until everything is finished using the 'monitor' command\n6) Use 'shutdown' command to kill worker nodes\n7) Collect result data and statistics with the 'retrievedata' and 'retrievestats' commands");

		System.exit(1);
	}

	

	public void clearQueue() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(getOrCry("queueIP"));
		factory.setPort(Integer.parseInt(getOrCry("queuePort")));
		factory.setUsername(getOrCry("queueUsername"));
		factory.setPassword(getOrCry("queuePassword"));
		factory.setVirtualHost(getOrCry("queueVHost"));
		
		connection = factory.newConnection();
		channel = connection.createChannel();
		channel.queuePurge(getOrCry("queueName"));
		
		channel.close();
		connection.close();
		
		log.info("Deleted job queue");
	}

	

	public void createInstances(int count, String imageID, String flavorID, int volume) throws Exception {
		
		Set<String> workerIDs = new HashSet<String>();
        ServerCreated ser;

		for (int i=0; i<count;i++){

			//authenticate
			Iterable<Module> modules = ImmutableSet.<Module>of(new SLF4JLoggingModule());
	
	        String provider = getOrCry("provider");
	        String identity = getOrCry("identity"); // tenantName:userName
	        String credential = getOrCry("credential");
	
	        novaApi = ContextBuilder.newBuilder(provider)
	                .endpoint(getOrCry("endpoint"))
	                .credentials(identity, credential)
	                .modules(modules)
	                .buildApi(NovaApi.class);
	        
	        ServerApi serverApi = this.novaApi.getServerApiForZone(getOrCry("zone"));
	        
	        //build up volume      
	        BlockDeviceMapping blockDeviceMappings = BlockDeviceMapping.builder()
	                .uuid(imageID).sourceType("image").destinationType("volume")
	                .volumeSize(volume).bootIndex(0).build();
	        
	        
	        CreateServerOptions options = CreateServerOptions.Builder.
	        		availabilityZone("nova").
	        		keyPairName(getOrCry("keypair")).
	        		blockDeviceMappings(ImmutableSet.of(blockDeviceMappings));      		
	        
	        // create instances
        	ser = serverApi.create("worker", imageID,flavorID, options);
        	log.info("Create worker");
        	//wait 10 seconds for the instance to be initialized (can i do it another way?)
        	TimeUnit.SECONDS.sleep(10);

            System.out.println("Instance ID:"+ser.getId());
            String ipAddress = getIPofID(ser.getId());
            // store jar file
            storeJar(ipAddress);
            log.info("Store jar file");
            workerIDs.add(getOrCry("zone")+"/"+ser.getId());
            
            
        }
		System.out.println("Run the script on all worker instances");
		runScript(workerIDs);
		log.info("Script file");
        System.exit(1);
		// Closeables.close(novaApi, true);
	}

	private void runScript(Set<String> workerIDs) throws Exception {
		
		RunScriptOptions options = RunScriptOptions.Builder
	            .blockOnComplete(true)
	            .overrideLoginUser("ubuntu")
	            .overrideLoginPrivateKey(getKeyFromFile())
	            .wrapInInitScript(false)
	            .runAsRoot(false);

		client.runScriptOnNodesMatching(NodePredicates.withIds(Iterables.toArray(workerIDs, String.class)), script, options);
	}

	private String getKeyFromFile() throws Exception {
		 
		BufferedReader br = new BufferedReader(new FileReader(getOrCry("pemFilePath")));
		    try {
		        StringBuilder sb = new StringBuilder();
		        String line = br.readLine();

		        while (line != null) {
		            sb.append(line);
		            sb.append("\n");
		            line = br.readLine();
		        }
		        return sb.toString();
		    } finally {
		        br.close();
		    }
	}
	private void storeJar(String ipAddress) throws SftpException, FileNotFoundException, InterruptedException {
		
		String SFTPHOST = ipAddress;
        int    SFTPPORT = 22;
        String SFTPUSER = "ubuntu";
        // this file can be id_rsa or id_dsa based on which algorithm is used to create the key
        String privateKey = getOrCry("privateKeyPath"); 
        String FILETOTRANSFER = getOrCry("jarFilePath");
        String SFTPWORKINGDIR = "/home/ubuntu/"; 
        
        JSch jSch = new JSch();
        Session     session     = null;
        Channel     channel    = null;
        ChannelSftp channelSftp = null;
        
        try {
            jSch.addIdentity(privateKey);
            System.out.println("Private Key Added.");
            session = jSch.getSession(SFTPUSER,SFTPHOST,SFTPPORT);
            System.out.println("Session Created. Will connect with ip:"+ipAddress);
             
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
        	TimeUnit.SECONDS.sleep(10);
            session.connect();
            channel = session.openChannel("sftp");
            channel.connect();
            System.out.println("shell channel connected....");
            channelSftp = (ChannelSftp)channel;
            channelSftp.cd(SFTPWORKINGDIR);
            File f = new File(FILETOTRANSFER);
            channelSftp.put(new FileInputStream(f), f.getName());
            System.out.println("Jar file transfered");


        } catch (JSchException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            if(channelSftp!=null){
                channelSftp.disconnect();
                channelSftp.exit();
            }
            if(channel!=null) ((ChannelSftp) channel).disconnect();
             
            if(session!=null) session.disconnect();
        }
		
	}
	private String getIPofID(String id) throws InterruptedException {
		
		Iterable<Module> modules = ImmutableSet.<Module> of(new SshjSshClientModule());

		 String provider = getOrCry("provider");
		 String identity = getOrCry("identity"); // tenantName:userName
		 String credential = getOrCry("credential");
	
		ComputeServiceContext context = ContextBuilder.newBuilder(provider)
               .endpoint(getOrCry("endpoint"))
               .credentials(identity, credential)
               .modules(modules)
               .buildApi(ComputeServiceContext.class);
	       
		client = context.getComputeService();

		for (ComputeMetadata node : client.listNodes()) {
			if(node.getProviderId().equals(id)){
		  	  	NodeMetadata metadata = client.getNodeMetadata(node.getId());		  	  	
		  	  	for (String ip:metadata.getPublicAddresses())
		  	  		return ip;
			}
		}
		return null;
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

	

	

	public void queue(Long limit, String filePath) throws IOException, TimeoutException {
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(getOrCry("queueIP"));
		factory.setPort(Integer.parseInt(getOrCry("queuePort")));
		factory.setUsername(getOrCry("queueUsername"));
		factory.setPassword(getOrCry("queuePassword"));
		factory.setVirtualHost(getOrCry("queueVHost"));
		
		connection = factory.newConnection();
		channel = connection.createChannel();
		
		//it is declared differently if it needs to be called from several clients (our case) check here: https://www.rabbitmq.com/api-guide.html
		channel.queueDeclare(getOrCry("queueName"), false, false, false, null);
		
		//put messages in the queue
		BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
		
		String line = null;
		int counter=0;
		while ((line = br.readLine()) != null) {
			counter++;
			//send the messages to the queue
			channel.basicPublish("",getOrCry("queueName"),null,line.getBytes());
			
			if (counter >= limit) break;
		}
	 
		br.close();
		
		System.out.println("Queue filled successfully with "+channel.messageCount(getOrCry("queueName"))+" messages.");
		
		channel.close();
		connection.close();
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

	
}