package com.sequenceiq.yarntest.mr;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Random;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

public class ExampleRunner {
	
	private Path createTempDir() {
		long now = System.currentTimeMillis();
	    int rand = new Random().nextInt(Integer.MAX_VALUE);
	    Path tmpDir = new Path(QuasiMonteCarlo.TMP_DIR_PREFIX + "_" + now + "_" + rand);
		return tmpDir;
	}

	private JobStatus printJobStatus(YARNRunner yarnRunner, JobID jobID)
			throws IOException, InterruptedException {
		JobStatus jobStatus;
		jobStatus = yarnRunner.getJobStatus(jobID);
		// print overall job M/R progresses
		System.out.println(jobStatus.getJobName() + " (" + jobStatus.getQueue() + ")" + " progress M/R: " + jobStatus.getMapProgress() + "/" + jobStatus.getReduceProgress());
	//	System.out.println(jobStatus.getTrackingUrl());
	//	System.out.println(jobStatus.getReservedMem() + " "+ jobStatus.getUsedMem() + " "+ jobStatus.getNumUsedSlots());
		
//			// list map & reduce tasks statuses and progress		
//			TaskReport[] reports = yarnRunner.getTaskReports(jobID, TaskType.MAP);
//			for (int i = 0; i < reports.length; i++) {
//				System.out.println("MAP: " + reports[i].getCurrentStatus() + " " + reports[i].getTaskID() + " " + reports[i].getProgress()); 
//			}
//			reports = yarnRunner.getTaskReports(jobID, TaskType.REDUCE);
//			for (int i = 0; i < reports.length; i++) {
//				System.out.println("REDUCE: " + reports[i].getCurrentStatus() + " " + reports[i].getTaskID() + " " + reports[i].getProgress()); 
//			}
		return jobStatus;
	}	
	
	private void printQueueInfo(HttpClient client, ObjectMapper mapper) {
		
		GetMethod get = new GetMethod("http://sandbox.hortonworks.com:8088/ws/v1/cluster/scheduler");
	    get.setRequestHeader("Accept", "application/json");
	    try {
	        int statusCode = client.executeMethod(get);

	        if (statusCode != HttpStatus.SC_OK) {
	          System.err.println("Method failed: " + get.getStatusLine());
	        }
	        
			InputStream in = get.getResponseBodyAsStream();
			
			JsonNode jsonNode = mapper.readValue(in, JsonNode.class);
			ArrayNode queues = (ArrayNode) jsonNode.path("scheduler").path("schedulerInfo").path("queues").get("queue");
			for (int i = 0; i < queues.size(); i++) {
				JsonNode queueNode = queues.get(i);						
				System.out.println("queueName / usedCapacity / absoluteUsedCap / absoluteCapacity / absMaxCapacity: " + 
						queueNode.findValue("queueName") + " / " +
						queueNode.findValue("usedCapacity") + " / " + 
						queueNode.findValue("absoluteUsedCapacity") + " / " + 
						queueNode.findValue("absoluteCapacity") + " / " +
						queueNode.findValue("absoluteMaxCapacity"));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {	        
			get.releaseConnection();
		}	      
	        
	}
	
	private void run() throws Exception {
		HttpClient client = new HttpClient();
		ObjectMapper mapper = new ObjectMapper();
		
		Configuration highPrioConf = new Configuration();
		highPrioConf.set("mapreduce.job.queuename", "highPriority");	
//		conf.set(MRJobConfig.MAP_CPU_VCORES, "2");
//		conf.set(MRJobConfig.MAP_MEMORY_MB, "2048");
//		conf.set(MRJobConfig.REDUCE_CPU_VCORES, "1");
//		conf.set(MRJobConfig.REDUCE_MEMORY_MB, "1024");
		
		Configuration lowPrioConf = new Configuration();		
		

        // create YarnRunner to use for job status listing
        YARNRunner yarnRunner = new YARNRunner(highPrioConf);	
        
		// list queue with YarnClient, not so useful
		YarnClient yarnClient = new YarnClientImpl();
        yarnClient.init(highPrioConf);
        yarnClient.start();        
        List<QueueInfo> queues = yarnClient.getAllQueues();
        for (QueueInfo queueInfo : queues) {
			System.out.println(queueInfo.getQueueName() + " "+ queueInfo.getCapacity() + " " + queueInfo.getCurrentCapacity());
		} 
		
        FileSystem fs = FileSystem.get(highPrioConf);
        Path tmpDir1 = createTempDir();        
        Path tmpDir2 = createTempDir();    
	    try {    
	    	// submit MR App to highPriority queue
		    JobID highPrioJobID = QuasiMonteCarlo.submitPiEstimationMRApp("PiEstimationHIGHPrio", 10, 3, tmpDir1, highPrioConf);
		    
	    	// submit MR App to highPriority queue
		    JobID lowPrioJobID = QuasiMonteCarlo.submitPiEstimationMRApp("PiEstimationLOWPrio", 10, 3, tmpDir2, lowPrioConf);		    
		    
		    // list job statuses & queue infos until job is completed
		    JobStatus highPrioJobStatus = printJobStatus(yarnRunner, highPrioJobID);
		    JobStatus lowPrioJobStatus = printJobStatus(yarnRunner, lowPrioJobID);
		    
			for(;!lowPrioJobStatus.isJobComplete();) {
				highPrioJobStatus = printJobStatus(yarnRunner, highPrioJobID);								
			    lowPrioJobStatus = printJobStatus(yarnRunner, lowPrioJobID);				
				
			    printQueueInfo(client, mapper);
				Thread.sleep(1000);
			}
	    } finally {
	    	fs.deleteOnExit(tmpDir1);
	    	fs.deleteOnExit(tmpDir2);
	    }
		
		
	}

	public static void main(String[] args) throws Exception {
		new ExampleRunner().run();
	}
}
