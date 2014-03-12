package com.sequenceiq.yarntest.client;

import java.util.Random;

import org.apache.commons.httpclient.HttpClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.codehaus.jackson.map.ObjectMapper;

import com.sequenceiq.yarntest.monitoring.MRJobStatus;
import com.sequenceiq.yarntest.monitoring.QueueInformation;
import com.sequenceiq.yarntest.mr.QuasiMonteCarlo;
import com.sequenceiq.yarntest.queue.QueueOrchestrator;

public class JobClient {

	public static void main(String[] args) {
		try {
			JobClient jobClient = new JobClient();
			QueueOrchestrator qo = new QueueOrchestrator();
			HttpClient client = new HttpClient();
			ObjectMapper mapper = new ObjectMapper();
			String schedulerURL = "http://sandbox.hortonworks.com:8088/ws/v1/cluster/scheduler";
			
			MRJobStatus mrJobStatus = new MRJobStatus();
			QueueInformation queueInformation = new QueueInformation();
			
			//Create low priority setup - low priority root queue (capacity-scheduler.xml)
			Path tempDirLow = jobClient.createTempDir();
			//Create high priority setup - high priority root queue (capacity-scheduler.xml)
			Path tempDirHigh = jobClient.createTempDir();
			
			String lowPriorityQueue = new String("lowPriority");
			String highPriorityQueue = new String("highPriority");
			
			JobID lowPriorityJobID = qo.submitJobsIntoQueues(lowPriorityQueue, tempDirLow);
			JobID highPriorityJobID = qo.submitJobsIntoQueues(highPriorityQueue, tempDirHigh);
			
			Configuration lowPriorityConf = qo.getConfiguration(lowPriorityQueue);
			// create YarnRunner to use for job status listing
	        YARNRunner yarnRunnerLow = new YARNRunner(lowPriorityConf);
			// list low priority job status
			JobStatus lowPriorityJobStatus = mrJobStatus.printJobStatus(yarnRunnerLow, lowPriorityJobID);
			
			Configuration highPriorityConf = qo.getConfiguration(highPriorityQueue);
			// create YarnRunner to use for job status listing
			YARNRunner yarnRunnerHigh = new YARNRunner(highPriorityConf);
			// list high priority job status
			JobStatus highPriorityJobStatus = mrJobStatus.printJobStatus(yarnRunnerHigh, highPriorityJobID);
					
			// list job statuses & queue information until job(s) are completed
			for(;!lowPriorityJobStatus.isJobComplete();) {
				highPriorityJobStatus = mrJobStatus.printJobStatus(yarnRunnerLow, highPriorityJobID);								
				lowPriorityJobStatus = mrJobStatus.printJobStatus(yarnRunnerHigh, lowPriorityJobID);				
				
				queueInformation.printQueueInfo(client, mapper, schedulerURL);
				Thread.sleep(1000);
			}
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	private Path createTempDir() {
		long now = System.currentTimeMillis();
	    int rand = new Random().nextInt(Integer.MAX_VALUE);
	    Path tmpDir = new Path(QuasiMonteCarlo.TMP_DIR_PREFIX + "_" + now + "_" + rand);
		return tmpDir;
	}
}