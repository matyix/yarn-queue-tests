package com.sequenceiq.yarntest.queue;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;

import com.sequenceiq.yarntest.mr.QuasiMonteCarlo;

public class QueueOrchestrator {

	public JobID submitJobsIntoQueues(String queueName, Path tempDir) throws Exception {
		
		Configuration priorityConf = this.getConfiguration(queueName);

		// list queue with YarnClient, not so useful
		YarnClient yarnClient = new YarnClientImpl();
        yarnClient.init(priorityConf);
        yarnClient.start();  
        
        List<QueueInfo> queues = yarnClient.getAllQueues();
        for (QueueInfo queueInfo : queues) {
			System.out.println("Queue Informations (name, capacity, current capacity): " + queueInfo.getQueueName() + " "+ queueInfo.getCapacity() + " " + queueInfo.getCurrentCapacity());
		} 
		
        FileSystem fs = FileSystem.get(priorityConf);
         
	    try {    
	    	// submit MR App to highPriority queue
		    JobID jobID = QuasiMonteCarlo.submitPiEstimationMRApp("PiEstimation into: " + queueName, 10, 3, tempDir, priorityConf);
		    
		    return jobID;
		    
	    } finally {
	    	fs.deleteOnExit(tempDir);
	    	yarnClient.close();
	    	
	    }
	}
	
	public Configuration getConfiguration(String queueName) {
		Configuration priorityConf = new Configuration();
		priorityConf.set("mapreduce.job.queuename", queueName);
		
		/**
		 * Additional job configuration parameters can be submitted for the job
		conf.set(MRJobConfig.MAP_CPU_VCORES, "2");
		conf.set(MRJobConfig.MAP_MEMORY_MB, "2048");
		conf.set(MRJobConfig.REDUCE_CPU_VCORES, "1");
		conf.set(MRJobConfig.REDUCE_MEMORY_MB, "1024");	
		 */	
		
		return priorityConf;
	}
}
