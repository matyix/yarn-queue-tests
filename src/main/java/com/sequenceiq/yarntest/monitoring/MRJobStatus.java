package com.sequenceiq.yarntest.monitoring;

import java.io.IOException;

import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TaskReport;

public class MRJobStatus {

	public JobStatus printJobStatus(YARNRunner yarnRunner, JobID jobID) throws IOException, InterruptedException {
		JobStatus jobStatus;
		jobStatus = yarnRunner.getJobStatus(jobID);
		
		// print overall job M/R progresses
		System.out.println(jobStatus.getJobName() + " (" + jobStatus.getQueue() + ")" + " progress M/R: " + jobStatus.getMapProgress() + "/" + jobStatus.getReduceProgress());
		System.out.println(jobStatus.getTrackingUrl());
		System.out.println(jobStatus.getReservedMem() + " "+ jobStatus.getUsedMem() + " "+ jobStatus.getNumUsedSlots());
		
		// list map & reduce tasks statuses and progress		
		TaskReport[] reports = yarnRunner.getTaskReports(jobID, TaskType.MAP);
		for (int i = 0; i < reports.length; i++) {
			System.out.println("MAP: " + reports[i].getCurrentStatus() + " " + reports[i].getTaskID() + " " + reports[i].getProgress()); 
		}
		reports = yarnRunner.getTaskReports(jobID, TaskType.REDUCE);
		for (int i = 0; i < reports.length; i++) {
			System.out.println("REDUCE: " + reports[i].getCurrentStatus() + " " + reports[i].getTaskID() + " " + reports[i].getProgress()); 
		}
		return jobStatus;
	}	
}



