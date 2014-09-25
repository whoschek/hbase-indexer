package com.ngdata.hbaseindexer.master;

import com.ngdata.hbaseindexer.model.api.BatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class BatchStateUpdater implements Runnable {
    private WriteableIndexerModel indexerModel;
    private JobClient jobClient;
    private ScheduledExecutorService executor;
    private int pollInterval;
    private Log log = LogFactory.getLog(BatchStateUpdater.class);

    protected BatchStateUpdater(WriteableIndexerModel indexerModel, JobClient jobClient,
                      ScheduledExecutorService executor, int pollInterval) {
        this.indexerModel = indexerModel;
        this.jobClient = jobClient;
        this.executor = executor;
        this.pollInterval = pollInterval;
    }

    @Override
    public void run() {
        for (IndexerDefinition indexerDefinition : indexerModel.getIndexers()) {
            log.debug("Checking batch state for " + indexerDefinition.getName());
            BatchBuildInfo batchBuildInfo = indexerDefinition.getActiveBatchBuildInfo();
            if (batchBuildInfo != null) {
                Set<String> jobs = batchBuildInfo.getMapReduceJobTrackingUrls().keySet();

                boolean batchDone = true;
                boolean overAllSuccess = true;
                for (String jobId : jobs) {
                    RunningJob job = null;
                    try {
                        job = jobClient.getJob(JobID.forName(jobId));
                    } catch (IOException e) {
                        log.error("Could not get job " + jobId + " for index " + indexerDefinition.getName() +
                                " while checking active build info.",e);
                        batchDone = false;
                        break;
                    }
                    if (job != null) {
                        int jobState = 0;
                        try {
                            jobState = job.getJobState();
                        } catch (IOException e) {
                            log.error("Could not get jobstate for job " + jobId + " for index " +
                                    indexerDefinition.getName() + " while checking active build info.",e);
                            batchDone = false;
                            break;
                        }
                        batchDone = batchDone && jobState != JobStatus.RUNNING;
                        overAllSuccess = overAllSuccess && jobState == JobStatus.SUCCEEDED;
                    } else {
                        log.warn("Could not find job " + jobId + " while checking active batch builds for indexer " +
                            indexerDefinition.getName());
                    }
                }
                if (batchDone) {
                    markBatchBuildCompleted(indexerDefinition.getName(), overAllSuccess);
                } else {
                    executor.schedule(this, pollInterval, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private void markBatchBuildCompleted(String indexerName, boolean success) {
        try {
            // Lock internal bypasses the index-in-delete-state check, which does not matter (and might cause
            // failure) in our case.
            String lock = indexerModel.lockIndexerInternal(indexerName, false);
            try {
                // Read current situation of record and assure it is still actual
                IndexerDefinition indexer = indexerModel.getFreshIndexer(indexerName);

                BatchBuildInfo activeJobInfo = indexer.getActiveBatchBuildInfo();

                if (activeJobInfo == null) {
                    // This might happen if we got some older update event on the indexer right after we
                    // marked this job as finished.
                    log.warn("Unexpected situation: indexer batch build completed but indexer does not have an active" +
                            " build job. Index: " + indexer.getName() + ". Ignoring this event.");
                    return;
                }

                BatchBuildInfo batchBuildInfo = new BatchBuildInfo(activeJobInfo);
                batchBuildInfo = batchBuildInfo.finishedSuccessfully(success);

                indexer = new IndexerDefinitionBuilder()
                        .startFrom(indexer)
                        .lastBatchBuildInfo(batchBuildInfo)
                        .activeBatchBuildInfo(null)
                        .batchIndexingState(IndexerDefinition.BatchIndexingState.INACTIVE)
                        .build();

                indexerModel.updateIndexerInternal(indexer);

                log.info("Marked indexer batch build as finished for indexer " + indexerName);
            } finally {
                indexerModel.unlockIndexer(lock, true);
            }
        } catch (Throwable t) {
            log.error("Error trying to mark index batch build as finished for indexer " + indexerName, t);
        }
    }
}
