package com.seatunnel.example;

import com.hazelcast.client.config.ClientConfig;
import com.seatunnel.example.entity.JobInstance;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.YamlSeaTunnelConfigBuilder;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/2/24 22:53
 */
public class SeaTunnelExecutor {

    private void executeJobBySeaTunnel(String filePath, Long jobInstanceId) {
        Common.setDeployMode(DeployMode.CLIENT);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobInstanceId + "_job");
        SeaTunnelClient seaTunnelClient;
        ClientJobProxy clientJobProxy;
        try {
            seaTunnelClient = createSeaTunnelClient();
            SeaTunnelConfig seaTunnelConfig = new YamlSeaTunnelConfigBuilder().build();
            ClientJobExecutionEnvironment jobExecutionEnv = seaTunnelClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
            clientJobProxy = jobExecutionEnv.execute();
        } catch (Throwable e) {
            //log.error("Job execution submission failed.", e);
            JobInstance jobInstance = null; // jobInstanceDao.getJobInstance(jobInstanceId);
            jobInstance.setJobStatus(JobStatus.FAILED);
            jobInstance.setEndTime(new Date());
            String jobInstanceErrorMessage = null; //JobUtils.getJobInstanceErrorMessage(e.getMessage());
            jobInstance.setErrorMessage(jobInstanceErrorMessage);
            //jobInstanceDao.update(jobInstance);
            throw new RuntimeException(e.getMessage(), e);
        }
        JobInstance jobInstance = null; //jobInstanceDao.getJobInstance(jobInstanceId);
        jobInstance.setJobEngineId(Long.toString(clientJobProxy.getJobId()));
        //jobInstanceDao.update(jobInstance);

//        CompletableFuture.runAsync(
//                () -> {
//                    waitJobFinish(clientJobProxy, jobInstanceId, Long.toString(clientJobProxy.getJobId()), seaTunnelClient);
//                },
//                taskExecutor);
    }

    private SeaTunnelClient createSeaTunnelClient() {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        return new SeaTunnelClient(clientConfig);
    }

    public void waitJobFinish(ClientJobProxy clientJobProxy, Long jobInstanceId, String jobEngineId, SeaTunnelClient seaTunnelClient) {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        CompletableFuture<JobResult> future = CompletableFuture.supplyAsync(
                clientJobProxy::waitForJobCompleteV2, executor);
        JobResult jobResult = new JobResult(JobStatus.FAILED, "");
        try {
            jobResult = future.get();
            executor.shutdown();
        } catch (InterruptedException e) {
            jobResult.setError(e.getMessage());
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            jobResult.setError(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            seaTunnelClient.close();
            //log.info("and jobInstanceService.complete begin");
            //jobInstanceService.complete(jobInstanceId, jobEngineId, jobResult);
        }
    }
}
