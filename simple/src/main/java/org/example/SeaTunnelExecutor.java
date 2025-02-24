package org.example;

import com.hazelcast.client.config.ClientConfig;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.YamlSeaTunnelConfigBuilder;

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
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
            clientJobProxy = jobExecutionEnv.execute();
        } catch (Throwable e) {
            log.error("Job execution submission failed.", e);
            JobInstance jobInstance = jobInstanceDao.getJobInstance(jobInstanceId);
            jobInstance.setJobStatus(JobStatus.FAILED);
            jobInstance.setEndTime(new Date());
            String jobInstanceErrorMessage = JobUtils.getJobInstanceErrorMessage(e.getMessage());
            jobInstance.setErrorMessage(jobInstanceErrorMessage);
            jobInstanceDao.update(jobInstance);
            throw new RuntimeException(e.getMessage(), e);
        }
        JobInstance jobInstance = jobInstanceDao.getJobInstance(jobInstanceId);
        jobInstance.setJobEngineId(Long.toString(clientJobProxy.getJobId()));
        jobInstanceDao.update(jobInstance);
        // Use Spring thread pool to handle asynchronous user retrieval
        CompletableFuture.runAsync(
                () -> {
                    waitJobFinish(
                            clientJobProxy,
                            jobInstanceId,
                            Long.toString(clientJobProxy.getJobId()),
                            seaTunnelClient);
                },
                taskExecutor);
    }

    private SeaTunnelClient createSeaTunnelClient() {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        return new SeaTunnelClient(clientConfig);
    }
}
