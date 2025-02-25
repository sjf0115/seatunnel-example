package com.seatunnel.example;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.Config;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.YamlSeaTunnelConfigBuilder;
import org.apache.seatunnel.engine.core.job.JobResult;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 功能：SeaTunnelExample
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/2/23 19:46
 */
public class SeaTunnelExample {

    public static ClientConfig createClientConfig() {
        ClientConfig hazelcastConfig = new ClientConfig();

        // 1. 设置集群名称（需与 SeaTunnel 集群一致）
        hazelcastConfig.setClusterName("seatunnel");

        // 2. 添加集群成员地址
        hazelcastConfig.getNetworkConfig()
                .addAddress("localhost:5801");

        // 3. 配置网络参数（可选）
        ClientNetworkConfig networkConfig = hazelcastConfig.getNetworkConfig();
        networkConfig.setConnectionTimeout(5000);  // 连接超时时间（毫秒）
        hazelcastConfig.setNetworkConfig(networkConfig);

        return hazelcastConfig;
    }

    public static Config createHazelcastConfig() {
        Config config = new Config();
        config.setClusterName("seatunnel");

        // 2. 添加集群成员地址
        config.getNetworkConfig()
                .setPortAutoIncrement(false)
                .setPort(5801)
                .setPublicAddress("localhost");
        return config;
    }

    public static String writeJobConfigIntoConfFile(String jobConfig, Long jobDefineId) {
        String projectRoot = System.getProperty("user.dir");
        String filePath =
                projectRoot + File.separator + "profile" + File.separator + jobDefineId + ".conf";
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.getParentFile().mkdirs();
            }

            FileWriter fileWriter = new FileWriter(file);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            bufferedWriter.write(jobConfig);
            bufferedWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return filePath;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Hazelcast 客户端配置
//        Config hazelcastConfig = createHazelcastConfig();
//        SeaTunnelConfig seaTunnelConfig = new SeaTunnelConfig();
//        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);

        // 初始化引擎客户端
        //ClientConfig clientConfig = createClientConfig();
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);

        // 执行环境
        String filePath = writeJobConfigIntoConfFile("env {\n" +
                "  # You can set SeaTunnel environment configuration here\n" +
                "  parallelism = 2\n" +
                "  job.mode = \"BATCH\"\n" +
                "  checkpoint.interval = 10000\n" +
                "}\n" +
                "\n" +
                "source {\n" +
                "  # This is a example source plugin **only for test and demonstrate the feature source plugin**\n" +
                "  FakeSource {\n" +
                "    parallelism = 2\n" +
                "    result_table_name = \"fake\"\n" +
                "    row.num = 16\n" +
                "    schema = {\n" +
                "      fields {\n" +
                "        name = \"string\"\n" +
                "        age = \"int\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "\n" +
                "  # If you would like to get more information about how to configure SeaTunnel and see full list of source plugins,\n" +
                "  # please go to https://seatunnel.apache.org/docs/connector-v2/source\n" +
                "}\n" +
                "\n" +
                "sink {\n" +
                "  Console {\n" +
                "  }\n" +
                "\n" +
                "  # If you would like to get more information about how to configure SeaTunnel and see full list of sink plugins,\n" +
                "  # please go to https://seatunnel.apache.org/docs/connector-v2/sink\n" +
                "}", 1111L);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("FakeToConsole");
        SeaTunnelConfig seaTunnelConfig = new YamlSeaTunnelConfigBuilder().build();
        ClientJobExecutionEnvironment environment = engineClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);

        // 提交任务
        ClientJobProxy clientJobProxy = environment.execute();

        // 作业状态轮询
        while (!clientJobProxy.getJobStatus().isEndState()) {
            Thread.sleep(5000);
            System.out.println("Current status: " + clientJobProxy.getJobStatus());
        }

        // 等待任务完成
        CompletableFuture<JobResult> completableFuture = CompletableFuture.supplyAsync(clientJobProxy::waitForJobCompleteV2);
        completableFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                System.err.println("Job failed: " + ex.getMessage());
            } else {
                System.out.println("Job completed with status: " + result.getStatus());
            }
        });
    }
}
