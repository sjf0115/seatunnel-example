package com.seatunnel.example.entity;

import lombok.Data;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.starter.enums.EngineType;
import org.apache.seatunnel.engine.core.job.JobStatus;

import java.util.Date;

/**
 * 功能：JobInstance
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/2/25 22:47
 */

@Data
public class JobInstance {
//    @TableId(value = "id", type = IdType.INPUT)
    private Long id;

//    @TableField("job_define_id")
    private Long jobDefineId;

//    @TableField("job_status")
    private JobStatus jobStatus;

//    @TableField("job_config")
    private String jobConfig;

//    @TableField("engine_name")
    private EngineType engineName;

//    @TableField("engine_version")
    private String engineVersion;

//    @TableField("job_engine_id")
    private String jobEngineId;

//    @TableField("create_user_id")
    private Integer createUserId;

//    @TableField("update_user_id")
    private Integer updateUserId;

//    @TableField("create_time")
    private Date createTime;

//    @TableField("update_time")
    private Date updateTime;

//    @TableField("end_time")
    private Date endTime;

//    @TableField("job_type")
    private JobMode jobType;

//    @TableField("error_message")
    private String errorMessage;
}
