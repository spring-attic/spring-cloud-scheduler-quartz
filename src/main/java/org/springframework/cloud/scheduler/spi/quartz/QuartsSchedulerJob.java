/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.scheduler.spi.quartz;

import org.quartz.JobExecutionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class QuartsSchedulerJob extends QuartzJobBean {

    private static final Logger logger = LoggerFactory.getLogger(QuartsSchedulerJob.class);

    private TaskLauncher taskLauncher;

    private AppDeploymentRequest taskRequest;

    public void setTaskLauncher(TaskLauncher taskLauncher) { this.taskLauncher = taskLauncher; }

    public void setTaskRequest(AppDeploymentRequest taskRequest) { this.taskRequest = taskRequest; }

    @Override
    protected void executeInternal(JobExecutionContext jobExecutionContext) {
        logger.debug("launching scheduled quartz job {}", taskRequest.getDefinition().getName());

        String sd = taskLauncher.launch(taskRequest);
        logger.debug("App launched ! The status is {}" + taskLauncher.status(sd));
    }
}
