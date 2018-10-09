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

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Holds configuration properties for Quartz Scheduler.
 *
 * @author Vedran Pavic
 * @author Stephane Nicoll
 * @author Manokethan Parameswaran
 */
@Validated
@ConfigurationProperties(prefix = QuartzSchedulerProperties.QUARTZ_SCHEDULER_PROPERTIES)
public class QuartzSchedulerProperties {

	/**
	 * Namespace to use for Quartz Scheduler configuration properties.
	 */
	public static final String QUARTZ_SCHEDULER_PROPERTIES = "spring.cloud.scheduler.quartz";

	/**
	 * Whether to automatically start the scheduler after initialization.
	 */
	private boolean autoStartup = true;

	/**
	 * Delay in seconds after which the scheduler is started once initialization completes. Setting
	 * this property makes sense if no jobs should be run before the entire application
	 * has started up.
	 */
	private int startupDelay = 0;

	/**
	 * Whether to wait for running jobs to complete on shutdown.
	 */
	private boolean waitForJobsToCompleteOnShutdown = false;

	/**
	 * Whether configured jobs should overwrite existing job definitions.
	 */
	private boolean overwriteExistingJobs = false;

	/**
	 * Additional Quartz Scheduler properties.
	 */
	private final Map<String, String> properties = new HashMap<>();

	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public int getStartupDelay() {
		return this.startupDelay;
	}

	public void setStartupDelay(int startupDelay) {
		this.startupDelay = startupDelay;
	}

	public boolean isWaitForJobsToCompleteOnShutdown() {
		return this.waitForJobsToCompleteOnShutdown;
	}

	public void setWaitForJobsToCompleteOnShutdown(
			boolean waitForJobsToCompleteOnShutdown) {
		this.waitForJobsToCompleteOnShutdown = waitForJobsToCompleteOnShutdown;
	}

	public boolean isOverwriteExistingJobs() {
		return this.overwriteExistingJobs;
	}

	public void setOverwriteExistingJobs(boolean overwriteExistingJobs) {
		this.overwriteExistingJobs = overwriteExistingJobs;
	}

	public Map<String, String> getProperties() {
		return this.properties;
	}

}
