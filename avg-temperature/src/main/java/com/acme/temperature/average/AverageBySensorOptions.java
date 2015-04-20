/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.acme.temperature.average;

import org.springframework.xd.module.options.spi.ModuleOption;

/**
 * @author Marius Bogoevici
 */
public class AverageBySensorOptions {

	private int timeWindowLength = 5000;

	private int timeWindowShift = 1000;

	public int getTimeWindowLength() {
		return timeWindowLength;
	}

	@ModuleOption("the time interval for calculating averages")
	public void setTimeWindowLength(int timeWindowLength) {
		this.timeWindowLength = timeWindowLength;
	}

	public int getTimeWindowShift() {
		return timeWindowShift;
	}

	@ModuleOption("the frequency with which averages are emitted")
	public void setTimeWindowShift(int timeWindowShift) {
		this.timeWindowShift = timeWindowShift;
	}
}
