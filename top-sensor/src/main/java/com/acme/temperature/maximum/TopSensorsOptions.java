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

package com.acme.temperature.maximum;

import org.springframework.xd.module.options.spi.ModuleOption;

/**
 * @author Marius Bogoevici
 */
public class TopSensorsOptions {

	private int topValues = 10;

	private int timeWindowLength = 2000;

	public int getTopValues() {
		return topValues;
	}

	@ModuleOption("the maximum numbers of sensors to return")
	public void setTopValues(int topValues) {
		this.topValues = topValues;
	}

	public int getTimeWindowLength() {
		return timeWindowLength;
	}

	@ModuleOption("the frequency with which the top sensors are calculated")
	public void setTimeWindowLength(int timeWindowLength) {
		this.timeWindowLength = timeWindowLength;
	}
}
