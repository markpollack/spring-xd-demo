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

import static org.springframework.xd.tuple.TupleBuilder.tuple;
import static rx.observables.MathObservable.averageDouble;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import org.springframework.xd.rxjava.Processor;
import org.springframework.xd.tuple.Tuple;

/**
 * @author Marius Bogoevici
 */
public class AverageBySensor implements Processor<Tuple,Tuple> {

	private int timeWindowLength;

	private int timeWindowShift;

	private Scheduler scheduler = Schedulers.computation();

	public AverageBySensor(int timeWindowLength, int timeWindowShift) {
		this.timeWindowLength = timeWindowLength;
		this.timeWindowShift = timeWindowShift;
	}

	public void setScheduler(Scheduler scheduler) {
		this.scheduler = scheduler;
	}

	@Override
	public Observable<Tuple> process(Observable<Tuple> inputStream) {

		return inputStream
				.window(timeWindowLength, timeWindowShift, TimeUnit.MILLISECONDS, scheduler)
				.flatMap(w ->
						w.groupBy(data -> data.getInt("sensorId"))
								.flatMap(dataById ->
												averageDouble(dataById.map(t -> t.getDouble("temperature")))
														.map(d -> tuple().of("sensorId", dataById.getKey(), "averageTemperature", d))
								)
				);
	}

}
