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
import static rx.Observable.just;
import static rx.Observable.zip;
import static rx.observables.MathObservable.averageDouble;

import java.util.concurrent.TimeUnit;

import rx.Observable;

import org.springframework.xd.rxjava.Processor;
import org.springframework.xd.tuple.Tuple;
import org.springframework.xd.tuple.TupleBuilder;

/**
 * @author Marius Bogoevici
 */
public class AverageBySensor implements Processor<Tuple,Tuple>{

	private int timeWindowLength;

	private int timeWindowShift;

	public AverageBySensor(int timeWindowLength, int timeWindowShift) {
		this.timeWindowLength = timeWindowLength;
		this.timeWindowShift = timeWindowShift;
	}

	@Override
	public Observable<Tuple> process(Observable<Tuple> inputStream) {

		return inputStream
				.window(timeWindowLength, timeWindowShift, TimeUnit.MILLISECONDS)
				.flatMap(w -> w.groupBy(data -> data.getInt("sensorId"))
								.flatMap(dataById ->
										zip(just(dataById.getKey()),
												 averageDouble(dataById.map(d -> d.getDouble("temperature"))).single(),
												(k, v) -> TupleBuilder.tuple().of("sensorId", k, "averageTemperature", v))));
	}

}
