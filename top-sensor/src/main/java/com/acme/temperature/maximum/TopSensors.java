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

import static org.springframework.xd.tuple.TupleBuilder.tuple;
import static rx.Observable.just;
import static rx.Observable.zip;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import rx.Observable;

import org.springframework.xd.rxjava.Processor;
import org.springframework.xd.tuple.Tuple;

/**
 * @author Marius Bogoevici
 */
public class TopSensors implements Processor<Tuple, Tuple> {

	private int timeWindowLength;

	private int maxTopSensors;

	public TopSensors(int timeWindowLength, int maxTopSensors) {
		this.timeWindowLength = timeWindowLength;
		this.maxTopSensors = maxTopSensors;
	}

	@Override
	public Observable<Tuple> process(final Observable<Tuple> inputStream) {
		return inputStream
				.window(timeWindowLength, TimeUnit.MILLISECONDS)
				.flatMap(w ->
						w.groupBy(t -> t.getInt("sensorId"))
								.flatMap(g -> zip(just(g.getKey()), g.last().map(t->t.getDouble("averageTemperature")), (k, v) -> tuple().of("sensorId", k, "averageTemperature", v)))
						.toSortedList((t1, t2) -> Double.compare(t2.getDouble("averageTemperature"), t1.getDouble("averageTemperature"))).map(l -> l.subList(0, Math.min(maxTopSensors, l.size()))).map(l -> tuple().of("hottest", asMap(l))));

	}

	private static Map<String, Double> asMap(List<Tuple> tuples) {
		Map<String, Double> returnValue = new LinkedHashMap<>();
		for (Tuple tuple : tuples) {
			returnValue.put(Integer.toString(tuple.getInt("sensorId")), tuple.getDouble("averageTemperature"));
		}
		return returnValue;
	}
}
