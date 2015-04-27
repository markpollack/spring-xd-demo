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

import static com.acme.temperature.maximum.TuplePropertyMatcher.withField;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.collection.IsMapContaining.hasEntry;

import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.TestScheduler;
import rx.subjects.TestSubject;

import org.springframework.xd.tuple.Tuple;
import org.springframework.xd.tuple.TupleBuilder;

/**
 * @author Marius Bogoevici
 */
public class TopSensorsTest {

	@Test
	public void testTopSensors() throws Exception {

		TestScheduler testScheduler = new TestScheduler();

		TopSensors topSensors = new TopSensors(1000, 3);
		topSensors.setScheduler(testScheduler);

		TestSubject<Tuple> inputStream = TestSubject.create(testScheduler);

		Observable<Tuple> outputStream = topSensors.process(inputStream);

		TestSubscriber<Tuple> testSubscriber = new TestSubscriber<>();
		outputStream.subscribe(testSubscriber);


		inputStream.onNext(TupleBuilder.tuple().put("sensorId", 1).put("averageTemperature", 19.5).build());
		inputStream.onNext(TupleBuilder.tuple().put("sensorId", 2).put("averageTemperature", 19.6).build());
		inputStream.onNext(TupleBuilder.tuple().put("sensorId", 3).put("averageTemperature", 19.8).build());
		inputStream.onNext(TupleBuilder.tuple().put("sensorId", 4).put("averageTemperature", 19.9).build());


		inputStream.onNext(TupleBuilder.tuple().put("sensorId", 1).put("averageTemperature", 29.5).build(), 1500);
		inputStream.onNext(TupleBuilder.tuple().put("sensorId", 2).put("averageTemperature", 28.6).build(), 1500);
		inputStream.onNext(TupleBuilder.tuple().put("sensorId", 3).put("averageTemperature", 22.8).build(), 1500);
		inputStream.onNext(TupleBuilder.tuple().put("sensorId", 4).put("averageTemperature", 21.9).build(), 1500);


		testScheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);

		testSubscriber.assertNoErrors();

		Assert.assertThat(testSubscriber.getOnNextEvents(), hasSize(1));

		LinkedHashMap<String, Object> firstResult = new LinkedHashMap<>();
		firstResult.put("4", 19.9);
		firstResult.put("3", 19.8);
		firstResult.put("2", 19.6);
		Assert.assertThat(testSubscriber.getOnNextEvents(), hasItem(withField("hottest", firstResult)));

		LinkedHashMap<String, Object> secondResult = new LinkedHashMap<>();
		secondResult.put("1", 29.5);
		secondResult.put("2", 28.6);
		secondResult.put("3", 22.8);

		testScheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);

		Assert.assertThat(testSubscriber.getOnNextEvents(), hasSize(2));
		Assert.assertThat(testSubscriber.getOnNextEvents(), hasItem(withField("hottest", firstResult)));
		Assert.assertThat(testSubscriber.getOnNextEvents(), hasItem(withField("hottest", secondResult)));

	}

}
