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

import static com.acme.temperature.average.TuplePropertyMatcher.withProperty;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.springframework.xd.tuple.TupleBuilder.tuple;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.TestScheduler;
import rx.subjects.TestSubject;

import org.springframework.xd.tuple.Tuple;

/**
 * @author Marius Bogoevici
 */
public class AverageBySensorTests {

	@Test
	public void testAverageCalculation() throws Exception {

		TestScheduler testScheduler = new TestScheduler();

		AverageBySensor averageBySensor = new AverageBySensor(1000, 3);
		averageBySensor.setScheduler(testScheduler);

		TestSubject<Tuple> inputStream = TestSubject.create(testScheduler);

		Observable<Tuple> outputStream = averageBySensor.process(inputStream);

		TestSubscriber<Tuple> testSubscriber = new TestSubscriber<>();
		outputStream.subscribe(testSubscriber);

		inputStream.onNext(tuple().put("sensorId", 0).put("temperature", 10.0).build());
		inputStream.onNext(tuple().put("sensorId", 1).put("temperature", 20.0).build());

		inputStream.onNext(tuple().put("sensorId", 0).put("temperature", 11.0).build(), 500);
		inputStream.onNext(tuple().put("sensorId", 1).put("temperature", 21.0).build(), 500);

		Assert.assertThat(testSubscriber.getOnNextEvents(), hasSize(0));

		testScheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);

		testSubscriber.assertNoErrors();
		Assert.assertThat(testSubscriber.getOnNextEvents(), hasSize(2));
		Assert.assertThat(testSubscriber.getOnNextEvents(),
				hasItem(allOf(withProperty("sensorId", 0),withProperty("averageTemperature",10.5))));
		Assert.assertThat(testSubscriber.getOnNextEvents(),
				hasItem(allOf(withProperty("sensorId", 1),withProperty("averageTemperature",20.5))));

	}

}
