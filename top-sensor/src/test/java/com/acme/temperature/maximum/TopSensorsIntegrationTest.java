package com.acme.temperature.maximum;

import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.*;
import static org.springframework.xd.dirt.test.process.SingleNodeProcessingChainSupport.*;

import org.springframework.xd.dirt.server.singlenode.SingleNodeApplication;
import org.springframework.xd.dirt.test.SingleNodeIntegrationTestSupport;
import org.springframework.xd.dirt.test.SingletonModuleRegistry;
import org.springframework.xd.dirt.test.process.SingleNodeProcessingChain;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.test.RandomConfigurationSupport;
import org.springframework.xd.tuple.Tuple;
import org.springframework.xd.tuple.TupleBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TopSensorsIntegrationTest {

	private static SingleNodeApplication application;

	private static int RECEIVE_TIMEOUT = 5000;

	private static String moduleName = "highest-temperature";

	private SingleNodeProcessingChain chain;

	/**
	 * Start the single node container, binding random unused ports, etc. to not conflict with any other instances
	 * running on this host. Configure the ModuleRegistry to include the project module.
	 */
	@BeforeClass
	public static void setUp() {

		RandomConfigurationSupport randomConfigSupport = new RandomConfigurationSupport();
		application = new SingleNodeApplication().run();
		SingleNodeIntegrationTestSupport singleNodeIntegrationTestSupport = new SingleNodeIntegrationTestSupport
				(application);
		singleNodeIntegrationTestSupport.addModuleRegistry(new SingletonModuleRegistry(ModuleType.processor,
				moduleName));

	}

	@After
	public void tearDown() {
		chain.destroy();
	}

	@AfterClass
	public static void after() {
		application.close();
	}

	@Test
	public void testTupleType() {

		String streamName = "testHighestTemperature";

		chain = chain(application, streamName, "highest-temperature --topValues=3");

		List<Tuple> inputData = new ArrayList<Tuple>();

		inputData.add(TupleBuilder.tuple().of("sensorId", 1, "averageTemperature", 19.5));
		inputData.add(TupleBuilder.tuple().of("sensorId", 2, "averageTemperature", 19.6));
		inputData.add(TupleBuilder.tuple().of("sensorId", 3, "averageTemperature", 19.8));
		inputData.add(TupleBuilder.tuple().of("sensorId", 4, "averageTemperature", 19.9));

		for (Tuple tuple: inputData) {
			chain.sendPayload(tuple);
		}

		assertResults(chain);
	}

	@Test
	public void testJson() throws IOException {

		String streamName = "testHighestTemperatureJson";

		chain = chain(application, streamName,
				"highest-temperature --topValues=3 --inputType=application/x-xd-tuple");
		List<String> jsonData = new ArrayList<String>();

		jsonData.add("{\"sensorId\":\"" + 1 + "\" , \"averageTemperature\" : \"" + 19.5 + "\"}");
		jsonData.add("{\"sensorId\":\"" + 2 + "\" , \"averageTemperature\" : \"" + 19.6 + "\"}");
		jsonData.add("{\"sensorId\":\"" + 3 + "\" , \"averageTemperature\" : \"" + 19.8 + "\"}");
		jsonData.add("{\"sensorId\":\"" + 4 + "\" , \"averageTemperature\" : \"" + 19.9 + "\"}");

		for (String json : jsonData) {
			chain.sendPayload(json);
		}
		assertResults(chain);
	}

	private void assertResults(SingleNodeProcessingChain chain) {
		List<Tuple> outputData = new ArrayList<Tuple>();
		for (int i = 0; i < 2; i++) {
			Tuple tuple = (Tuple)chain.receivePayload(RECEIVE_TIMEOUT);
			outputData.add(tuple);
			System.out.println("output Tuple = [ + " + tuple + "]");
		}
		assertEquals(19.9, (Double)outputData.get(0).getValue("hottest", Map.class).get("4"), 0.01);
		assertEquals(19.8, (Double)outputData.get(0).getValue("hottest", Map.class).get("3"), 0.01);
		assertEquals(19.6, (Double) outputData.get(0).getValue("hottest", Map.class).get("2"), 0.01);
		assertThat((Collection<?>)outputData.get(1).getValue("hottest", Map.class).entrySet(), hasSize(0));
	}
}
