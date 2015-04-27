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

import org.hamcrest.CustomMatcher;

import org.springframework.xd.tuple.Tuple;

/**
 * @author Marius Bogoevici
 */
class TuplePropertyMatcher extends CustomMatcher<Tuple> {

	private String propertyName;

	private Object value;

	public TuplePropertyMatcher(String propertyName, Object value) {
		super("tuple with property");
		this.propertyName = propertyName;
		this.value = value;
	}

	public static TuplePropertyMatcher withField(String propertyName, Object value) {
		return new TuplePropertyMatcher(propertyName, value);
	}

	@Override
	public boolean matches(Object item) {
		return value.equals(((Tuple) item).getValue(propertyName));
	}
}
