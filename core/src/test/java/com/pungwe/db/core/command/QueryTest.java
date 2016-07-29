/*
 * Copyright (C) 2016 Ian Michell.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pungwe.db.core.command;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by ian on 27/07/2016.
 */
public class QueryTest {

    @Test
    public void testSingleEqualTo() throws Exception {
        Map<String, String> test = new LinkedHashMap<>();
        test.put("field", "value");

        assertTrue(Query.select().where("field").isEqualTo("value").query().test(test));
    }

    @Test
    public void testFieldEqualTo() throws Exception {
        assertTrue(Query.select().where("field").isEqualTo("value").query().test("field", "value"));
        assertTrue(Query.select().where("field").isEqualTo("value").and("and").isEqualTo("other").query()
                .test("and", "other"));
        assertTrue(Query.select().where("field").isEqualTo("value").or("and").isEqualTo("other").query()
                .test("and", "other"));
        assertFalse(Query.select().where("field").isEqualTo("value").query().test("field", "invalid"));
        assertFalse(Query.select().where("field").isEqualTo("value").query().test("invalid", "value"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDoubleAndSameField() throws Exception {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("field", "value");
        Query.select().where("field").isEqualTo("value").and("anotherField").isEqualTo("value")
                .and("field").isEqualTo("another value").query().test(values);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOrDoubleAndSameField() throws Exception {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("field", "value");
        Query.select().where("field").isEqualTo("value").or("field").isEqualTo("value").and("anotherField")
                .isEqualTo("value").and("field").isEqualTo("another value").query().test(values);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldDoubleAndSameField() throws Exception {
        Query.select().where("field").isEqualTo("value").and("anotherField").isEqualTo("value")
                .and("field").isEqualTo("another value").query().test("field", "value");
    }

    @Test
    public void testNestedEqualTo() throws Exception {
        Map<String, Object> test = new LinkedHashMap<>();
        Map<String, String> nested = new LinkedHashMap<>();
        nested.put("field", "value");
        test.put("nested", nested);

        assertTrue(Query.select().where("nested.field").isEqualTo("value")
                .query().test(test));
    }

    @Test
    public void testAndEqualTo() throws Exception {
        Map<String, Object> test = new LinkedHashMap<>();
        test.put("field", "value");
        Map<String, String> nested = new LinkedHashMap<>();
        nested.put("field", "value");
        test.put("nested", nested);

        assertTrue(Query.select().where("nested.field").isEqualTo("value").and("field").isEqualTo("value")
                .query().test(test));
    }

    @Test
    public void testOrEqualTo() throws Exception {
        Map<String, Object> test = new LinkedHashMap<>();
        test.put("field", "value");
        Map<String, String> nested = new LinkedHashMap<>();
        nested.put("field", "value");
        test.put("nested", nested);

        assertFalse(Query.select().where("nested.field").isEqualTo("invalid").and("field").isEqualTo("value")
                .query().test(test));
        assertTrue(Query.select().where("nested.field").isEqualTo("invalid").or("field").isEqualTo("value")
                .query().test(test));
    }

    @Test
    public void testNotEqualTo() throws Exception {
        Map<String, String> value = new LinkedHashMap<>();
        value.put("field", "value");
        assertFalse(Query.select().where("field").isNotEqualTo("value").query().test(value));
        assertTrue(Query.select().where("field").isNotEqualTo("not").query().test(value));
        assertFalse(Query.select().where("field").isNotEqualTo("value").query().test("field", "value"));
        assertTrue(Query.select().where("field").isNotEqualTo("value").query().test("field", "not"));
    }

    @Test
    public void testMatch() throws Exception {
        Map<String, String> value = new LinkedHashMap<>();
        value.put("field", "value");
        assertFalse(Query.select().where("field").match("\\d+").query().test(value));
        assertTrue(Query.select().where("field").match("\\w+").query().test(value));
        assertFalse(Query.select().where("field").match("\\d+").query().test("field", "value"));
        assertTrue(Query.select().where("field").match("\\w+").query().test("field", "not"));
    }

    @Test
    public void testDoesNotMatch() throws Exception {
        Map<String, String> value = new LinkedHashMap<>();
        value.put("field", "value");
        assertTrue(Query.select().where("field").doesNotMatch("\\d+").query().test(value));
        assertFalse(Query.select().where("field").doesNotMatch("\\w+").query().test(value));
        assertTrue(Query.select().where("field").doesNotMatch("\\d+").query().test("field", "value"));
        assertFalse(Query.select().where("field").doesNotMatch("\\w+").query().test("field", "not"));
    }

    @Test
    public void testGreaterThan() throws Exception {
        Map<String, Integer> value = new LinkedHashMap<>();
        value.put("field", 1);
        assertFalse(Query.select().where("field").isGreaterThan(1, Integer::compareTo).query().test(value));
        assertTrue(Query.select().where("field").isGreaterThan(5, Integer::compareTo).query().test(value));
        assertFalse(Query.select().where("field").isGreaterThan(2, Integer::compareTo).query().test("field", 3));
        assertTrue(Query.select().where("field").isGreaterThan(2, Integer::compareTo).query().test("field", 1));
    }

    @Test
    public void testLessThan() throws Exception {
        Map<String, Integer> value = new LinkedHashMap<>();
        value.put("field", 2);
        assertFalse(Query.select().where("field").isLessThan(2, Integer::compareTo).query().test(value));
        assertTrue(Query.select().where("field").isLessThan(0, Integer::compareTo).query().test(value));
        assertFalse(Query.select().where("field").isLessThan(2, Integer::compareTo).query().test("field", 1));
        assertTrue(Query.select().where("field").isLessThan(2, Integer::compareTo).query().test("field", 3));
    }
}
