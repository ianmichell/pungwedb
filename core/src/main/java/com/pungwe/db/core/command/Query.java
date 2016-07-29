package com.pungwe.db.core.command;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * <p>
 * This class holds the relevant field information and predicates for a query during execution. It can test the values
 * of a single field or an entire object.
 * <p>
 * If the database engine has an index for some of the fields contained in the query predicate, then they
 * should be tested individually to eliminate objects that are not needed for a full scan. This is relatively
 * inexpensive, depending on how much index coverage of the records being queried there is... The query planner
 * might use only one index and scan the rest (as index lookups can result in a lot of IO if they are large).
 * <p>
 * If the database engine has an index that covers all the fields contained in the query predicate, then a full test
 * should be executed as it would be far more efficient.
 * <p>
 * If the database engine does not have an index that covers any of the fields contained in the index, then a full
 * test is executed, which depending on the size of the individual records and number scanned could be very
 * expensive.
 */
public class Query {

    private final Set<QueryField> fields;
    private final QueryPredicate predicate;

    public static Query select() {
        return new Query(new ArrayList<>());
    }

    public static Query select(String... fields) {
        return select(Arrays.stream(fields).map(QueryField::new).collect(Collectors.toList()));
    }

    public static Query select(QueryField... fields) {
        return select(Arrays.asList(fields));
    }

    public static Query select(Collection<QueryField> fields) {
        return new Query(fields);
    }

    /**
     * Create a new inner query predicate. This is used for nested conditions...
     *
     * @param field the field being tested
     *
     * @return a field predicate.
     */
    public static FieldPredicate builder(String field) {
        return new QueryPredicate().field(field);
    }

    // FIXME: Add some cool modifiers here...
    private Query(Collection<QueryField> fields) {
        Set<QueryField> selectedFields = new LinkedHashSet<>();
        selectedFields.addAll(fields);
        this.fields = Collections.unmodifiableSet(selectedFields);
        this.predicate = new QueryPredicate(this);
    }

    public Set<QueryField> selectedFields() {
        return fields;
    }

    public Set<String> testedFields() {
        // We want this to be ordered at least...
        Set<String> fields = new LinkedHashSet<>();
        QueryPredicate current = predicate;
        while (current != null) {
            fields.add(current.get().getField());
            current = current.next();
        }
        return fields;
    }


    /**
     * Tests a single value. Useful if an index doesn't cover an entire query...
     *
     * @param field the field being tested
     * @param value the value of the field being tested
     * @return true if it passes, false if not.
     */
    public boolean test(String field, Object value) {
        validateField(predicate, field);
        return test(predicate, field, value);
    }

    @SuppressWarnings("unchecked")
    private boolean test(QueryPredicate p, String field, Object value) {
        FieldPredicate fieldCondition = p.get();
        // this will be false if the field doesn't exist or the predicate is for a collection...
        boolean pass = fieldCondition.getField().equals(field) && !fieldCondition.isCollection();
        Predicate<Object> condition = (Predicate<Object>)fieldCondition.getCondition();
        pass = pass && condition.test(value);
        if (!pass && p.getAnd() != null) {
            return test(p.getAnd(), field, value);
        }
        if (!pass && p.getOr() != null) {
            return test(p.getOr(), field, value);
        }
        return pass;
    }

    /**
     * Tests a single value. Useful if an index doesn't cover the fields of an entire query...
     *
     * @param field  the field being tested
     * @param values the values being tested
     * @return true if it passes, false if not.
     */
    public boolean test(String field, Collection<?> values) {
        validateField(predicate, field);
        return test(predicate, field, values);
    }

    @SuppressWarnings("unchecked")
    public boolean test(QueryPredicate p, String field, Collection<?> values) {
        FieldPredicate fieldCondition = p.get();
        // this will be false if the field doesn't exist or the predicate is for a collection...
        boolean pass = fieldCondition.getField().equals(field) && fieldCondition.isCollection();
        Predicate<Collection<?>> condition = (Predicate<Collection<?>>)fieldCondition.getCondition();
        pass = pass && condition.test(values);
        if (!pass && p.getAnd() != null) {
            return test(p.getAnd(), field, values);
        }
        if (!pass && p.getOr() != null) {
            return test(p.getOr(), field, values);
        }
        return pass;
    }

    private void validateField(QueryPredicate p, String field) {
        AtomicInteger count = new AtomicInteger();
        QueryPredicate current = p;
        while (current != null) {
            if (current.get().getField().equals(field)) {
                count.incrementAndGet();
            }
            // Test!
            if (count.get() > 1) {
                break;
            }
            // If and is null, then set counter to 0
            if (current.getAnd() == null) {
                count.set(0);
                current = current.getOr();
            } else {
                current = current.getAnd();
            }
        }
        if (count.get() > 1) {
            throw new IllegalArgumentException("Invalid Query");
        }
    }
    /**
     * Tests the value of an entire object. Used during a wide scan... Potentially very inefficient is objects being
     * tested have fallen out of an index and everything is being scanned.
     *
     * @param value the map of values. Key must is classed as the field name and the value is tested.
     * @return true if it passes, false if not.
     */
    public boolean test(Map<String, ?> value) {
        for (String field : testedFields()) {
            validateField(predicate, field);
        }
        return testQueryPredicate(predicate, value);
    }

    @SuppressWarnings("unchecked")
    private boolean testQueryPredicate(QueryPredicate p, Map<String, ?> value) {
        // Get the field first predicate and test it...
        FieldPredicate first = p.get();
        String testField = first.getField();
        Predicate<Object> condition = (Predicate<Object>)first.getCondition();
        Optional<?> testValue = getValue(testField, value);
        // Check the test value type against the predicate type... If it's a collection, then test or if it's there...
        if (first.isCollection() && (!testValue.isPresent() || !Collection.class.isAssignableFrom(testValue.get()
                .getClass()))) {
            return p.getOr() != null && testQueryPredicate(p.getOr(), value);
        }
        // Check the condition against the value, check the and predicate if it passes, or check the or.
        boolean pass = condition.test(testValue.orElse(null));
        if (pass && p.getAnd() != null) {
            return testQueryPredicate(p.getAnd(), value);
        } else if (!pass && p.getOr() != null) {
            return testQueryPredicate(p.getOr(), value);
        }
        return pass;
    }

    private Optional<?> getValue(String field, Map<String, ?> value) {
        if (value.containsKey(field)) {
            return Optional.of(value.get(field));
        }
        // Split via dot notation...
        String[] fieldSplit = field.split("\\.");
        Object found = getValue(fieldSplit, value);
        if (found == null) {
            return Optional.empty();
        }
        return Optional.of(found);
    }

    private Object getValue(String[] fields, Map<?, ?> value) {
        Object found = value.get(fields[0]);
        // If it's null, we don't want it...
        if (found == null) {
            return null;
        }
        if (Collection.class.isAssignableFrom(found.getClass()) && fields.length > 1) {
            // We only want the values if they are a map.
            Collection<Map<?, ?>> values = ((Collection<?>)found).stream().filter(o -> o != null &&
                    Map.class.isAssignableFrom(o.getClass())).map(o -> (Map<?, ?>)o).collect(Collectors.toList());
            if (values.isEmpty()) {
                return null;
            }
            return getCollectionOfValues(Arrays.copyOfRange(fields, 1, fields.length), values);
        }
        // Check if it's a map
        if (Map.class.isAssignableFrom(found.getClass()) && fields.length > 1) {
            return getValue(Arrays.copyOfRange(fields, 1, fields.length), (Map<?, ?>)found);
        }
        return found;
    }

    /**
     * Walk through the collection of values.
     * @param fields the fields that are fetched from inside the list of values
     * @param values the values to be searched.
     * @return a collection of the values found...
     */
    private Collection<?> getCollectionOfValues(String[] fields, Collection<Map<?, ?>> values) {
        return values.stream().filter(map -> map != null && map.containsKey(fields[0]))
                .map(m -> getValue(fields, m)).collect(Collectors.toList());
    }

    public FieldPredicate where(String field) {
        return predicate.field(field);
    }

    public static class QueryPredicate {
        private Query query;
        private FieldPredicate predicate;
        private QueryPredicate and, or;

        private QueryPredicate(Query query) {
            this.query = query;
        }

        private QueryPredicate() {

        }

        private QueryPredicate next() {
            return and != null ? and : or;
        }

        private FieldPredicate get() {
            return predicate;
        }

        private QueryPredicate getAnd() {
            return and;
        }

        private QueryPredicate getOr() {
            return or;
        }

        public FieldPredicate field(String field) {
            predicate = new FieldPredicate(this, field);
            return predicate;
        }

        public FieldPredicate and(String field) {
            and = new QueryPredicate(query);
            return and.field(field);
        }

        public QueryPredicate and(QueryPredicate predicate) {
            and = predicate;
            return this;
        }

        public FieldPredicate or(String field) {
            or = new QueryPredicate(query);
            return or.field(field);
        }

        public QueryPredicate or(QueryPredicate predicate) {
            or = predicate;
            return this;
        }

        public Query query() {
            if (query == null) {
                return new Query(new ArrayList<>());
            }
            return query;
        }

    }

    public static class QueryField {
        private final String field;
        private final String as;

        public QueryField(String field) {
            this(field, null);
        }

        public QueryField(String field, String as) {
            this.field = field;
            this.as = as;
        }

        public String getField() {
            return field;
        }

        public String getAs() {
            return as;
        }
    }

    // FIXME: Add shapes
    public static class FieldPredicate {

        private boolean collection;
        private final QueryPredicate query;
        private final String field;
        private Predicate<?> condition;

        private FieldPredicate(QueryPredicate query, String field) {
            this.query = query;
            this.field = field;
        }

        public String getField() {
            return field;
        }

        public Predicate<?> getCondition() {
            return condition;
        }

        public boolean isCollection() {
            return collection;
        }

        public QueryPredicate match(String regex) {
            return match(Pattern.compile(regex));
        }

        public QueryPredicate match(Pattern p) {
            condition = p.asPredicate();
            return query;
        }

        public QueryPredicate doesNotMatch(String regex) {
            return doesNotMatch(Pattern.compile(regex));
        }

        public QueryPredicate doesNotMatch(Pattern p) {
            condition = o -> o == null || !p.matcher(o.toString()).matches();
            return query;
        }

        // ==
        public <T> QueryPredicate isEqualTo(T value) {
            condition = o -> o != null && o.equals(value);
            return query;
        }

        // !=
        public <T> QueryPredicate isNotEqualTo(T value) {
            condition = o -> (o == null && value != null) || !o.equals(value);
            return query;
        }

        // <
        @SuppressWarnings("unchecked")
        public <T> QueryPredicate isLessThan(T value, Comparator<T> comparator) {
            condition = (T o) -> comparator.compare(o, value) > 0;
            return query;
        }

        // >
        public <T> QueryPredicate isGreaterThan(T value, Comparator<T> comparator) {
            condition = (T o) -> comparator.compare(o, value) < 0;
            return query;
        }

        public <T> QueryPredicate contains(T value) {
            collection = true;
            condition = (Collection<T> o) -> o != null && o.contains(value);
            return query;
        }

        public <T> QueryPredicate doesNotContain(T value) {
            collection = true;
            condition = (Collection<T> o) -> o == null || !o.contains(value);
            return query;
        }

        public QueryPredicate containsMatch(String regex) {
            return containsMatch(Pattern.compile(regex));
        }

        public QueryPredicate containsMatch(Pattern p) {
            collection = true;
            condition = (Collection<?> o) -> o.parallelStream().filter(t -> String.class.isAssignableFrom(t.getClass()))
                    .map(t -> (String)t).anyMatch(p.asPredicate());
            return query;
        }

        public QueryPredicate doesNotContainMatch(String regex) {
            return doesNotContainMatch(Pattern.compile(regex));
        }

        public QueryPredicate doesNotContainMatch(Pattern p) {
            condition = (Collection<?> o) -> o.parallelStream().filter(t -> String.class.isAssignableFrom(t.getClass()))
                    .map(t -> (String)t).anyMatch(s -> s == null || !p.matcher(s).matches());
            return query;
        }

        public <T> QueryPredicate containsAll(T... values) {
            return containsAll(Arrays.asList(values));
        }

        public <T> QueryPredicate containsAll(Collection<T> values) {
            collection = true;
            condition = (Collection<T> o) -> o != null && o.containsAll(values);
            return query;
        }

        public <T> QueryPredicate doesNotContainAll(T... values) {
            return doesNotContainAll(Arrays.asList(values));
        }

        public <T> QueryPredicate doesNotContainAll(Collection<T> values) {
            collection = true;
            condition = (Collection<T> o) -> o == null || !o.containsAll(values);
            return query;
        }

        public <T> QueryPredicate containsAny(T... values) {
            return containsAny(Arrays.asList(values));
        }

        public <T> QueryPredicate containsAny(Collection<T> values) {
            collection = true;
            condition = (Collection<T> o) -> o != null && o.parallelStream().anyMatch(values::contains);
            return query;
        }

        public <T> QueryPredicate containsNone(T... values) {
            return containsNone(Arrays.asList(values));
        }

        public <T> QueryPredicate containsNone(Collection<T> values) {
            collection = true;
            condition = (Collection<T> o) -> o == null || o.parallelStream().noneMatch(values::contains);
            return query;
        }

        public <T> QueryPredicate memberOf(T... values) {
            return memberOf(Arrays.asList(values));
        }

        public <T> QueryPredicate memberOf(Collection<T> values) {
            condition = values::contains;
            return query;
        }

        public <T> QueryPredicate notMemberOf(T... values) {
            return notMemberOf(Arrays.asList(values));
        }

        public <T> QueryPredicate notMemberOf(Collection<T> values) {
            condition = (T o) -> !values.contains(o);
            return query;
        }

        public <T> QueryPredicate between(T first, T last, Comparator<T> comparator) {
            return between(first, false, last, false, comparator);
        }

        public <T> QueryPredicate between(T first, boolean inclusive, T last, Comparator<T> comparator) {
            return between(first, inclusive, last, false, comparator);
        }

        public <T> QueryPredicate between(T first, T last, boolean inclusive, Comparator<T> comparator) {
            return between(first, false, last, inclusive, comparator);
        }

        public <T> QueryPredicate between(T first, boolean firstInclusive, T last, boolean lastInclusive,
                                          Comparator<T> comparator) {
            condition = (T o) -> {
                int firstCmp = comparator.compare(first, o);
                int lastCmp = comparator.compare(last, o);
                if (firstInclusive && lastInclusive) {
                    return firstCmp >= 0 && lastCmp <= 0;
                }
                if (firstInclusive) {
                    return firstCmp >= 0 && lastCmp < 0;
                }
                if (lastInclusive) {
                    return firstCmp > 0 && lastCmp <= 0;
                }
                return firstCmp > 0 && lastCmp < 0;
            };
            return query;
        }
    }
}
