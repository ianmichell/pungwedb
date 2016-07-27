package com.pungwe.db.core.command;

import java.util.*;
import java.util.function.Predicate;

/**
 * Created by ian on 18/06/2016.
 */
public class Query {

    private final Set<String> fields;
    private final QueryPredicate predicate;

    // FIXME: Add some cool modifiers here...
    private Query(String... fields) {
        Set<String> selectedFields = new LinkedHashSet<>();
        Collections.addAll(selectedFields, fields);
        this.fields = Collections.unmodifiableSet(selectedFields);
        this.predicate = new QueryPredicate(this);
    }

    public static QueryPredicate select(String... fields) {
        return new QueryPredicate(new Query(fields));
    }

    public FieldPredicate where(String field) {
        return new FieldPredicate(predicate, field);
    }

    private static class QueryPredicate {
        private Query query;
        private FieldPredicate predicate;
        private QueryPredicate and, or;

        private QueryPredicate(Query query) {
            this.query = query;
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
    private static class FieldPredicate {

        private final QueryPredicate query;
        private final String field;
        private Predicate<?> condition;

        private FieldPredicate(QueryPredicate query, String field) {
            this.query = query;
            this.field = field;
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
            condition = (T o) -> comparator.compare(o, value) > 0;
            return query;
        }

        public <T> QueryPredicate contains(T value) {
            condition = (Collection<T> o) -> o != null && o.contains(value);
            return query;
        }

        public <T> QueryPredicate doesNotContain(T value) {
            condition = (Collection<T> o) -> o == null || !o.contains(value);
            return query;
        }

        public <T> QueryPredicate containsAll(T... values) {
            return containsAll(Arrays.asList(values));
        }

        public <T> QueryPredicate containsAll(Collection<T> values) {
            condition = (Collection<T> o) -> o != null && o.containsAll(values);
            return query;
        }

        public <T> QueryPredicate doesNotContainAll(T... values) {
            return doesNotContainAll(Arrays.asList(values));
        }

        public <T> QueryPredicate doesNotContainAll(Collection<T> values) {
            condition = (Collection<T> o) -> o == null || !o.containsAll(values);
            return query;
        }

        public <T> QueryPredicate containsAny(T... values) {
            return containsAny(Arrays.asList(values));
        }

        public <T> QueryPredicate containsAny(Collection<T> values) {
            condition = (Collection<T> o) -> o != null && o.parallelStream().anyMatch(values::contains);
            return query;
        }

        public <T> QueryPredicate containsNone(T... values) {
            return containsNone(Arrays.asList(values));
        }

        public <T> QueryPredicate containsNone(Collection<T> values) {
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
