package com.github.mercurydb.queryutils;

/**
 * Used by generated table code
 */
public class ValueExtractablePredicate<T, F> extends AbstractValueExtractablePredicate<T, F> {
    public final HgPredicate predicate;

    public ValueExtractablePredicate(ValueExtractableSeed fe, HgPredicate<F> predicate) {
        super(fe);
        this.predicate = predicate;
    }

    public boolean test(Object value) {
        return predicate.test(value);
    }
}
