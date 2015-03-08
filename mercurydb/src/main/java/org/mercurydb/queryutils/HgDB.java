package org.mercurydb.queryutils;

import java.util.*;

/**
 * <p>
 * This class is the main class used for queries and join operations
 * on a hgdb instance. It can be used like the following:
 * </p>
 * <p>
 * For 1 predicate -- A.X = B.Y
 * JoinDriver.join(TableA.joinX(), TableB.joinY());
 * </p>
 * For >1 predicates -- A.X=B.Y and B.C=C.D and A.Y=D.F
 * <pre>
 * JoinDriver.join(
 * new Predicate(TableA.joinX(), TableB.joinY()),
 * new Predicate(TableB.joinC(), TableC.joinD()),
 * new Predicate(TableA.joinY(), TableD.joinF()));
 * </pre>
 * </p>
 * <p>
 * All join methods return a JoinResult, which is basically
 * an Iterator<JoinRecord>. Here is an example of a join method
 * in code and how to retrieve data values from JoinRecords:
 * </p>
 * <pre>
 * for (JoinRecord jr : JoinDriver.join(TableA.joinX(), TableB.joinY())) {
 * A x = (A)jr.get(A.class); // Always returns Object, so must cast
 * B y = (B)jr.get(B.class); // Always returns Object, so must cast
 * }
 * </pre>
 */
public class HgDB {
    @SafeVarargs
    public static<T> HgStream<T> query(AbstractFieldExtractablePredicate<T>... extractableValues) {
        if (extractableValues.length == 0) {
            throw new IllegalArgumentException("Must supply at least one argument to query");
        }

        HgStream<T> seed = extractableValues[0].getDefaultStream();

        // find smallest applicable index
        int smallestIndex = Integer.MAX_VALUE;
        Set<Object> index = null;
        FieldExtractable usedFE = null;
        for (FieldExtractableSeed<T> fe : extractableValues) {
            if (fe instanceof FieldExtractableRelation) {
                FieldExtractableRelation<T> fer = (FieldExtractableRelation<T>) fe;
                if (fer.isIndexed() && fer.relation == HgRelation.EQ) {
                    Set<Object> newIndex = fe.getIndex().get(fer.value);
                    int newSmallestIndex = newIndex.size();
                    if (newSmallestIndex < smallestIndex) {
                        smallestIndex = newSmallestIndex;
                        index = newIndex;
                        usedFE = fe;
                    }
                }
            }
        }

        if (index != null) {
            seed = new Retrieval(index, index.size());
        } else {
            seed = extractableValues[0].getDefaultStream();
        }

        for (AbstractFieldExtractablePredicate<T> fe : extractableValues) {
            if (fe == usedFE) continue;
            seed = seed.filter(fe);
        }
        return seed;
    }

    /**
     * Joins a set of Predicates. Creates optimal
     * join operation according to those rules defined
     * in JoinPredicate.
     *
     * @param preds One or more JoinPredicates
     * @return a JoinResult of a join on preds
     * @throws IllegalStateException if preds do not unify
     */
    public static HgPolyTupleStream join(JoinPredicate... preds) {
        if (preds.length == 1) {
            return join(preds[0].predicate, preds[0].stream1, preds[0].stream2);
        } else if (preds.length > 1) {
            Arrays.sort(preds);
            System.out.println("Joining " + preds[0].stream1.getContainerClass() +
                    " x " + preds[0].stream2.getContainerClass());
            HgPolyTupleStream result = join(preds[0].stream1, preds[0].stream2);

            for (int i = 1; i < preds.length; ++i) {
                JoinPredicate p = preds[i];
                if (result.getContainedTypes().contains(p.stream1.getContainerClass())) {
                    p.stream1 = result.joinOn(p.stream1);
                } else if (result.getContainedTypes().contains(p.stream2.getContainerClass())) {
                    p.stream2 = result.joinOn(p.stream2);
                } else {
                    continue;
                }

                return join(Arrays.copyOfRange(preds, 1, preds.length));
            }

            throw new IllegalStateException("Predicates do not unify!");
        } else {
            throw new IllegalArgumentException("Must supply at least one predicate to join.");
        }
    }

    /**
     * Returns a HgPolyTupleStream using an equality predicate.
     *
     * @param a HgTupleStream // TODO documentation
     * @param b HgTupleStream // TODO documentation
     * @return The HgPolyTupleStream resulting from performing on a join on the inputs with the equality relation.
     */
    public static HgPolyTupleStream join(
            final HgTupleStream a,
            final HgTupleStream b) {
        return join(HgRelation.EQ, a, b);
    }

    /**
     * Joins two JoinStreams and returns a JoinResult. A JoinResult
     * is a JoinStream which is basically an iterator over JoinRecords.
     * JoinRecords are essentially a Map of class types to Objects. So you
     * can select Objects from the result based on their class type. It
     * will select the correct join method based on the index status
     * of its arguments.
     *
     * @param a        HgTupleStream // TODO documentation
     * @param b        HgTupleStream // TODO documentation
     * @param relation HgRelation // TODO documentation
     * @return The HgPolyTupleStream resulting from performing on a join on the inputs with the given relation.
     */
    public static HgPolyTupleStream join(
            HgRelation relation,
            final HgTupleStream a,
            final HgTupleStream b) {
        if (a.getContainerClass().equals(b.getContainerClass())) {
            /*
             * Self Join
             */
            throw new UnsupportedOperationException("Self joins not currently supported :(");
        } else if (!a.getContainedTypes().retainAll(b.getContainedTypes())
                || !b.getContainedTypes().retainAll(a.getContainedTypes())) {
            /*
             * Filter operation
             */
            return joinFilter(a, b);
        } else if (a.isIndexed() && b.isIndexed()) {
            /*
             * Both A and B indexed
             * Do index intersection A, B
             */
            return joinIndexIntersection(a, b);
        } else if (a.isIndexed() || b.isIndexed()) {
            /*
             * Only A indexed
             * Scan B, use A index
             */
            return joinIndexScan(a, b);
        } else {
            /*
             * Neither is indexed
             * Do hash join
             */
            return joinHash(a, b);
        }
    }

    /**
     * Performs a filter join on two streams
     * where one's set of contained types is
     * a subset of the other's.
     *
     * @param a HgTupleStream // TODO documentation
     * @param b HgTupleStream // TODO documentation
     * @return HgPolyTupleStream // TODO documentation
     */
    @SuppressWarnings("unchecked")
    private static HgPolyTupleStream joinFilter(
            final HgTupleStream a,
            final HgTupleStream b) {
        final HgTupleStream ap;
        final HgTupleStream bp;

        // Perform Filter operation on A
        if (b.getContainedTypes().retainAll(a.getContainedTypes())) {
            ap = a;
            bp = b;
        } else {
            ap = b;
            bp = b;
        }

        return new HgPolyTupleStream(a, b) {
            //Iterator<Object> aKeys = ap.keys().iterator();
            HgTuple currA;

            @Override
            public boolean hasNext() {
                if (ap.hasNext()) {
                    currA = ap.next();
                    Object jkv1o = ap.extractFieldFromTuple(currA);
                    Object jkv2o = bp.extractFieldFromTuple(currA);

                    // TODO why does having these be equal mean hasNext() is true?
                    // TODO also if this logic is sound, reduce it into a single expression as IntelliJ recommends
                    if (jkv1o.equals(jkv2o)) {
                        return true;
                    } else {
                        return hasNext();
                    }
                }

                return false;
            }

            @Override
            public HgTuple next() {
                return currA;
            }
        };
    }

    /**
     * Joins two JoinStreams. Both of which must be
     * indexed. Performs an intersection of two JoinStreams,
     * both of which must be index retrievals.
     *
     * @param a // TODO documentation
     * @param b // TODO documentation
     * @return // TODO documentation
     */
    @SuppressWarnings("unchecked")
    private static HgPolyTupleStream joinIndexIntersection(
            final HgTupleStream a,
            final HgTupleStream b) {
        return new HgPolyTupleStream(a, b) {
            Iterator<Object> aKeys = a.getIndex().keySet().iterator();
            Iterator<Object> aInstances = Collections.emptyIterator();
            Object currA;
            Iterator<Object> bInstances = Collections.emptyIterator();
            Object currB;
            Iterable<Object> bSeed;

            @Override
            public boolean hasNext() {
                // Note: this was difficult for Cole's feeble mind to think about
                // TODO: comment this sorcery
                if (bInstances.hasNext()) {
                    currB = bInstances.next();
                    return true;
                } else if (aInstances.hasNext()) {
                    currA = aInstances.next();
                    bInstances = bSeed.iterator();
                    return hasNext();
                }

                while (aKeys.hasNext()) {
                    Object currKey = aKeys.next();
                    bSeed = b.getIndex().get(currKey);
                    if (bSeed != null) {
                        aInstances = a.getIndex().get(currKey).iterator();
                        return hasNext();
                    }
                }
                return false;
            }

            @Override
            public HgTuple next() {
                return new HgTuple(a, currA, b, currB);
            }
        };
    }

    /**
     * Joins two JoinStreams, one of which must
     * be indexed. Scans over the non-indexed stream
     * and uses the index on the indexed stream.
     *
     * @param a // TODO documentation
     * @param b // TODO documentation
     * @return // TODO documentation
     */
    @SuppressWarnings("unchecked")
    private static HgPolyTupleStream joinIndexScan(
            final HgTupleStream a,
            final HgTupleStream b) {
        System.out.println("Performing Index Scan");

        final HgTupleStream ap;
        final HgTupleStream bp;

        if (b.isIndexed()) {
            ap = b;
            bp = a;
        } else {
            ap = a;
            bp = b;
        }

        return new HgPolyTupleStream(a, b) {
            Object currB;
            Iterator<Object> bInstances = bp.getObjectIterator();
            Iterator<Object> aInstances = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                if (aInstances.hasNext()) {
                    return true;
                } else {
                    while (bInstances.hasNext()) {
                        currB = bInstances.next();
                        Object currKey = bp.extractField(currB);
                        Iterable<Object> aIterable = ap.getIndex().get(currKey);
                        if (aIterable != null) {
                            aInstances = aIterable.iterator();
                            return true;
                        }
                    }
                }

                return false;
            }

            @Override
            public HgTuple next() {
                return new HgTuple(ap, aInstances.next(), bp, currB);
            }
        };
    }

    /**
     * Simple hash join algorithm. Inhales the results
     * into a // TODO finish documentation
     *
     * @param a // TODO documentation
     * @param b // TODO documentation
     * @return // TODO documentation
     */
    @SuppressWarnings("unchecked")
    public static HgPolyTupleStream joinHash(
            final HgTupleStream a,
            final HgTupleStream b) {
        System.out.println("Performing Hash Join.");

        final HgTupleStream ap;
        final HgTupleStream bp;

        if (a.getCardinality() < b.getCardinality()) {
            ap = a;
            bp = b;
        } else {
            ap = b;
            bp = a;
        }

        final Map<Object, Set<Object>> aMap = new HashMap<>();

        // Inhale stream A into hash table
        for (HgTuple aInstance : ap) {
            Object key = ap.extractFieldFromTuple(aInstance);

            Set<Object> l = aMap.get(key);
            if (l == null) {
                l = new HashSet<>();
            }

            l.add(aInstance);
            aMap.put(key, l);
        }

        FieldExtractableFakeIndex fei = new FieldExtractableFakeIndex(ap.getFieldExtractor(), aMap);
        ap.setJoinKey(fei);
        return joinIndexScan(ap, bp);
    }

    /**
     * Simple nested loops join algorithm.
     *
     * @param a // TODO documentation
     * @param b // TODO documentation
     * @return // TODO documentation
     */
    public static HgPolyTupleStream joinNestedLoops(
            final HgTupleStream a,
            final HgTupleStream b) {
        return new HgPolyTupleStream(a, b) {
            HgTuple currA, currB;

            @Override
            public boolean hasNext() {
                while (b.hasNext() && currA != null) {
                    currB = b.next();
                    if (a.extractFieldFromTuple(currA)
                            .equals(b.extractFieldFromTuple(currB))) {
                        return true;
                    }
                }

                if (a.hasNext()) {
                    b.reset();
                    currA = a.next();
                    return hasNext();
                }

                return false;
            }

            @Override
            public HgTuple next() {
                return new HgTuple(a, currA, b, currB);
            }
        };
    }
}