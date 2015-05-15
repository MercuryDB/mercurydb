package com.github.mercurydb.queryutils.graph;

import com.github.mercurydb.queryutils.HgBiPredicate;
import com.github.mercurydb.queryutils.HgDB;
import com.github.mercurydb.queryutils.HgTupleStream;
import com.github.mercurydb.queryutils.JoinPredicate;

import java.util.HashMap;
import java.util.Map;

public class HgClosureGraph<TA, TB> {

    private HgTupleStream streamA;
    private HgTupleStream streamB;
    private HgBiPredicate<TA, TB> connectPredicate;

    private Map<Object, Node> nodeMap;

    public HgClosureGraph(HgTupleStream streamA, HgTupleStream streamB, HgBiPredicate<TA, TB> connectPredicate) {
        this.streamA = streamA;
        this.streamB = streamB;
        this.connectPredicate = connectPredicate;

        initializeGraph();
    }

    private void initializeGraph() {
        // first we setup the node map of joined elements -> node wrappers
        initNodeMap();

        /*
         * Now we actually build up the graph. We can do this by seeding this graph
         * with a join of the parameters in the constructor.
         */
        for (HgTupleStream.HgTuple t : HgDB.join(streamA, streamB, connectPredicate)) {
            Object oA = streamA.extractFieldFromTuple(t);
            Object oB = streamB.extractFieldFromTuple(t);

            Node nA = nodeMap.get(oA);
            Node nB = nodeMap.get(oB);

            nA.next = nB;
        }
    }

    private void initNodeMap() {
        nodeMap = new HashMap<>();

        // first let's wrap the extracted elements in Nodes
        for (HgTupleStream.HgTuple t : streamA) {
            Object element = streamA.extractFieldFromTuple(t);
            nodeMap.put(element, new Node(element));
        }

        for (HgTupleStream.HgTuple t : streamB) {
            Object element = streamB.extractFieldFromTuple(t);
            nodeMap.put(element, new Node(element));
        }

        streamA.reset();
        streamB.reset();
    }

    public int calculateSteps(Node nA, Node nB, int maxSteps) {
        int count = 0;
        while (nA != null && nA != nB && (maxSteps < 0 || count < maxSteps)) {
            nA = nA.next;
            ++count;
        }
        return nA == nB ? -1 : count;
    }

    public JoinPredicate transitiveClosurePredicate() {
        return new JoinPredicate(streamA, streamB, (o1, o2) ->
                calculateSteps(nodeMap.get(o1), nodeMap.get(o2), -1) >= 0
        );
    }

    public JoinPredicate transitiveClosurePredicate(int maxSteps) {
        return new JoinPredicate(streamA, streamB, (o1, o2) ->
                calculateSteps(nodeMap.get(o1), nodeMap.get(o2), maxSteps) >= 0
        );
    }

    private class Node<T> {
        private Node next;
        private T data;

        private Node(T data) {
            this.data = data;
        }
    }
}
