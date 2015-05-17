package com.github.mercurydb.queryutils.graph;

import com.github.mercurydb.queryutils.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.*;

public class HgQueryGraph<T> {
    private HgTupleStream stream;
    private ValueExtractable sourceId;
    private ValueExtractable targetId;

    private Map<Object, ClosureNode> nodeMap;

    public HgQueryGraph(HgTupleStream stream,
                        ValueExtractable sourceId,
                        ValueExtractable targetId) {
        this.stream = stream;
        this.sourceId = sourceId;
        this.targetId = targetId;
        initGraph();
    }

    private void initGraph() {
        // first we setup the node map of joined elements -> node wrappers
        nodeMap = Maps.newIdentityHashMap();
        stream.forEach(t -> {
            Object sourceKey = sourceId.extractValue(t.get(sourceId.getTableId()));
            Object targetKey = targetId.extractValue(t.get(targetId.getTableId()));
            ClosureNode sourceNode = nodeMap.get(sourceKey);
            ClosureNode targetNode = nodeMap.get(targetKey);
            if (sourceNode == null) {
                sourceNode = new ClosureNode(sourceKey);
                nodeMap.put(sourceKey, sourceNode);
            }
            if (targetNode == null) {
                targetNode = new ClosureNode(targetKey);
                nodeMap.put(targetKey, targetNode);
            }
            sourceNode.next.add(targetNode);
        });

    }

    public int countIntermediateEdges(T sourceKey, T targetKey, int maxSteps) {
        return calculateSteps(nodeMap.get(sourceKey), nodeMap.get(targetKey), 0, maxSteps);
    }

    public int countIntermediateEdges(T sourceKey, T targetKey) {
        return calculateSteps(nodeMap.get(sourceKey), nodeMap.get(targetKey), 0, -1);
    }

    public List<T> breadthFirstFrom(T sourceKey) {
        List<T> result = new LinkedList<>();
        breadthFirstFromHelper(nodeMap.get(sourceKey), result);
        return result;
    }

    private void breadthFirstFromHelper(ClosureNode node, List<T> result) {
        if (node == null) {
            return;
        }
        result.add(node.data);
        node.next.forEach(n -> breadthFirstFromHelper(n, result));
    }

    private int calculateSteps(ClosureNode nA, ClosureNode nB, int depth, int maxSteps) {
        if (nA == null || nB == null || (maxSteps >= 0 && depth >= maxSteps)) {
            return -1;
        } else if (nA == nB) {
            return depth;
        }

        for (ClosureNode n : nA.next) {
            int result = calculateSteps(n, nB, depth+1, maxSteps);
            if (result >= 0) {
                return result;
            }
        }

        return -1;
    }

    public HgStream<T> streamBreadthFirstFrom(T start) {
        ClosureNode startNode = nodeMap.get(start);
        return new HgWrappedIterableStream<T>(breadthFirstFrom(start)) {
            @Override
            public HgTupleStream joinOn(ValueExtractable fe) {
                return HgTupleStream.createJoinInput(fe, this, true);
            }
        };
    }

    public HgBiPredicate<T, T> reachabilityPredicate() {
        return (o1, o2) -> calculateSteps(nodeMap.get(o1), nodeMap.get(o2), 0, -1) >= 0;
    }

    public HgBiPredicate<T, T> reachabilityPredicate(int minSteps, int maxSteps) {
        return (o1, o2) -> calculateSteps(
                nodeMap.get(o1), nodeMap.get(o2), 0,
                maxSteps) >= minSteps;
    }

    private class ClosureNode {
        private Set<ClosureNode> next = Sets.newIdentityHashSet();
        private T data;
        private ClosureNode(Object data) {
            this.data = (T)data;
        }
    }
}
