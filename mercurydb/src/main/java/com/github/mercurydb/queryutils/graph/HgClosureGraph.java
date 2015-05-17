package com.github.mercurydb.queryutils.graph;

import com.github.mercurydb.queryutils.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.*;

public class HgClosureGraph<T> {
    private HgTupleStream stream;
    private ValueExtractable sourceId;
    private ValueExtractable targetId;

    private Map<Object, ClosureNode> nodeMap;

    public HgClosureGraph(HgTupleStream stream,
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
                sourceNode = new ClosureNode();
                nodeMap.put(sourceKey, sourceNode);
            }
            if (targetNode == null) {
                targetNode = new ClosureNode();
                nodeMap.put(targetKey, targetNode);
            }
            sourceNode.next.add(targetNode);
        });

    }


    public int calculateSteps(ClosureNode nA, ClosureNode nB, int depth, int maxSteps) {
        if (maxSteps >= 0 && depth >= maxSteps) {
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

    public JoinPredicate transitiveClosurePredicate() {
        return new JoinPredicate(stream.joinOn(sourceId), stream.joinOn(targetId), (o1, o2) ->
                calculateSteps(nodeMap.get(o1), nodeMap.get(o2), 0, -1) >= 0
        );
    }

    public JoinPredicate transitiveClosurePredicate(int maxSteps) {
        return new JoinPredicate(stream.joinOn(sourceId), stream.joinOn(targetId), (o1, o2) ->
                calculateSteps(nodeMap.get(o1), nodeMap.get(o2), 0, maxSteps) >= 0
        );
    }

    private class ClosureNode {
        private Set<ClosureNode> next = Sets.newIdentityHashSet();
    }
}
