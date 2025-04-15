package org.juno.calcite.adapter.neo4j;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Neo4jAggregate extends Aggregate implements Neo4jRel {
    public Neo4jAggregate(RelOptCluster cluster,
                         RelTraitSet traitSet,
                         RelNode input,
                         ImmutableBitSet groupSet,
                         List<ImmutableBitSet> groupSets,
                         List<AggregateCall> aggCalls) {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input,
                         ImmutableBitSet groupSet,
                         List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new Neo4jAggregate(getCluster(), traitSet, input,
                groupSet, groupSets, aggCalls);
    }

    private String getAggregationFunction(AggregateCall aggCall) {
        if (aggCall.getAggregation().equals(SqlStdOperatorTable.COUNT)) {
            return "COUNT";
        } else if (aggCall.getAggregation().equals(SqlStdOperatorTable.SUM)) {
            return "SUM";
        } else if (aggCall.getAggregation().equals(SqlStdOperatorTable.MIN)) {
            return "MIN";
        } else if (aggCall.getAggregation().equals(SqlStdOperatorTable.MAX)) {
            return "MAX";
        } else if (aggCall.getAggregation().equals(SqlStdOperatorTable.AVG)) {
            return "AVG";
        } else if (aggCall.getAggregation().kind == SqlKind.COLLECT) {
            return "COLLECT";
        } else if (aggCall.getAggregation().kind == SqlKind.STDDEV_POP) {
            return "STDEV";
        } else if (aggCall.getAggregation().kind == SqlKind.STDDEV_SAMP) {
            return "STDEVP";
        } else {
            throw new UnsupportedOperationException(
                "Unsupported aggregation function: " + aggCall.getAggregation());
        }
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());

        Set<Integer> neededColumns = new HashSet<>(groupSet.asList());
for (AggregateCall aggCall : aggCalls) {
    neededColumns.addAll(aggCall.getArgList());
}
        
        // Handle grouping
        // if (!groupSet.isEmpty()) {
        //     implementor.addWith(" WITH ");
        //     int i = 0;
        //     for (int group : groupSet) {
        //         if (i > 0) {
        //             implementor.add(", ");
        //         }
        //         implementor.add(implementor.getFieldNames().get(group));
        //         i++;
        //     }
        // }
        if (!neededColumns.isEmpty()) {
            implementor.addWith(" WITH ");
            int i = 0;
            for (int column : neededColumns) {
                if (i > 0) {
                    implementor.add(", ");
                }
                implementor.add(implementor.getFieldNames().get(column));
                i++;
            }
        }
        // Handle aggregation
        implementor.add(" RETURN ");
        int i = 0;
        // First add grouping columns
        for (int group : groupSet) {
            if (i > 0) {
                implementor.add(", ");
            }
            implementor.add(implementor.getFieldNames().get(group));
            i++;
        }

        // Then add aggregation functions
        for (AggregateCall aggCall : aggCalls) {
            if (i > 0) {
                implementor.add(", ");
            }

            String aggFunction = getAggregationFunction(aggCall);
            
            if (aggCall.isDistinct()) {
                implementor.add(aggFunction)
                          .add("(DISTINCT ");
            } else {
                implementor.add(aggFunction)
                          .add("(");
            }
            
            if (aggCall.getArgList().isEmpty()) {
                implementor.add("*");  // COUNT(*)
            } else {
                int j = 0;
                // Count specific columns, the columns need to be added in WITH clause!!!!
                for (Integer arg : aggCall.getArgList()) {
                    if (j > 0) {
                        implementor.add(", ");
                    }
                    implementor.add(implementor.getFieldNames().get(arg));
                    j++;
                }
            }
            implementor.add(")");
            
            if (aggCall.name != null) {
                implementor.add(" AS ")
                          .add(aggCall.name);
            }
            i++;
        }
    }
}