package org.juno.calcite.adapter.neo4j;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class Neo4jRules {
    public static final Convention NEO4J = new Convention.Impl("NEO4J", Neo4jRel.class);

    public static final RelOptRule AGGREGATE = new Neo4jAggregateRule();

    private static class Neo4jAggregateRule extends ConverterRule {
        private Neo4jAggregateRule() {
            super(LogicalAggregate.class, Convention.NONE, NEO4J,
                    "Neo4jAggregateRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            final LogicalAggregate agg = (LogicalAggregate) rel;
            final RelTraitSet traitSet = agg.getTraitSet().replace(NEO4J);
            final RelNode input = agg.getInput();
            
            return new Neo4jAggregate(
                    agg.getCluster(),
                    traitSet,
                    convert(input, input.getTraitSet().replace(NEO4J)),
                    agg.getGroupSet(),
                    agg.getGroupSets(),
                    agg.getAggCallList());
        }
    }
}