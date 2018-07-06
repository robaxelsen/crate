/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.collect;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.aggregation.AggregationContext;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.FieldTypeLookup;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.DataTypes;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.crate.concurrent.CompletableFutures.failedFuture;
import static io.crate.execution.dsl.projection.Projections.shardProjections;
import static io.crate.execution.engine.collect.LuceneShardCollectorProvider.getCollectorContext;

public final class GroupByOptimizedIterator {

    public static BatchIterator<Row> tryBuild(IndexShard indexShard,
                                              LuceneQueryBuilder luceneQueryBuilder,
                                              FieldTypeLookup fieldTypeLookup,
                                              BigArrays bigArrays,
                                              LuceneReferenceResolver referenceResolver,
                                              InputFactory inputFactory,
                                              DocInputFactory docInputFactory,
                                              RoutedCollectPhase collectPhase,
                                              CollectTask collectTask) {
        Collection<? extends Projection> shardProjections = shardProjections(collectPhase.projections());
        GroupProjection groupProjection = getSingleStringKeyGroupProjection(shardProjections);
        if (groupProjection == null) return null;

        assert groupProjection.keys().size() == 1 : "Must have 1 key if getSingleStringKeyGroupProjection returned a projection";
        Reference keyRef = getKeyRef(collectPhase.toCollect(), groupProjection.keys().get(0));
        if (keyRef == null) {
            return null; // group by on non-reference
        }
        MappedFieldType keyFieldType = fieldTypeLookup.get(keyRef.column().fqn());
        if (!keyFieldType.hasDocValues()) {
            return null;
        }
        if (Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE) || Symbols.containsColumn(collectPhase.where(), DocSysColumns.SCORE)) {
            return null; // TODO: handle this in collector?
        }

        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        Engine.Searcher searcher = sharedShardContext.acquireSearcher();
        try {
            QueryShardContext queryShardContext = sharedShardContext.indexService().newQueryShardContext(
                shardId.getId(),
                searcher.reader(),
                System::currentTimeMillis,
                null
            );
            collectTask.addSearcher(sharedShardContext.readerId(), searcher);

            InputFactory.Context<? extends LuceneCollectorExpression<?>> docCtx = docInputFactory.getCtx();
            docCtx.add(Iterables.filter(collectPhase.toCollect(), s -> !s.equals(keyRef)));
            IndexOrdinalsFieldData keyIndexFieldData = queryShardContext.getForField(keyFieldType);

            InputFactory.Context<CollectExpression<Row, ?>> ctxForAggregations = inputFactory.ctxForAggregations();
            ctxForAggregations.add(groupProjection.values());

            List<AggregationContext> aggregations = ctxForAggregations.aggregations();
            List<? extends LuceneCollectorExpression<?>> expressions = docCtx.expressions();

            AggregateMode mode = groupProjection.mode();
            RamAccountingContext ramAccounting = collectTask.queryPhaseRamAccountingContext();

            CollectorContext collectorContext = getCollectorContext(
                sharedShardContext.readerId(), docCtx, queryShardContext::getForField);

            for (LuceneCollectorExpression<?> expression: expressions) {
                expression.startCollect(collectorContext);
            }
            InputRow inputRow = new InputRow(docCtx.topLevelInputs());

            LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
                collectPhase.where(),
                indexShard.mapperService(),
                queryShardContext,
                sharedShardContext.indexService().cache()
            );
            return CollectingBatchIterator.newInstance(
                searcher::close,
                t -> {},
                () -> {
                    Map<BytesRef, Object[]> statesByKey = new HashMap<>();
                    try {
                        Weight weight = searcher.searcher().createNormalizedWeight(queryContext.query(), false);
                        List<LeafReaderContext> leaves = searcher.searcher().getTopReaderContext().leaves();

                        for (LeafReaderContext leaf: leaves) {
                            Scorer scorer = weight.scorer(leaf);
                            if (scorer == null) {
                                continue;
                            }
                            for (LuceneCollectorExpression<?> expression: expressions) {
                                expression.setNextReader(leaf);
                            }
                            SortedSetDocValues values = keyIndexFieldData.load(leaf).getOrdinalsValues();
                            ObjectArray<Object[]> statesByOrd = bigArrays.newObjectArray(values.getValueCount());
                            DocIdSetIterator docs = scorer.iterator();

                            for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                                for (LuceneCollectorExpression<?> expression: expressions) {
                                    expression.setNextDocId(doc);
                                }
                                for (CollectExpression<Row, ?> expression: ctxForAggregations.expressions()) {
                                    expression.setNextRow(inputRow);
                                }
                                if (values.advanceExact(doc)) {
                                    long ord = values.nextOrd();
                                    Object[] states = statesByOrd.get(ord);
                                    if (states == null) {
                                        states = new Object[aggregations.size()];
                                        for (int i = 0; i < aggregations.size(); i++) {
                                            AggregationContext aggregation = aggregations.get(i);
                                            AggregationFunction function = aggregation.function();
                                            states[i] = mode.onRow(
                                                ramAccounting,
                                                function,
                                                function.newState(ramAccounting, Version.CURRENT, bigArrays),
                                                aggregation.inputs()
                                            );
                                        }
                                        statesByOrd.set(ord, states);
                                    } else {
                                        for (int i = 0; i < aggregations.size(); i++) {
                                            AggregationContext aggregation = aggregations.get(i);
                                            states[i] = mode.onRow(
                                                ramAccounting,
                                                aggregation.function(),
                                                states[i],
                                                aggregation.inputs()
                                            );
                                        }
                                    }
                                }
                            }

                            for (long ord = 0; ord < statesByOrd.size(); ord++) {
                                Object[] states = statesByOrd.get(ord);
                                // TODO: merge states of different leafs...
                                if (states == null) {
                                    continue;
                                }
                                statesByKey.put(BytesRef.deepCopyOf(values.lookupOrd(ord)), states);
                            }
                            statesByOrd.close();
                        }
                    } catch (IOException e) {
                        return failedFuture(e);
                    }

                    Iterable<Row> rows = Iterables.transform(statesByKey.entrySet(), new Function<Map.Entry<BytesRef, Object[]>, Row>() {

                        RowN row = new RowN(1 + aggregations.size());
                        Object[] cells = new Object[row.numColumns()];

                        {
                            row.cells(cells);
                        }

                        @Nullable
                        @Override
                        public Row apply(@Nullable Map.Entry<BytesRef, Object[]> input) {
                            assert input != null : "input must not be null";

                            cells[0] = input.getKey();
                            Object[] states = input.getValue();
                            for (int i = 0, c = 1; i < states.length; i++, c++) {
                                cells[c] = mode.finishCollect(ramAccounting, aggregations.get(i).function(), states[i]);
                            }
                            return row;
                        }
                    });
                    return CompletableFuture.completedFuture(rows);
                }
            );
        } catch (Throwable t) {
            searcher.close();
            throw t;
        }
    }



    @Nullable
    private static Reference getKeyRef(List<Symbol> toCollect, Symbol key) {
        if (key instanceof InputColumn) {
            Symbol keyRef = toCollect.get(((InputColumn) key).index());
            if (keyRef instanceof Reference) {
                return ((Reference) keyRef);
            }
        }
        return null;
    }

    private static GroupProjection getSingleStringKeyGroupProjection(Collection<? extends Projection> shardProjections) {
        if (shardProjections.size() != 1) {
            return null;
        }
        Projection shardProjection = shardProjections.iterator().next();
        if (!(shardProjection instanceof GroupProjection)) {
            return null;
        }
        GroupProjection groupProjection = (GroupProjection) shardProjection;
        if (groupProjection.keys().size() != 1 || groupProjection.keys().get(0).valueType() != DataTypes.STRING) {
            return null;
        }
        return groupProjection;
    }
}
