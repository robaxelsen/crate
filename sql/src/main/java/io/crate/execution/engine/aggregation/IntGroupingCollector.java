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

package io.crate.execution.engine.aggregation;

import com.carrotsearch.hppc.IntObjectHashMap;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.symbol.AggregateMode;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.StreamSupport;

public final class IntGroupingCollector implements Collector<Row, IntObjectHashMap<Object[]>, Iterable<Row>> {

    private final CollectExpression<Row, ?>[] expressions;
    private final AggregateMode mode;
    private final AggregationFunction[] aggregations;
    private final Input[][] inputs;
    private final RamAccountingContext ramAccounting;
    private final Input<Integer> keyInput;
    private final Version indexVersionCreated;
    private final BigArrays bigArrays;

    public IntGroupingCollector(CollectExpression<Row, ?>[] expressions,
                                AggregateMode mode,
                                AggregationFunction[] aggregations,
                                Input[][] inputs,
                                RamAccountingContext ramAccounting,
                                Input<Integer> keyInput,
                                Version indexVersionCreated,
                                BigArrays bigArrays) {
        this.expressions = expressions;
        this.mode = mode;
        this.aggregations = aggregations;
        this.inputs = inputs;
        this.ramAccounting = ramAccounting;
        this.keyInput = keyInput;
        this.indexVersionCreated = indexVersionCreated;
        this.bigArrays = bigArrays;
    }

    @Override
    public Supplier<IntObjectHashMap<Object[]>> supplier() {
        return IntObjectHashMap::new;
    }

    @Override
    public BiConsumer<IntObjectHashMap<Object[]>, Row> accumulator() {
        return this::onNextRow;
    }

    @Override
    public BinaryOperator<IntObjectHashMap<Object[]>> combiner() {
        return (state1, state2) -> {
            throw new UnsupportedOperationException("Combine not supported");
        };
    }

    @Override
    public Function<IntObjectHashMap<Object[]>, Iterable<Row>> finisher() {
        return this::mapToRows;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    private void onNextRow(IntObjectHashMap<Object[]> statesByKey, Row row) {
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        int key = keyInput.getInt();
        if (statesByKey.containsKey(key)) {
            Object[] states = statesByKey.get(key);
            for (int i = 0; i < aggregations.length; i++) {
                states[i] = mode.onRow(ramAccounting, aggregations[i], states[i], inputs[i]);
            }
        } else {
            addNewEntry(statesByKey, key);
        }
    }

    private void addNewEntry(IntObjectHashMap<Object[]> statesByKey, int key) {
        Object[] states = new Object[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            AggregationFunction aggregation = aggregations[i];
            states[i] = mode.onRow(
                ramAccounting,
                aggregation,
                aggregation.newState(ramAccounting, indexVersionCreated, bigArrays),
                inputs[i]
            );
        }
        statesByKey.put(key, states);
    }

    private Iterable<Row> mapToRows(IntObjectHashMap<Object[]> stateByKey) {
        final Object[] cells = new Object[1 + aggregations.length];
        final Row row = new RowN(cells);
        return StreamSupport.stream(stateByKey.spliterator(), false)
            .map(cursor -> {
                cells[0] = cursor.key;
                Object[] states = cursor.value;
                for (int c = 1, i = 0; i < states.length; i++, c++) {
                    cells[c] = mode.finishCollect(ramAccounting, aggregations[i], states[i]);
                }
                return row;
            })::iterator;
    }
}
