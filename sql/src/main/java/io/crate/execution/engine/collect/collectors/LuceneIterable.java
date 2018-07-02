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

package io.crate.execution.engine.collect.collectors;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class LuceneIterable implements Iterable<LuceneIterable.Docs> {

    private final IndexSearcher indexSearcher;
    private final Query query;
    private final boolean needsScores;

    @Nullable
    private Weight weight;

    public LuceneIterable(IndexSearcher indexSearcher,
                          Query query,
                          boolean needsScores) {
        this.indexSearcher = indexSearcher;
        this.query = query;
        this.needsScores = needsScores;
    }

    @Override
    public Iterator<Docs> iterator() {
        if (weight == null) {
            try {
                weight = indexSearcher.createNormalizedWeight(query, needsScores);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new DocsIterator(indexSearcher.getTopReaderContext().leaves(), weight);
    }

    static final class Docs extends DocIdSetIterator {

        final LeafReaderContext leaf;
        private final Scorer scorer;
        private final DocIdSetIterator it;
        private final LeafReader reader;
        private final Bits liveDocs;

        Docs(LeafReaderContext leaf, Scorer scorer) {
            reader = leaf.reader();
            this.leaf = leaf;
            this.scorer = scorer;
            this.it = scorer.iterator();
            liveDocs = reader.getLiveDocs();
        }

        @Override
        public int docID() {
            return it.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            for (int doc; (doc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS; ) {
                if (docDeleted(doc)) {
                    continue;
                }
                return doc;
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        private boolean docDeleted(int doc) {
            return liveDocs != null  && liveDocs.get(doc) == false;
        }

        @Override
        public int advance(int target) throws IOException {
            return it.advance(target);
        }

        @Override
        public long cost() {
            return it.cost();
        }
    }

    private static class DocsIterator implements Iterator<Docs> {
        private final Weight weight;
        private final Iterator<LeafReaderContext> leavesIt;

        private Docs docs = null;

        DocsIterator(List<LeafReaderContext> leaves, Weight weight) {
            this.weight = weight;
            this.leavesIt = leaves.iterator();
        }

        @Override
        public boolean hasNext() {
            if (docs == null) {
                docs = nextDocs();
            }
            return docs != null;
        }

        @Override
        public Docs next() {
            if (!hasNext()) {
                throw new NoSuchElementException("DocsIterator exhausted");
            }
            Docs docs = this.docs;
            this.docs = null;
            return docs;
        }

        @Nullable
        private Docs nextDocs() {
            Scorer scorer = null;
            LeafReaderContext leaf = null;
            while (scorer == null && leavesIt.hasNext()) {
                leaf = leavesIt.next();
                try {
                    scorer = weight.scorer(leaf);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (scorer == null) {
                return null;
            }
            return new Docs(leaf, scorer);
        }
    }
}
