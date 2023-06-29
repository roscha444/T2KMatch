/**
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */
package de.uni_mannheim.informatik.dws.t2k.match.components;

import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.t2k.similarity.WebJaccardStringSimilarity;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.SimilarityFloodingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.ComparatorLogger;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.DeviationSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Component that runs the label-based similarity flooding algorithm.
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SFValueBasedMatching {

    Map<MatchableTableColumn, MatchableTableColumn> oldToNew = new HashMap<>();

    SurfaceForms surfaceForms;

    public void setSf(SurfaceForms sf) {
        this.surfaceForms = sf;
    }

    public static class SFComparatorWebJaccard implements Comparator<MatchableTableColumn, MatchableTableColumn> {

        Map<MatchableTableColumn, MatchableTableColumn> oldToNew = new HashMap<>();

        private static final long serialVersionUID = 1L;
        private final WebJaccardStringSimilarity similarity = new WebJaccardStringSimilarity();
        private ComparatorLogger comparisonLog;
        private Map<Integer, Map<Integer, List<Correspondence<MatchableTableRow, MatchableTableColumn>>>> tableToCorrespondenceMap;

        private SimilarityMeasure<String> stringSimilarity = new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5);

        private SimilarityMeasure<Double> numericSimilarity = new DeviationSimilarity();

        private WeightedDateSimilarity dateSimilarity = new WeightedDateSimilarity(1, 3, 5);


        SurfaceForms sf;
        private KnowledgeBase kb;

        public SFComparatorWebJaccard(Map<MatchableTableColumn, MatchableTableColumn> oldToNew,
            Map<Integer, Map<Integer, List<Correspondence<MatchableTableRow, MatchableTableColumn>>>> tableToCorrespondenceMap, SurfaceForms sf, KnowledgeBase kb) {
            this.oldToNew = oldToNew;
            this.tableToCorrespondenceMap = tableToCorrespondenceMap;
            this.sf = sf;
            this.kb = kb;
        }

        @Override
        public double compare(MatchableTableColumn record1, MatchableTableColumn record2, Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {

            MatchableTableRowComparatorBasedOnSurfaceForms mt = new MatchableTableRowComparatorBasedOnSurfaceForms(stringSimilarity, kb.getPropertyIndices(), 0.2, sf);

            MatchableTableColumn secondRecord = oldToNew.get(record2);
            if (tableToCorrespondenceMap.containsKey(record1.getTableId()) && tableToCorrespondenceMap.get(record1.getTableId()).containsKey(secondRecord.getTableId())) {

                List<Correspondence<MatchableTableRow, MatchableTableColumn>> corrList = tableToCorrespondenceMap.get(record1.getTableId()).get(secondRecord.getTableId());
                double result = 0.0;
                int countResult = 0;
                for (Correspondence<MatchableTableRow, MatchableTableColumn> corr : corrList) {
                    int columnIndexFirstTable = record1.getColumnIndex();
                    DataType firstColumnType = corr.getFirstRecord().getType(columnIndexFirstTable);

                    int columnIndexSecondTable = secondRecord.getColumnIndex();
                    DataType secondColumnType = corr.getSecondRecord().getType(columnIndexSecondTable);

                    if (firstColumnType != null && secondColumnType != null) {
                        countResult++;
                        if (firstColumnType.equals(secondColumnType)) {
                            if (firstColumnType.equals(DataType.string)) {
                                result += mt.compare(corr.getFirstRecord(), corr.getSecondRecord(), columnIndexFirstTable, columnIndexSecondTable);
                            } else if (firstColumnType.equals(DataType.numeric)) {
                                result += numericSimilarity.calculate((Double) corr.getFirstRecord().get(columnIndexFirstTable), (Double) corr.getSecondRecord().get(columnIndexSecondTable));
                            } else if (firstColumnType.equals(DataType.date)) {
                                result += dateSimilarity.calculate((LocalDateTime) corr.getFirstRecord().get(columnIndexFirstTable),
                                    (LocalDateTime) corr.getSecondRecord().get(columnIndexSecondTable));
                            }
                        }
                    }
                }
                return result / countResult;
            }
            return 0.0;
        }

        @Override
        public ComparatorLogger getComparisonLog() {
            return this.comparisonLog;
        }

        @Override
        public void setComparisonLog(ComparatorLogger comparatorLog) {
            this.comparisonLog = comparatorLog;
        }

    }

    private MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
    private WebTables web;
    private KnowledgeBase kb;
    private Map<Integer, Set<String>> classesPerTable;
    private Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences;

    /**
     * @param instanceCorrespondences the instanceCorrespondences to set
     */
    public void setInstanceCorrespondences(
        Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {
        this.instanceCorrespondences = instanceCorrespondences;
    }

    public SFValueBasedMatching(MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, WebTables web, KnowledgeBase kb, Map<Integer, Set<String>> classesPerTable,
        Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {
        this.matchingEngine = matchingEngine;
        this.web = web;
        this.kb = kb;
        this.classesPerTable = classesPerTable;
        this.instanceCorrespondences = instanceCorrespondences;
    }

    public Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> run() throws Exception {

        Map<Integer, Map<Integer, List<Correspondence<MatchableTableRow, MatchableTableColumn>>>> tableToCorrespondenceMap = new HashMap<>();

        for (Correspondence<MatchableTableRow, MatchableTableColumn> corr : instanceCorrespondences.get()) {
            int firstTableId = corr.getFirstRecord().getTableId();
            if (!tableToCorrespondenceMap.containsKey(firstTableId)) {
                tableToCorrespondenceMap.put(firstTableId, new HashMap<>());
            }
            int secondTableId = corr.getSecondRecord().getTableId();
            if (!tableToCorrespondenceMap.get(firstTableId).containsKey(secondTableId)) {
                tableToCorrespondenceMap.get(firstTableId).put(secondTableId, new ArrayList<>());
            }
            tableToCorrespondenceMap.get(firstTableId).get(secondTableId).add(corr);
        }

        Map<Integer, List<MatchableTableColumn>> columnsPerWebTable = web.getSchema().get().stream().collect(Collectors.groupingBy(MatchableTableColumn::getTableId));
        Map<Integer, List<MatchableTableColumn>> columnsPerKBTable = getColumnPerDBPediaTable();

        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences = new ProcessableCollection<>();

        for (List<MatchableTableColumn> columnListWebTable : columnsPerWebTable.values()) {
            if (columnListWebTable.size() > 0) {
                int tableId = columnListWebTable.get(0).getTableId();
                Set<String> dbPediaClassesForTable = classesPerTable.get(tableId);
                for (String dbPediaClass : dbPediaClassesForTable) {
                    List<MatchableTableColumn> columnListKB = columnsPerKBTable.get(kb.getClassIds().get(dbPediaClass));
                    if (columnListKB != null && columnListKB.size() > 0) {
                        columnListKB.removeIf(x -> x.getIdentifier().equals("URI"));
                        SimilarityFloodingAlgorithm<MatchableTableColumn, MatchableTableRow> sf = new SimilarityFloodingAlgorithm<>(columnListWebTable, columnListKB,
                            new SFComparatorWebJaccard(oldToNew, tableToCorrespondenceMap, surfaceForms, kb));
                        sf.setRemoveOid(true);
                        sf.setMinSim(0.01);
                        sf.run();
                        correspondences.addAll(sf.getResult().get());
                    }
                }
            }
        }
        return correspondences;
    }

    private Map<Integer, List<MatchableTableColumn>> getColumnPerDBPediaTable() {
        // first invert the direct of class indices, such that we can obtain a table id given a class name
        Map<String, Integer> nameToId = MapUtils.invert(kb.getClassIndices());

        // first translate class names to table ids and convert the map into a list of pairs
        // no need to use DataProcessingEngine as both variables are local
        Processable<Pair<Integer, Integer>> tablePairs = new ProcessableCollection<>();
        for (Integer webTableId : classesPerTable.keySet()) {

            Set<String> classesForTable = classesPerTable.get(webTableId);

            for (String className : classesForTable) {
                Pair<Integer, Integer> p = new Pair<Integer, Integer>(webTableId, nameToId.get(className));
                tablePairs.add(p);
            }

        }

        final Map<Integer, Set<Integer>> classesPerColumnId = new HashMap<>();
        for (Integer tableId : kb.getPropertyIndices().keySet()) {

            // PropertyIndices maps a table id to a map of global property id -> local column index
            // here we are only interested in the global id
            Set<Integer> propertyIds = kb.getPropertyIndices().get(tableId).keySet();

            for (Integer columnId : propertyIds) {
                Set<Integer> tablesForColumnId = MapUtils.get(classesPerColumnId, columnId, new HashSet<Integer>());

                tablesForColumnId.add(tableId);
            }
        }

        //TODO the steps before this line should be done once in the driver program, so we don't have to transfer the knowledge base to the workers

        // now we join all web table columns with the just created pairs via the columns' table id and the first object of the pairs (which is the web table id)
        Function<Integer, MatchableTableColumn> tableColumnToTableId = new Function<Integer, MatchableTableColumn>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer execute(MatchableTableColumn input) {
                return input.getTableId();
            }
        };

        Function<Integer, Pair<Integer, Integer>> pairToFirstObject = new Function<Integer, Pair<Integer, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer execute(Pair<Integer, Integer> input) {
                return input.getFirst();
            }
        };

        // this join results in: <web table column, <web table id, dbpedia table id>>
        Processable<Pair<MatchableTableColumn, Pair<Integer, Integer>>> tableColumnsWithClassIds = web.getSchema().join(tablePairs, tableColumnToTableId, pairToFirstObject);

        // then we join the result with all dbpedia columns via the pairs' second object (which is the dbpedia table id) and the dbpedia columns' table id
        Function<Integer, Pair<MatchableTableColumn, Pair<Integer, Integer>>> tableColumnsWithClassIdsToClassId = new Function<Integer, Pair<MatchableTableColumn, Pair<Integer, Integer>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer execute(Pair<MatchableTableColumn, Pair<Integer, Integer>> input) {
                // input.getSecond() returns the pair that we created in the beginning
                // so that pair's second is the dbpedia table id
                return input.getSecond().getSecond();
            }
        };

        // for dbpedia columns we have to consider which properties exist for which class (a property can exist for multiple classes)
        // to make it work, we create pairs of <dbpedia table id, dbpedia column> for all tables where a property exists
        RecordMapper<MatchableTableColumn, Pair<Integer, MatchableTableColumn>> dbpediaColumnToTableIdMapper = new RecordMapper<MatchableTableColumn, Pair<Integer, MatchableTableColumn>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void mapRecord(MatchableTableColumn record,
                DataIterator<Pair<Integer, MatchableTableColumn>> resultCollector) {

                for (Integer tableId : classesPerColumnId.get(record.getColumnIndex())) {
                    Pair<Integer, MatchableTableColumn> tableWithColumn = new Pair<Integer, MatchableTableColumn>(tableId, record);

                    resultCollector.next(tableWithColumn);
                }

            }
        };

        Processable<Pair<Integer, MatchableTableColumn>> dbpediaColumnsForAllTables = kb.getSchema().map(dbpediaColumnToTableIdMapper);
        Map<Integer, List<Pair<Integer, MatchableTableColumn>>> kbSchema = dbpediaColumnsForAllTables.get().stream().collect(Collectors.groupingBy(Pair::getFirst));
        Map<Integer, List<MatchableTableColumn>> result = new HashMap<>();

        for (Entry<Integer, List<Pair<Integer, MatchableTableColumn>>> entry : kbSchema.entrySet()) {
            List<MatchableTableColumn> tmp = new ArrayList<>();
            for (Pair<Integer, MatchableTableColumn> pair : entry.getValue()) {
                MatchableTableColumn oldColumn = pair.getSecond();
                MatchableTableColumn newColumn = new MatchableTableColumn(entry.getKey(), kb.getPropertyIndices().get(entry.getKey()).get(oldColumn.getColumnIndex()), oldColumn.getHeader(),
                    oldColumn.getType(), oldColumn.getIdentifier());
                oldToNew.put(oldColumn, newColumn);
                tmp.add(oldColumn);
            }
            result.put(entry.getKey(), tmp);
        }

        return result;
    }
}
