package de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding;

import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract class for similarity flooding matcher
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public abstract class SimilarityFloodingMatching {

    protected WebTables web;
    protected KnowledgeBase kb;
    protected Map<Integer, Set<String>> classesPerTable;
    protected Map<MatchableTableColumn, MatchableTableColumn> originalMatchableToAdaptedMatchable = new HashMap<>();

    public SimilarityFloodingMatching(WebTables web, KnowledgeBase kb, Map<Integer, Set<String>> classesPerTable) {
        this.web = web;
        this.kb = kb;
        this.classesPerTable = classesPerTable;
    }

    protected Map<Integer, List<MatchableTableColumn>> getColumnPerDBPediaTable() {
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
                originalMatchableToAdaptedMatchable.put(oldColumn, newColumn);
                tmp.add(oldColumn);
            }
            result.put(entry.getKey(), tmp);
        }

        return result;
    }

    protected Map<Integer, List<MatchableTableColumn>> getColumnPerWBTable() {
        return web.getSchema().get().stream().collect(Collectors.groupingBy(MatchableTableColumn::getTableId));
    }

    protected Map<Integer, Map<Integer, List<Correspondence<MatchableTableRow, MatchableTableColumn>>>> getTableToCorrespondenceMap(
        Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {
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
        return tableToCorrespondenceMap;
    }
}
