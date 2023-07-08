package de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.pipline;

import de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.matcher.SimilarityFloodingMatching;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.SimilarityFloodingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.Filter;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.FixpointFormula;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.IPGNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNodeType;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class SimilarityFloodingPipeline extends SimilarityFloodingMatching {

    private Comparator<MatchableTableColumn, MatchableTableColumn> comparator;
    private Map<Integer, Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>>> schemaCorrespondenceMatrix;
    private double minSim;
    private FixpointFormula fixpointFormula;
    private Filter filter = Filter.StableMarriage;
    private boolean removeOid = true;

    public SimilarityFloodingPipeline(WebTables web, KnowledgeBase kb, Map<Integer, Set<String>> classesPerTable,
        Map<Integer, Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>>> schemaCorrespondenceMatrix,
        double minSim, FixpointFormula fixpointFormula,
        Comparator<MatchableTableColumn, MatchableTableColumn> comparator) {
        super(web, kb, classesPerTable);
        this.comparator = comparator;
        this.schemaCorrespondenceMatrix = schemaCorrespondenceMatrix;
        this.minSim = minSim;
        this.fixpointFormula = fixpointFormula;
    }

    public Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> run() throws Exception {
        Map<Integer, List<MatchableTableColumn>> columnsPerWebTable = getColumnPerWBTable();
        Map<Integer, List<MatchableTableColumn>> columnsPerKBTable = getColumnPerDBPediaTable();

        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCorrespondences = new ProcessableCollection<>();

        for (Entry<Integer, Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>>> webTableKey : schemaCorrespondenceMatrix.entrySet()) {
            int webTableId = webTableKey.getKey();
            List<MatchableTableColumn> webTable = columnsPerWebTable.get(webTableId);

            for (Entry<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>> kbTableKey : webTableKey.getValue().entrySet()) {
                int kbTableId = kbTableKey.getKey();
                List<MatchableTableColumn> kbTable = columnsPerKBTable.get(kbTableId);

                kbTable.removeIf(x -> x.getIdentifier().equals("URI"));

                SimilarityFloodingAlgorithm<MatchableTableColumn, MatchableTableRow> sfMatcher = new SimilarityFloodingAlgorithm<>(webTable, kbTable, comparator, fixpointFormula);
                sfMatcher.setFilter(filter);
                sfMatcher.setMinSim(minSim);
                sfMatcher.setRemoveOid(removeOid);
                sfMatcher.run();
                recordStatistic(sfMatcher);

                Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> result = sfMatcher.getResult().get();

                for (Correspondence<MatchableTableColumn, MatchableTableRow> corr : result) {
                    ProcessableCollection<Correspondence<MatchableTableRow, Matchable>> causal = new ProcessableCollection<>();
                    MatchableTableRow firstCol = new MatchableTableRow(String.valueOf(kbTableId), new Object[0], kbTableId, new DataType[0]);
                    MatchableTableRow secondCol = new MatchableTableRow(String.valueOf(kbTableId), new Object[0], kbTableId, new DataType[0]);
                    causal.add(new Correspondence<>(firstCol, secondCol, 0));
                    corr.setCausalCorrespondences(causal);
                }

                resultCorrespondences.addAll(result);
            }
        }
        return resultCorrespondences;
    }

    private void recordStatistic(SimilarityFloodingAlgorithm<MatchableTableColumn, MatchableTableRow> sfMatcher) {

        // TODO (ask Alex which part of the matrix should be included in the statistics)
        List<IPGNode<MatchableTableColumn>> filteredSchemaCorrespondence = sfMatcher.getIpg().vertexSet().stream()
            .filter(
                x -> x.getCurrSim() > minSim
                    && x.getPairwiseConnectivityNode().getA().getMatchable() != null
                    && x.getPairwiseConnectivityNode().getB().getMatchable() != null
                    && (removeOid || x.getPairwiseConnectivityNode().getA().getType().equals(SFNodeType.LITERAL) && x.getPairwiseConnectivityNode().getB().getType().equals(SFNodeType.LITERAL)))
            .collect(Collectors.toList());

        countMatrices++;
        sumCorrespondences += filteredSchemaCorrespondence.size();
        maxFields = Math.max(maxFields, filteredSchemaCorrespondence.size());
        minFields = Math.min(minFields, filteredSchemaCorrespondence.size());
    }

    int countMatrices = 0;
    int sumCorrespondences = 0;
    int maxFields = Integer.MIN_VALUE;
    int minFields = Integer.MAX_VALUE;

    public void getMatrixStatistics() {
        double avgFieldsInMatrix = (double) sumCorrespondences / countMatrices;
        System.out.println("Anzahl Korrespondenzen " + sumCorrespondences);
        System.out.println("Max Felder in Matrix " + maxFields);
        System.out.println("Min Felder in Matrix " + minFields);
        System.out.println("Avg Felder in Matrix " + avgFieldsInMatrix);
        System.out.println();
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }
}
