package de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.pipline;

import de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.matcher.SimilarityFloodingMatching;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.SimilarityFloodingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.FixpointFormula;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class SimilarityFloodingPipeline extends SimilarityFloodingMatching {

    private Comparator<MatchableTableColumn, MatchableTableColumn> comparator;
    private Map<Integer, Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>>> schemaCorrespondenceMatrix;
    private double minSim;
    private FixpointFormula fixpointFormula;

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

        for (Entry<Integer, Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>>> firstTable : schemaCorrespondenceMatrix.entrySet()) {
            int firstTableId = firstTable.getKey();

            for (Entry<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>> secondTable : firstTable.getValue().entrySet()) {
                int secondTableId = secondTable.getKey();

                List<MatchableTableColumn> webTable = columnsPerWebTable.get(firstTableId);
                List<MatchableTableColumn> kbTable = columnsPerKBTable.get(secondTableId);

                kbTable.removeIf(x -> x.getIdentifier().equals("URI"));
                SimilarityFloodingAlgorithm<MatchableTableColumn, MatchableTableRow> sfMatcher = new SimilarityFloodingAlgorithm<>(webTable, kbTable, comparator, fixpointFormula);
                sfMatcher.setMinSim(minSim);
                sfMatcher.setRemoveOid(true);
                sfMatcher.run();

                resultCorrespondences.addAll(sfMatcher.getResult().get());

            }
        }
        return resultCorrespondences;
    }

}
