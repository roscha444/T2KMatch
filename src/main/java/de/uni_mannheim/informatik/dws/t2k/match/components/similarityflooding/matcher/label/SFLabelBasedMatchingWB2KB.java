package de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.matcher.label;

import de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.matcher.SimilarityFloodingMatching;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.SimilarityFloodingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.FixpointFormula;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Component that runs the label-based similarity flooding algorithm.
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SFLabelBasedMatchingWB2KB extends SimilarityFloodingMatching {

    private static final double MIN_SIM = 0.10;
    private static final boolean REMOVE_OID = true;
    private static final boolean USE_ALTERNATIVE_INC_FNC = true;

    public SFLabelBasedMatchingWB2KB(WebTables web, KnowledgeBase kb, Map<Integer, Set<String>> classesPerTable) {
        super(web, kb, classesPerTable);
    }

    public Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> run() throws Exception {
        Map<Integer, List<MatchableTableColumn>> columnsPerWebTable = getColumnPerWBTable();
        Map<Integer, List<MatchableTableColumn>> columnsPerKBTable = getColumnPerDBPediaTable();

        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCorrespondences = new ProcessableCollection<>();

        for (List<MatchableTableColumn> webTable : columnsPerWebTable.values()) {
            if (webTable.size() > 0) {
                int tableId = webTable.get(0).getTableId();
                Set<String> dbPediaClassesForTable = classesPerTable.get(tableId);
                for (String dbPediaClass : dbPediaClassesForTable) {
                    List<MatchableTableColumn> kbTable = columnsPerKBTable.get(kb.getClassIds().get(dbPediaClass));
                    if (kbTable != null && kbTable.size() > 0) {
                        kbTable.removeIf(x -> x.getIdentifier().equals("URI"));

                        SimilarityFloodingAlgorithm<MatchableTableColumn, MatchableTableRow> sfMatcher = new SimilarityFloodingAlgorithm<>(webTable, kbTable, new LabelComparator(), FixpointFormula.A);
                        sfMatcher.setRemoveOid(REMOVE_OID);
                        sfMatcher.setMinSim(MIN_SIM);
                        sfMatcher.run();

                        resultCorrespondences.addAll(sfMatcher.getResult().get());
                    }
                }
            }
        }
        return resultCorrespondences;
    }
}
