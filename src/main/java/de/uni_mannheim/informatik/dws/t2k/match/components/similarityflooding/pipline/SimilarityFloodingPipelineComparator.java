package de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.pipline;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import java.util.List;
import java.util.Map;

public class SimilarityFloodingPipelineComparator implements Comparator<MatchableTableColumn, MatchableTableColumn> {

    Map<Integer, Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>>> schemaCorrespondenceMatrix;

    public SimilarityFloodingPipelineComparator(Map<Integer, Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>>> schemaCorrespondenceMatrix) {
        this.schemaCorrespondenceMatrix = schemaCorrespondenceMatrix;
    }

    @Override
    public double compare(MatchableTableColumn record1, MatchableTableColumn record2, Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {
        int firstTableId = record1.getTableId();
        if (!schemaCorrespondenceMatrix.containsKey(firstTableId) || schemaCorrespondenceMatrix.get(firstTableId).size() == 0) {
            return 0.0;
        }

        int secondTableId = record2.getTableId();
        if (!schemaCorrespondenceMatrix.get(firstTableId).containsKey(secondTableId)) {
            return 0.0;
        }

        for (Correspondence<MatchableTableColumn, MatchableTableRow> corr : schemaCorrespondenceMatrix.get(firstTableId).get(secondTableId)) {
            if (corr.getFirstRecord().getColumnIndex() == record1.getColumnIndex() && corr.getSecondRecord().getColumnIndex() == record2.getColumnIndex()) {
                return corr.getSimilarityScore();
            }
        }
        return 0.0;
    }
}
