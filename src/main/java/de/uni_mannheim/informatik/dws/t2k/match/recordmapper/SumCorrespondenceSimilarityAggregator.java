package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataAggregator;

/**
 * 
 * Sums up the similarity score of correspondences
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SumCorrespondenceSimilarityAggregator implements DataAggregator<String, Correspondence<MatchableTableColumn,MatchableTableRow>, Correspondence<MatchableTableColumn,MatchableTableRow>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Object> aggregate(
			Correspondence<MatchableTableColumn, MatchableTableRow> previousResult,
			Correspondence<MatchableTableColumn, MatchableTableRow> record,
            Object state) {
		if(previousResult == null)
			return new Pair<>(new Correspondence<MatchableTableColumn, MatchableTableRow>(record.getFirstRecord(), record.getSecondRecord(), record.getSimilarityScore(), record.getCausalCorrespondences()), state);
		else{
			previousResult.setsimilarityScore(record.getSimilarityScore() + previousResult.getSimilarityScore());
			return new Pair<>(previousResult, state);
		}
	}

    @Override
    public Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Object> merge(Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Object> pair,
        Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Object> pair1) {

        Object firstState = pair.getFirst();
        Correspondence<MatchableTableColumn, MatchableTableRow> firstCorrespondence = pair.getFirst();
        Correspondence<MatchableTableColumn, MatchableTableRow> secondCorrespondence = pair.getFirst();

        firstCorrespondence.setsimilarityScore( firstCorrespondence.getSimilarityScore() + secondCorrespondence.getSimilarityScore());
        return new Pair<>(firstCorrespondence, firstState);
    }

    @Override
	public Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Object> initialise(
			String keyValue) {
		return stateless(null);
	}

}
