package de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.matcher.label;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.ComparatorLogger;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;

/**
 * String comparator for label based SimilarityFlooding
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class LabelComparator implements Comparator<MatchableTableColumn, MatchableTableColumn> {

    private static final long serialVersionUID = 1L;
    private ComparatorLogger comparisonLog;

    private final LevenshteinSimilarity levenshteinSimilarity = new LevenshteinSimilarity();
    private final GeneralisedStringJaccard generalisedStringJaccard = new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.2, 0.2);
    private final TokenizingJaccardSimilarity tokenizingJaccardSimilarity = new TokenizingJaccardSimilarity();

    @Override
    public double compare(MatchableTableColumn record1, MatchableTableColumn record2, Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {
        return levenshteinSimilarity.calculate(record1.getHeader(), record2.getHeader());
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
