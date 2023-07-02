package de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.value.kb2wb;

import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowDateComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.ComparatorLogger;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.DeviationSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import java.util.List;
import java.util.Map;

/**
 * Comparator for value based SF, who only compares the same values against each other
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 **/
public class ValueTypeComparatorKB2WB implements Comparator<MatchableTableColumn, MatchableTableColumn> {

    private static final long serialVersionUID = 1L;
    private ComparatorLogger comparisonLog;

    private final Map<MatchableTableColumn, MatchableTableColumn> originalMatchableToAdaptedMatchable;
    private final SurfaceForms surfaceForms;
    private final KnowledgeBase kb;
    private final Map<Integer, Map<Integer, List<Correspondence<MatchableTableRow, MatchableTableColumn>>>> tableToCorrespondenceMap;


    // Similarities for specific data type
    private final SimilarityMeasure<String> stringSimilarity = new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.2, 0.2);
    private final SimilarityMeasure<Double> numericSimilarity = new DeviationSimilarity();
    private final WeightedDateSimilarity dateSimilarity = new WeightedDateSimilarity(1, 3, 5);

    public ValueTypeComparatorKB2WB(Map<MatchableTableColumn, MatchableTableColumn> originalMatchableToAdaptedMatchable,
        Map<Integer, Map<Integer, List<Correspondence<MatchableTableRow, MatchableTableColumn>>>> tableToCorrespondenceMap, SurfaceForms surfaceForms, KnowledgeBase kb) {
        this.originalMatchableToAdaptedMatchable = originalMatchableToAdaptedMatchable;
        this.tableToCorrespondenceMap = tableToCorrespondenceMap;
        this.surfaceForms = surfaceForms;
        this.kb = kb;
    }

    @Override
    public double compare(MatchableTableColumn record1, MatchableTableColumn record2, Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {
        MatchableTableRowComparatorBasedOnSurfaceForms stringSurfaceComparator = new MatchableTableRowComparatorBasedOnSurfaceForms(stringSimilarity, kb.getPropertyIndices(), 0.2, surfaceForms);
        MatchableTableRowComparator doubleComparator = new MatchableTableRowComparator<>(numericSimilarity, kb.getPropertyIndices(), 0.2);
        MatchableTableRowDateComparator dateComparator = new MatchableTableRowDateComparator(dateSimilarity, kb.getPropertyIndices(), 0.2);

        MatchableTableColumn first = originalMatchableToAdaptedMatchable.get(record1);

        double result = 0.0;
        int countResult = 0;

        if (tableToCorrespondenceMap.containsKey(record2.getTableId()) && tableToCorrespondenceMap.get(record2.getTableId()).containsKey(first.getTableId())) {
            surfaceForms.loadIfRequired();
            for (Correspondence<MatchableTableRow, MatchableTableColumn> corr : tableToCorrespondenceMap.get(record2.getTableId()).get(first.getTableId())) {
                MatchableTableRow getSecondRecord = corr.getFirstRecord();
                MatchableTableRow getFirstRecord = corr.getSecondRecord();

                int indexSecondRecord = record2.getColumnIndex();
                DataType typeSecondRecord = getSecondRecord.getType(indexSecondRecord);

                int indexFirstRecord = record1.getColumnIndex();
                DataType typeFirstRecord = getFirstRecord.getType(first.getColumnIndex());

                if (typeSecondRecord != null && typeFirstRecord != null) {

                    if (typeSecondRecord.equals(typeFirstRecord)) {
                        countResult++;
                        if (typeSecondRecord.equals(DataType.string)) {
                            result += stringSurfaceComparator.compare(getSecondRecord, getFirstRecord, indexSecondRecord, indexFirstRecord);
                        } else if (typeSecondRecord.equals(DataType.numeric)) {
                            result += doubleComparator.compare(getSecondRecord, getFirstRecord, record2, record1);
                        } else if (typeSecondRecord.equals(DataType.date)) {
                            result += dateComparator.compare(getSecondRecord, getFirstRecord, record2, record1);
                        }
                    }
                }
            }
        }

        result = result / countResult;

        if (Double.isNaN(result)) {
            return Double.MIN_VALUE;
        }

        return result;
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