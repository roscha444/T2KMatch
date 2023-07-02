package de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.value.wb2kb;

import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.similarity.WebJaccardStringSimilarity;
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
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Comparator for value based SF, who uses a string comparator for all values
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 **/
public class ValueStringComparatorWB2KB implements Comparator<MatchableTableColumn, MatchableTableColumn> {

    private static final long serialVersionUID = 1L;
    private ComparatorLogger comparisonLog;

    private final Map<MatchableTableColumn, MatchableTableColumn> originalMatchableToAdaptedMatchable;
    private final SurfaceForms surfaceForms;
    private final KnowledgeBase kb;
    private final Map<Integer, Map<Integer, List<Correspondence<MatchableTableRow, MatchableTableColumn>>>> tableToCorrespondenceMap;

    // Similarities for string types
    private final WebJaccardStringSimilarity webJaccardStringSimilarity = new WebJaccardStringSimilarity();
    private final LevenshteinSimilarity levenshteinSimilarity = new LevenshteinSimilarity();

    // Similarities for specific data type
    private final SimilarityMeasure<String> stringSimilarity = new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.2, 0.2);
    private final SimilarityMeasure<Double> numericSimilarity = new DeviationSimilarity();
    private final WeightedDateSimilarity dateSimilarity = new WeightedDateSimilarity(1, 3, 5);

    public ValueStringComparatorWB2KB(Map<MatchableTableColumn, MatchableTableColumn> originalMatchableToAdaptedMatchable,
        Map<Integer, Map<Integer, List<Correspondence<MatchableTableRow, MatchableTableColumn>>>> tableToCorrespondenceMap, SurfaceForms surfaceForms, KnowledgeBase kb) {
        this.originalMatchableToAdaptedMatchable = originalMatchableToAdaptedMatchable;
        this.tableToCorrespondenceMap = tableToCorrespondenceMap;
        this.surfaceForms = surfaceForms;
        this.kb = kb;
    }

    @Override
    public double compare(MatchableTableColumn record1, MatchableTableColumn record2, Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {
        MatchableTableColumn secondRecord = originalMatchableToAdaptedMatchable.get(record2);

        double result = 0.0;
        int countResult = 0;

        if (tableToCorrespondenceMap.containsKey(record1.getTableId()) && tableToCorrespondenceMap.get(record1.getTableId()).containsKey(secondRecord.getTableId())) {
            surfaceForms.loadIfRequired();
            for (Correspondence<MatchableTableRow, MatchableTableColumn> corr : tableToCorrespondenceMap.get(record1.getTableId()).get(secondRecord.getTableId())) {

                int indexFirstRecord = record1.getColumnIndex();
                DataType typeFirstRecord = corr.getFirstRecord().getType(indexFirstRecord);

                int indexSecondRecord = record2.getColumnIndex();
                DataType typeSecondRecord = corr.getSecondRecord().getType(secondRecord.getColumnIndex());

                if (typeFirstRecord != null && typeSecondRecord != null) {

                    Map<Integer, Integer> indexTranslation = kb.getPropertyIndices().get(secondRecord.getTableId());
                    Integer translatedIndex = indexTranslation.get(indexSecondRecord);

                    String valueFirstRecord = corr.getFirstRecord().get(indexFirstRecord).toString();
                    String valueSecondRecord = corr.getSecondRecord().get(translatedIndex).toString();

                    double currRecordSim = Double.MIN_VALUE;
                    currRecordSim = calculateMaxSimFromSurface(valueFirstRecord, valueSecondRecord, currRecordSim);
                    currRecordSim = calculateMaxSimFromSurface(valueSecondRecord, valueFirstRecord, currRecordSim);
                    result += currRecordSim;
                    countResult++;
                }
            }
        }
        result = result / countResult;

        if (Double.isNaN(result)) {
            return Double.MIN_VALUE;
        }

        return result;
    }

    private double calculateMaxSimFromSurface(String value1, String value2, double sim) {
        List<String> surfaceFormsValue2 = new LinkedList<>();
        surfaceFormsValue2.add(value2);
        surfaceFormsValue2.addAll(surfaceForms.getSurfaceForms(WebTablesStringNormalizer.normaliseValue(value2, false)));

        for (String v2 : surfaceFormsValue2) {
            double s = levenshteinSimilarity.calculate(value1, v2);
            sim = Math.max(s, sim);
        }
        return sim;
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