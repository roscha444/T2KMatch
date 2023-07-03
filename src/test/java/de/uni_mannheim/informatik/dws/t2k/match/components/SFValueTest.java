package de.uni_mannheim.informatik.dws.t2k.match.components;

import de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.matcher.value.SFValueBasedMatchingKB2WB;
import de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.matcher.value.SFValueBasedMatchingWB2KB;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.t2k.match.rules.WebTableKeyToRdfsLabelCorrespondenceGenerator;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.DefaultIndex;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import junit.framework.TestCase;

/**
 * Test for the SF Value based matcher {@link SFValueBasedMatchingWB2KB} and {@link SFValueBasedMatchingKB2WB}
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SFValueTest extends TestCase {

    public void test_WB_2_KB() throws Exception {
        // prepare

        MatchingGoldStandard schemaGs = null;
        // load schema gold standard
        File schemaGsFile = new File("src/test/resources/sfValueTest/gs_property.csv");
        if (schemaGsFile.exists()) {
            schemaGs = new MatchingGoldStandard();
            schemaGs.loadFromCSVFile(schemaGsFile);
            schemaGs.setComplete(true);
        }

        WebTables.setDoSerialise(false);
        WebTables wb = WebTables.loadWebTables(new File("src/test/resources/sfValueTest/wb"), false, true, false);

        KnowledgeBase kb = new KnowledgeBase();
        SurfaceForms sf = new SurfaceForms(new File("src/test/resources/sfValueTest/surfaceforms.txt"),
            new File("src/test/resources/sfValueTest/redirects.txt"));
        sf.loadIfRequired();
        IIndex index = new DefaultIndex("src/test/resources/sfValueTest/index");
        KnowledgeBase.loadClassHierarchy("src/test/resources/sfValueTest/redirects.txt");
        assertNotNull(sf);
        kb = KnowledgeBase.loadKnowledgeBase(new File("src/test/resources/sfValueTest/kb"), index, sf);

        Map<Integer, Set<String>> classPerTable = new HashMap<>();
        classPerTable.put(0, new HashSet<>());
        classPerTable.get(0).add("Building");

        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences = wb.getKeys().map(new WebTableKeyToRdfsLabelCorrespondenceGenerator(kb.getRdfsLabel()));
        MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine = new MatchingEngine<>();
        CandidateSelection cs = new CandidateSelection(matchingEngine, false, index, "src/test/resources/sfValueTest/index", wb, kb, sf, keyCorrespondences);
        Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = cs.run();

        CandidateFiltering classFilter = new CandidateFiltering(classPerTable, kb.getClassIndices(), instanceCorrespondences);
        instanceCorrespondences = classFilter.run();

        SFValueBasedMatchingWB2KB sfValueBasedMatchingWB2KB = new SFValueBasedMatchingWB2KB(wb, kb, classPerTable, instanceCorrespondences);
        sfValueBasedMatchingWB2KB.setSurfaceForms(sf);
        sfValueBasedMatchingWB2KB.setInstanceCorrespondences(instanceCorrespondences);

        // execute
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> result = sfValueBasedMatchingWB2KB.run();

        // validate

        evaluateSchemaCorrespondences(schemaGs, result, "sf");

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<MatchableTableColumn, MatchableTableRow> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getHeader(), correspondence.getSecondRecord().getHeader());
        }

        assertEquals("floorCount", resultList.get("etagen"));
        assertEquals("location", resultList.get("stadt"));
        assertEquals("rdf-schema#label", resultList.get("gebäude"));
        assertEquals("openingDate", resultList.get("jahr"));
    }

    public void test_KB_2_WB() throws Exception {
        // prepare

        MatchingGoldStandard schemaGs = null;
        // load schema gold standard
        File schemaGsFile = new File("src/test/resources/sfValueTest/gs_property.csv");
        if (schemaGsFile.exists()) {
            schemaGs = new MatchingGoldStandard();
            schemaGs.loadFromCSVFile(schemaGsFile);
            schemaGs.setComplete(true);
        }

        WebTables.setDoSerialise(false);
        WebTables wb = WebTables.loadWebTables(new File("src/test/resources/sfValueTest/wb"), false, true, false);

        KnowledgeBase kb = new KnowledgeBase();
        SurfaceForms sf = new SurfaceForms(new File("src/test/resources/sfValueTest/surfaceforms.txt"),
            new File("src/test/resources/sfValueTest/redirects.txt"));
        sf.loadIfRequired();
        IIndex index = new DefaultIndex("src/test/resources/sfValueTest/index");
        KnowledgeBase.loadClassHierarchy("src/test/resources/sfValueTest/redirects.txt");
        assertNotNull(sf);
        kb = KnowledgeBase.loadKnowledgeBase(new File("src/test/resources/sfValueTest/kb"), index, sf);

        Map<Integer, Set<String>> classPerTable = new HashMap<>();
        classPerTable.put(0, new HashSet<>());
        classPerTable.get(0).add("Building");

        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences = wb.getKeys().map(new WebTableKeyToRdfsLabelCorrespondenceGenerator(kb.getRdfsLabel()));
        MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine = new MatchingEngine<>();
        CandidateSelection cs = new CandidateSelection(matchingEngine, false, index, "src/test/resources/sfValueTest/index", wb, kb, sf, keyCorrespondences);
        Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = cs.run();

        CandidateFiltering classFilter = new CandidateFiltering(classPerTable, kb.getClassIndices(), instanceCorrespondences);
        instanceCorrespondences = classFilter.run();

        SFValueBasedMatchingKB2WB sfValueBasedMatching = new SFValueBasedMatchingKB2WB(wb, kb, classPerTable, instanceCorrespondences);
        sfValueBasedMatching.setSurfaceForms(sf);
        sfValueBasedMatching.setInstanceCorrespondences(instanceCorrespondences);

        // execute
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> result = sfValueBasedMatching.run();

        // validate

        evaluateSchemaCorrespondences(schemaGs, result, "sf");

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<MatchableTableColumn, MatchableTableRow> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getHeader(), correspondence.getSecondRecord().getHeader());
        }

        assertEquals("floorCount", resultList.get("etagen"));
        assertEquals("rdf-schema#label", resultList.get("gebäude"));
        assertEquals("openingDate", resultList.get("jahr"));
        assertEquals("location", resultList.get("stadt"));
    }

    protected void evaluateSchemaCorrespondences(MatchingGoldStandard schemaGs, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, String name) {
        Performance schemaPerf = null;
        if (schemaGs != null) {
            schemaCorrespondences.distinct();
            MatchingEvaluator<MatchableTableColumn, MatchableTableRow> schemaEvaluator = new MatchingEvaluator<>();
            Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondencesCollection = schemaCorrespondences.get();
            System.out.printf("%d %s schema correspondences%n", schemaCorrespondencesCollection.size(), name);
            schemaPerf = schemaEvaluator.evaluateMatching(schemaCorrespondencesCollection, schemaGs);
        }

        if (schemaPerf != null) {
            System.out
                .printf(
                    "Schema Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f%n",
                    schemaPerf.getPrecision(), schemaPerf.getRecall(),
                    schemaPerf.getF1());
        }
    }
}
