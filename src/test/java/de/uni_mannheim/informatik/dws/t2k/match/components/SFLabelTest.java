package de.uni_mannheim.informatik.dws.t2k.match.components;

import de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.matcher.label.SFLabelBasedMatchingKB2WB;
import de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.matcher.label.SFLabelBasedMatchingWB2KB;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.DefaultIndex;
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
 * Test for the SF Value based matcher {@link SFLabelBasedMatchingKB2WB} and {@link SFLabelBasedMatchingWB2KB}
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SFLabelTest extends TestCase {

    public void test_WB_2_KB() throws Exception {
        // prepare

        MatchingGoldStandard schemaGs = null;
        // load schema gold standard
        File schemaGsFile = new File("src/test/resources/sfLabelTest/gs_property.csv");
        if (schemaGsFile.exists()) {
            schemaGs = new MatchingGoldStandard();
            schemaGs.loadFromCSVFile(schemaGsFile);
            schemaGs.setComplete(true);
        }

        WebTables.setDoSerialise(false);
        WebTables wb = WebTables.loadWebTables(new File("src/test/resources/sfLabelTest/wb"), false, true, false);

        KnowledgeBase kb = new KnowledgeBase();
        SurfaceForms sf = new SurfaceForms(new File("src/test/resources/sfLabelTest/surfaceforms.txt"), new File("src/test/resources/sfLabelTest/redirects.txt"));
        sf.loadIfRequired();
        IIndex index = new DefaultIndex("src/test/resources/sfLabelTest/index");
        KnowledgeBase.loadClassHierarchy("src/test/resources/sfLabelTest/OntologyDBpedia");
        assertNotNull(sf);
        kb = KnowledgeBase.loadKnowledgeBase(new File("src/test/resources/sfLabelTest/kb"), index, sf);

        Map<Integer, Set<String>> classPerTable = new HashMap<>();
        classPerTable.put(0, new HashSet<>());
        classPerTable.get(0).add("City");

        SFLabelBasedMatchingWB2KB sfValueBasedMatching = new SFLabelBasedMatchingWB2KB(wb, kb, classPerTable);

        // execute
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> result = sfValueBasedMatching.run();

        // validate

        evaluateSchemaCorrespondences(schemaGs, result, "sf");

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<MatchableTableColumn, MatchableTableRow> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getHeader(), correspondence.getSecondRecord().getHeader());
        }

        assertEquals("rdf-schema#label", resultList.get("city population"));
        assertEquals("country", resultList.get("country"));
        assertEquals("areaTotal", resultList.get("areatotal"));
        assertEquals("URI", resultList.get("city"));
    }

    public void test_KB_2_WB() throws Exception {
        // prepare

        MatchingGoldStandard schemaGs = null;
        // load schema gold standard
        File schemaGsFile = new File("src/test/resources/sfLabelTest/gs_property.csv");
        if (schemaGsFile.exists()) {
            schemaGs = new MatchingGoldStandard();
            schemaGs.loadFromCSVFile(schemaGsFile);
            schemaGs.setComplete(true);
        }

        WebTables.setDoSerialise(false);
        WebTables wb = WebTables.loadWebTables(new File("src/test/resources/sfLabelTest/wb"), false, true, false);

        KnowledgeBase kb = new KnowledgeBase();
        SurfaceForms sf = new SurfaceForms(new File("src/test/resources/sfLabelTest/surfaceforms.txt"), new File("src/test/resources/sfLabelTest/redirects.txt"));
        sf.loadIfRequired();
        IIndex index = new DefaultIndex("src/test/resources/sfLabelTest/index");
        KnowledgeBase.loadClassHierarchy("src/test/resources/sfLabelTest/OntologyDBpedia");
        assertNotNull(sf);
        kb = KnowledgeBase.loadKnowledgeBase(new File("src/test/resources/sfLabelTest/kb"), index, sf);

        Map<Integer, Set<String>> classPerTable = new HashMap<>();
        classPerTable.put(0, new HashSet<>());
        classPerTable.get(0).add("City");

        SFLabelBasedMatchingWB2KB sfValueBasedMatching = new SFLabelBasedMatchingWB2KB(wb, kb, classPerTable);

        // execute
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> result = sfValueBasedMatching.run();

        // validate

        evaluateSchemaCorrespondences(schemaGs, result, "sf");

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<MatchableTableColumn, MatchableTableRow> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getHeader(), correspondence.getSecondRecord().getHeader());
        }

        assertEquals("rdf-schema#label", resultList.get("city population"));
        assertEquals("country", resultList.get("country"));
        assertEquals("areaTotal", resultList.get("areatotal"));
        assertEquals("URI", resultList.get("city"));
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
