package de.uni_mannheim.informatik.dws.t2k.match.components;

import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.DefaultIndex;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import junit.framework.TestCase;

/**
 * Test for the LabelBasedSimilarityFlooding
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SFLabelTest extends TestCase {

    public void test() throws Exception {
        // prepare
        WebTables.setDoSerialise(false);
        WebTables wb = WebTables.loadWebTables(new File("/Users/I523269/SAPDevelop/T2KMatch_2/src/test/resources/sfLabelTest/wb"), false, true, false);

        KnowledgeBase kb = new KnowledgeBase();
        SurfaceForms sf = new SurfaceForms(new File("/Users/I523269/SAPDevelop/T2KMatch_2/src/test/resources/sfLabelTest/surfaceforms.txt"), new File("src\\test\\resources\\redirect\\redirects"));
        sf.loadIfRequired();
        IIndex index = new DefaultIndex("src\\test\\resources\\sfTest\\index\\");
        KnowledgeBase.loadClassHierarchy("/Users/I523269/SAPDevelop/T2KMatch_2/src/test/resources/sfLabelTest/OntologyDBpedia");
        assertNotNull(sf);
        kb = KnowledgeBase.loadKnowledgeBase(new File("/Users/I523269/SAPDevelop/T2KMatch_2/src/test/resources/sfLabelTest/kb"), index, sf);
        System.out.println();

        Map<Integer, Set<String>> classPerTable = new HashMap<>();
        classPerTable.put(0, new HashSet<>());
        classPerTable.get(0).add("City");

        SFLabelBasedMatching sfValueBasedMatching = new SFLabelBasedMatching(wb, kb, classPerTable);

        // execute
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> result = sfValueBasedMatching.run();

        // validate
        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<MatchableTableColumn, MatchableTableRow> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getHeader(), correspondence.getSecondRecord().getHeader());
        }

        assertEquals("rdf-schema#label", resultList.get("city population"));
        assertEquals("country", resultList.get("country"));
        assertEquals("areaTotal", resultList.get("areatotal"));
        assertEquals("URI", resultList.get("city"));
    }

}
