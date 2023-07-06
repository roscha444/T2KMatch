package de.uni_mannheim.informatik.dws.t2k.match;

import au.com.bytecode.opencsv.CSVWriter;
import com.beust.jcommander.Parameter;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowDateComparator;
import de.uni_mannheim.informatik.dws.t2k.match.components.CandidateFiltering;
import de.uni_mannheim.informatik.dws.t2k.match.components.CandidateRefinement;
import de.uni_mannheim.informatik.dws.t2k.match.components.CandidateSelection;
import de.uni_mannheim.informatik.dws.t2k.match.components.ClassDecision;
import de.uni_mannheim.informatik.dws.t2k.match.components.ClassRefinement;
import de.uni_mannheim.informatik.dws.t2k.match.components.CombineSchemaCorrespondences;
import de.uni_mannheim.informatik.dws.t2k.match.components.DuplicateBasedSchemaMatching;
import de.uni_mannheim.informatik.dws.t2k.match.components.IdentityResolution;
import de.uni_mannheim.informatik.dws.t2k.match.components.LabelBasedSchemaMatching;
import de.uni_mannheim.informatik.dws.t2k.match.components.TableFiltering;
import de.uni_mannheim.informatik.dws.t2k.match.components.UpdateSchemaCorrespondences;
import de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.pipline.SimilarityFloodingPipeline;
import de.uni_mannheim.informatik.dws.t2k.match.components.similarityflooding.pipline.SimilarityFloodingPipelineComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.ExtractedTriple;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTable;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.t2k.match.rules.WebTableKeyToRdfsLabelCorrespondenceGenerator;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.DefaultIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.InMemoryIndex;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.Filter;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.FixpointFormula;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.model.io.CSVCorrespondenceFormatter;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.DataAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.CountAggregator;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.PercentageSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.BuildInfo;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.parallel.Parallel;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 
 * Executable class for the T2K Match algorithm.
 * 
 * See Ritze, D., Lehmberg, O., & Bizer, C. (2015, July). Matching html tables to dbpedia. In Proceedings of the 5th International Conference on Web Intelligence, Mining and Semantics (p. 10). ACM.
 * 
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class T2KMatch extends Executable implements Serializable {

    private static final long serialVersionUID = 1L;

    @Parameter(names = "-sf")
    private String sfLocation;

    @Parameter(names = "-kb", required=true)
    private String kbLocation;

    @Parameter(names = "-web", required=true)
    private String webLocation;

    @Parameter(names = "-identityGS")
    private String identityGSLocation;

    @Parameter(names = "-schemaGS")
    private String schemaGSLocation;

    @Parameter(names = "-classGS")
    private String classGSLocation;

    @Parameter(names = "-index")
    private String indexLocation;

    @Parameter(names = "-sparkMaster")
    private String sparkMaster;

    @Parameter(names = "-sparkJar")
    private String sparkJar;

    @Parameter(names = "-results", required=true)
    private String resultLocation;

    @Parameter(names = "-ontology", required=true)
    private String ontologyLocation;

    @Parameter(names = "-readGS")
    private String readGSLocation;

    @Parameter(names = "-writeGS")
    private String writeGSLocation;

    @Parameter(names = "-rd")
    private String rdLocation;

    @Parameter(names = "-verbose")
    private final boolean verbose = true;

    @Parameter(names = "-detectKeys")
    private boolean detectKeys;

    /*******
     * Parameters for algorithm configuration
     *******/
    @Parameter(names = "-mappedRatio")
    private final double par_mappedRatio=0.0;

    @Parameter(names = "-numIterations")
    private final int numIterations = 1;

    public static void main( String[] args ) throws Exception
    {
        T2KMatch t2k = new T2KMatch();

        if(t2k.parseCommandLine(T2KMatch.class, args)) {

            t2k.initialise();

            t2k.match();

        }
    }
    
    private IIndex index;
    private KnowledgeBase kb;
    private WebTables web;
    private MatchingGoldStandard instanceGs;
    private MatchingGoldStandard schemaGs;
    private MatchingGoldStandard classGs;
    private SurfaceForms sf;
    private File results;
    
    public void initialise() throws IOException {
        if(sfLocation==null && rdLocation==null){
            sf = new SurfaceForms(null, null);
        }else if(sfLocation==null && rdLocation!=null){
            sf = new SurfaceForms(null, new File(rdLocation));
        }else if(sfLocation!=null && rdLocation==null){
            sf = new SurfaceForms(new File(sfLocation), null);
        }else{
            sf = new SurfaceForms(new File(sfLocation), new File(rdLocation));
        }

        boolean createIndex = false;
        // create index for candidate lookup
        if(indexLocation==null) {
            // no index provided, create a new one in memory
            index = new InMemoryIndex();
            createIndex = true;
        } else{
            // load index from location that was provided
            index = new DefaultIndex(indexLocation);
            createIndex = !new File(indexLocation).exists();
        }
        if(createIndex) {
            sf.loadIfRequired();
        }

        //first load DBpedia class Hierarchy
        KnowledgeBase.loadClassHierarchy(ontologyLocation);

        // load knowledge base, fill index if it is empty
        kb = KnowledgeBase.loadKnowledgeBase(new File(kbLocation), createIndex?index:null, sf);

        // load instance gold standard
        if(identityGSLocation!=null) {
            File instGsFile = new File(identityGSLocation);
            if(instGsFile.exists()) {
                instanceGs = new MatchingGoldStandard();
                instanceGs.loadFromCSVFile(instGsFile);
                instanceGs.setComplete(true);
            }
        }

        // load schema gold standard
        if(schemaGSLocation!=null) {
            File schemaGsFile = new File(schemaGSLocation);
            if(schemaGsFile.exists()) {
                schemaGs = new MatchingGoldStandard();
                schemaGs.loadFromCSVFile(schemaGsFile);
                schemaGs.setComplete(true);
            }
        }

        // load class gold standard
        if(classGSLocation!=null) {
            File classGsFile = new File(classGSLocation);
            if(classGsFile.exists()) {
                classGs = new MatchingGoldStandard();
                classGs.loadFromCSVFile(classGsFile);
                classGs.setComplete(true);
            }
        }

        if(sparkJar==null) {
            sparkJar = BuildInfo.getJarPath(this.getClass()).getAbsolutePath();
        }

        results = new File(resultLocation);
        if(!results.exists()) {
            results.mkdirs();
        }
    }
    
    public void match() throws Exception {
        /***********************************************
         * Matching Framework Initialisation
         ***********************************************/
        // create matching engine
        MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine = new MatchingEngine<>();
        // disable stack-trace logging for long-running tasks
        Parallel.setReportIfStuck(false);

        web = WebTables.loadWebTables(new File(webLocation), false, true, detectKeys);

        /***********************************************
         * Gold Standard Adjustment
         ***********************************************/
        // remove all correspondences from the GS for tables that were not loaded
        if(instanceGs!=null) {
            instanceGs.removeNonexistingExamples(web.getRecords());
        }
        if(schemaGs!=null) {
            schemaGs.removeNonexistingExamples(web.getSchema());
        }
        if(classGs!=null) {
            classGs.removeNonexistingExamples(web.getTables());
        }

        /***********************************************
         * Key Preparation
         ***********************************************/
        // create schema correspondences between the key columns and rdfs:Label
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences = web.getKeys().map(new WebTableKeyToRdfsLabelCorrespondenceGenerator(kb.getRdfsLabel()));
        if(verbose) {
            for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : keyCorrespondences.get()) {
                System.out.printf("%s: [%d]%s%n", web.getTableNames().get(cor.getFirstRecord().getTableId()), cor.getFirstRecord().getColumnIndex(), cor.getFirstRecord().getHeader());
            }
        }

        /***********************************************
         * Candidate Selection
         ***********************************************/
        MatchingLogger.printHeader("Candidate Selection");
        CandidateSelection cs = new CandidateSelection(matchingEngine, sparkMaster!=null, index, indexLocation, web, kb, sf, keyCorrespondences);
        Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = cs.run();
        evaluateInstanceCorrespondences(instanceCorrespondences, "candidate");
        if(verbose) {
            printCandidateStatistics(instanceCorrespondences);
        }

        /***********************************************
         *Candidate Selection - Class Decision
         ***********************************************/
        MatchingLogger.printHeader("Candidate Selection - Class Decision");
        ClassDecision classDec = new ClassDecision();
        Map<Integer, Set<String>> classesPerTable = classDec.runClassDecision(kb, instanceCorrespondences, matchingEngine);
        evaluateClassCorrespondences(createClassCorrespondences(classesPerTable), "instance-based");

        /***********************************************
         *Candidate Selection - Candidate Refinement
         ***********************************************/
        MatchingLogger.printHeader("Candidate Selection - Candidate Refinement");
        CandidateRefinement cr = new CandidateRefinement(matchingEngine, sparkMaster!=null, index, indexLocation, web, kb, sf, keyCorrespondences, classesPerTable);
        instanceCorrespondences = cr.run();
        evaluateInstanceCorrespondences(instanceCorrespondences, "refined candidate");
        if(verbose) {
            printCandidateStatistics(instanceCorrespondences);
        }

        /***********************************************
         *Candidate Selection - Property-based Class Refinement
         ***********************************************/
        MatchingLogger.printHeader("Property-based Class Refinement");
        // match properties
        DuplicateBasedSchemaMatching schemaMatchingForClassRefinement = new DuplicateBasedSchemaMatching(matchingEngine, web, kb, sf, classesPerTable, instanceCorrespondences, false);
        schemaMatchingForClassRefinement.setFinalPropertySimilarityThreshold(0.03);
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = schemaMatchingForClassRefinement.run();
        // add key correspondences (some tables only have key correspondences)
        evaluateSchemaCorrespondences(schemaCorrespondences, "duplicate-based (refinement)");
        schemaCorrespondences = schemaCorrespondences.append(keyCorrespondences);
        // determine most probable class mapping
        ClassRefinement classRefinement = new ClassRefinement(kb.getPropertyIndices(), KnowledgeBase.getClassHierarchy(),schemaCorrespondences,classesPerTable, kb.getClassIds());
        classesPerTable = classRefinement.run();
        Map<Integer, String> finalClassPerTable = classRefinement.getFinalClassPerTable();
        evaluateClassCorrespondences(createClassCorrespondence(finalClassPerTable), "schema-based");

        /***********************************************
         *Candidate Selection - Class-based Filtering
         ***********************************************/
        CandidateFiltering classFilter = new CandidateFiltering(classesPerTable, kb.getClassIndices(), instanceCorrespondences);
        instanceCorrespondences = classFilter.run();
        evaluateInstanceCorrespondences(instanceCorrespondences, "property refined candidate");
        if (verbose) {
            printCandidateStatistics(instanceCorrespondences);
        }

        /***********************************************
         *Iterative Matching
         ***********************************************/
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> labelBasedSchemaCorrespondences = null;
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> lastSchemaCorrespondences = null;

        LabelBasedSchemaMatching labelBasedSchema = new LabelBasedSchemaMatching(matchingEngine, web, kb, classesPerTable, instanceCorrespondences);
        DuplicateBasedSchemaMatching duplicateBasedSchema = new DuplicateBasedSchemaMatching(matchingEngine, web, kb, sf, classesPerTable, instanceCorrespondences, false);
        CombineSchemaCorrespondences combineSchema = new CombineSchemaCorrespondences(keyCorrespondences);
        IdentityResolution identityResolution = new IdentityResolution(matchingEngine, web, kb, sf);
        UpdateSchemaCorrespondences updateSchema = new UpdateSchemaCorrespondences();

        int iteration = 0;
        do { // iterative matching loop
            /***********************************************
             * Schema Matching - Label Based
             ***********************************************/
            MatchingLogger.printHeader("Schema Matching - Label Based");
            labelBasedSchema.setInstanceCorrespondences(instanceCorrespondences);
            labelBasedSchemaCorrespondences = labelBasedSchema.run();
            evaluateSchemaCorrespondences(labelBasedSchemaCorrespondences, "label-based");

            /***********************************************
             * Schema Matching - Duplicate Based
             ***********************************************/
            MatchingLogger.printHeader("Schema Matching - Duplicate Based");
            duplicateBasedSchema.setInstanceCorrespondences(instanceCorrespondences);
            schemaCorrespondences = duplicateBasedSchema.run();
            evaluateSchemaCorrespondences(schemaCorrespondences, "duplicate-based");

            /***********************************************
             * Combine Schema Correspondences
             ***********************************************/
            MatchingLogger.printHeader("Combine Schema Correspondences (LabelBased <-> Schema Correspondences)");
            combineSchema.setSchemaCorrespondences(schemaCorrespondences);
            combineSchema.setLabelBasedSchemaCorrespondences(labelBasedSchemaCorrespondences);
            schemaCorrespondences = combineSchema.run();
            evaluateSchemaCorrespondences(schemaCorrespondences, "combined");

            /***********************************************
             * Iterative - Update Schema Correspondences
             ***********************************************/
            if (lastSchemaCorrespondences != null) {
                updateSchema.setSchemaCorrespondences(lastSchemaCorrespondences);
                updateSchema.setNewSchemaCorrespondences(schemaCorrespondences);
                schemaCorrespondences = updateSchema.run();
                evaluateSchemaCorrespondences(schemaCorrespondences, "updated");
            }

            /***********************************************
             * Identity Resolution
             ***********************************************/
            MatchingLogger.printHeader("Identity Resolution");
            identityResolution.setInstanceCorrespondences(instanceCorrespondences);
            identityResolution.setSchemaCorrespondences(schemaCorrespondences);
            instanceCorrespondences = identityResolution.run();
            evaluateInstanceCorrespondences(instanceCorrespondences, "final");
            if (verbose) {
                printCandidateStatistics(instanceCorrespondences);
            }

            lastSchemaCorrespondences = schemaCorrespondences;
        } while (++iteration < numIterations); // loop for iterative part

        /***********************************************
         * One-to-one Matching
         ***********************************************/

        Map<Integer, Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>>> schemaCorrespondenceMatrix = getSchemaCorrespondenceMatrix(schemaCorrespondences);
        instanceCorrespondences = matchingEngine.getTopKInstanceCorrespondences(instanceCorrespondences, 1, 0.0);
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondencesTopK = matchingEngine.getTopKSchemaCorrespondences(schemaCorrespondences, 1, 0.0);
        /***********************************************
         *Table Filtering - Mapped Ratio Filter
         ***********************************************/
        if (par_mappedRatio > 0.0) {
            TableFiltering tableFilter = new TableFiltering(web, instanceCorrespondences, classesPerTable, schemaCorrespondencesTopK);
            tableFilter.setMinMappedRatio(par_mappedRatio);
            tableFilter.run();
            classesPerTable = tableFilter.getClassesPerTable();
            instanceCorrespondences = tableFilter.getInstanceCorrespondences();
            schemaCorrespondencesTopK = tableFilter.getSchemaCorrespondences();
        }

        /***********************************************
         * Evaluation
         ***********************************************/
        System.out.println("==================================================");

        printMetaInformationen(classesPerTable, schemaCorrespondences);

        System.out.println("==================================================");

        System.out.println("T2K - Vanilla");
        printStatistics(classesPerTable, schemaCorrespondences);

        System.out.println("==================================================");

        System.out.println("T2K - TOPK");
        printStatistics(classesPerTable, schemaCorrespondencesTopK);

        System.out.println("==================================================");

        evaluateSchemaCorrespondences(schemaCorrespondencesTopK, "");

        System.out.println("==================================================");

        SimilarityFloodingPipelineComparator comparator = new SimilarityFloodingPipelineComparator(schemaCorrespondenceMatrix);

        System.out.println("==================================================");

        double minSim006 = 0.06;
        executeSimFlooding(FixpointFormula.A, minSim006, Filter.StableMarriage, classesPerTable, schemaCorrespondenceMatrix, comparator);
        executeSimFlooding(FixpointFormula.A, minSim006, Filter.TopOneK, classesPerTable, schemaCorrespondenceMatrix, comparator);
        executeSimFlooding(FixpointFormula.A, minSim006, Filter.HungarianAlgorithm, classesPerTable, schemaCorrespondenceMatrix, comparator);

        System.out.println("==================================================");

        double minSim004 = 0.04;
        executeSimFlooding(FixpointFormula.A, minSim004, Filter.StableMarriage, classesPerTable, schemaCorrespondenceMatrix, comparator);
        executeSimFlooding(FixpointFormula.A, minSim004, Filter.TopOneK, classesPerTable, schemaCorrespondenceMatrix, comparator);
        executeSimFlooding(FixpointFormula.A, minSim004, Filter.HungarianAlgorithm, classesPerTable, schemaCorrespondenceMatrix, comparator);

        System.out.println("==================================================");

        double minSim002 = 0.02;
        executeSimFlooding(FixpointFormula.A, minSim002, Filter.StableMarriage, classesPerTable, schemaCorrespondenceMatrix, comparator);
        executeSimFlooding(FixpointFormula.A, minSim002, Filter.TopOneK, classesPerTable, schemaCorrespondenceMatrix, comparator);
        executeSimFlooding(FixpointFormula.A, minSim002, Filter.HungarianAlgorithm, classesPerTable, schemaCorrespondenceMatrix, comparator);

        System.out.println("==================================================");

        evaluateInstanceCorrespondences(instanceCorrespondences, "");
        evaluateClassCorrespondences(createClassCorrespondence(finalClassPerTable), "");

        /***********************************************
         * Write Results
         ***********************************************/
        new CSVCorrespondenceFormatter().writeCSV(new File(results, "instance_correspondences.csv"), instanceCorrespondences);
        new CSVCorrespondenceFormatter().writeCSV(new File(results, "schema_correspondences.csv"), schemaCorrespondences);

        HashMap<Integer, String> inverseTableIndices = (HashMap<Integer, String>) MapUtils.invert(web.getTableIndices());
        CSVWriter csvWriter = new CSVWriter(new FileWriter(new File(results, "class_decision.csv")));
        for (Integer tableId : classesPerTable.keySet()) {
            csvWriter.writeNext(new String[]{tableId.toString(), inverseTableIndices.get(tableId), classesPerTable.get(tableId).toString().replaceAll("\\[", "").replaceAll("\\]", "")});
        }
        csvWriter.close();

        // generate triples from the matched tables and evaluate using Local-Closed-World Assumption (Note: no fusion happened so far, so values won't be too good...)
        TripleGenerator tripleGen = new TripleGenerator(web, kb);
        tripleGen.setComparatorForType(DataType.string,
            new MatchableTableRowComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5), kb.getPropertyIndices(), 0.5, sf, true));
        tripleGen.setComparatorForType(DataType.numeric, new MatchableTableRowComparator<>(new PercentageSimilarity(0.05), kb.getPropertyIndices(), 0.00));
        tripleGen.setComparatorForType(DataType.date, new MatchableTableRowDateComparator(new WeightedDateSimilarity(1, 3, 5), kb.getPropertyIndices(), 0.9));
        Processable<ExtractedTriple> triples = tripleGen.run(instanceCorrespondences, schemaCorrespondences);
        System.out.printf("Extracted %d existing (%.4f%% match values in KB) and %d new triples!%n", tripleGen.getExistingTripleCount(),
            tripleGen.getCorrectTripleCount() * 100.0 / (double) tripleGen.getExistingTripleCount(), tripleGen.getNewTripleCount());
        ExtractedTriple.writeCSV(new File(results, "extracted_triples.csv"), triples.get());

        //TODO add the correspondences to the tables and write them to the disk
    }

    private void printMetaInformationen(Map<Integer, Set<String>> classesPerTable, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
        System.out.println("META INFORMATIONEN");

        int tmp = 0;
        for (Entry<Integer, List<MatchableTableColumn>> entry : getColumnPerDBPediaTable(classesPerTable).entrySet()) {
            tmp += entry.getValue().size();
        }
        int averageKnowledgeHeaderLength = tmp / getColumnPerDBPediaTable(classesPerTable).size();

        tmp = 0;
        Map<Integer, List<MatchableTableColumn>> webTableTmp = web.getSchema().get().stream().collect(Collectors.groupingBy(MatchableTableColumn::getTableId));
        for (Entry<Integer, List<MatchableTableColumn>> entry : webTableTmp.entrySet()) {
            tmp += entry.getValue().size();
        }
        int averageWebTableHeaderLength = tmp / webTableTmp.size();

        int averageMatrixSize = calcAverageMatrix(classesPerTable, schemaCorrespondences.get().stream().collect(Collectors.groupingBy(x -> x.getFirstRecord().getTableId())));

        System.out.println("Avg KBTabellen Attribut Größe " + averageKnowledgeHeaderLength);
        System.out.println("Avg WebTabellen Attribut Größe " + averageWebTableHeaderLength);
        System.out.println("Avg Matrix Größe " + averageMatrixSize);
    }

    private void executeSimFlooding(FixpointFormula formula, Double minSim, Filter filter, Map<Integer, Set<String>> classesPerTable,
        Map<Integer, Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>>> schemaCorrespondenceMatrix,
        SimilarityFloodingPipelineComparator comparator) throws Exception {
        String runId = "SF " + formula.toString() + " " + minSim.toString() + " " + filter.toString();
        System.out.println(runId);
        SimilarityFloodingPipeline simFlooding = new SimilarityFloodingPipeline(web, kb, classesPerTable, schemaCorrespondenceMatrix, minSim, formula, comparator);
        simFlooding.setFilter(filter);
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> simFloodingResult = simFlooding.run();
        System.out.println("");
        System.out.println("Statistics after SF");
        simFlooding.getMatrixStatistics();
        evaluateSchemaCorrespondences(simFloodingResult, runId);
        System.out.println("");
        System.out.println("Statistics after " + filter.toString());
        printStatistics(classesPerTable, simFloodingResult);
    }

    private void printStatistics(Map<Integer, Set<String>> classesPerTable, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
        Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>> correspondencesByWebTable =
            schemaCorrespondences.get().stream().collect(Collectors.groupingBy(x -> x.getFirstRecord().getTableId()));
        int maxFields = correspondencesByWebTable.values().stream().max(Comparator.comparingInt(List::size)).orElse(new ArrayList<>()).size();
        int minFields = correspondencesByWebTable.values().stream().min(Comparator.comparingInt(List::size)).orElse(new ArrayList<>()).size();
        int sumCorrespondences = schemaCorrespondences.size();
        double avgFieldsInMatrix = (double) sumCorrespondences / correspondencesByWebTable.size();

        System.out.println("Anzahl Korrespondenzen " + sumCorrespondences);
        System.out.println("Max Felder in Matrix " + maxFields);
        System.out.println("Min Felder in Matrix " + minFields);
        System.out.println("Avg Felder in Matrix " + avgFieldsInMatrix);
        System.out.println();

        // TODO check this
        checkForClassCorrespondenceDuplicates(classesPerTable, correspondencesByWebTable);
    }

    private void checkForClassCorrespondenceDuplicates(Map<Integer, Set<String>> classesPerTable,
        Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>> correspondencesByWebTable) {
        for (Entry<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>> entry : correspondencesByWebTable.entrySet()) {
            List<Correspondence<MatchableTableColumn, MatchableTableRow>> list = entry.getValue();

            List<MatchableTableColumn> classCorrespondenceColumns = new ArrayList<>();
            String classCorrespodence = null;
            for (Correspondence<MatchableTableColumn, MatchableTableRow> corr : list) {
                outloop:
                for (String dbPediaClass : classesPerTable.get(entry.getKey())) {
                    List<MatchableTableColumn> kbTable = getColumnPerDBPediaTable(classesPerTable).get(kb.getClassIds().get(dbPediaClass));

                    if (kbTable == null) {
                        continue;
                    }

                    for (MatchableTableColumn kb : kbTable) {
                        if (classCorrespodence == null && kb.getIdentifier().equals(corr.getSecondRecord().getIdentifier())) {
                            classCorrespondenceColumns.add(kb);
                            classCorrespodence = dbPediaClass;
                            break outloop;
                        }
                        if (kb.getIdentifier().equals(corr.getSecondRecord().getIdentifier()) && classCorrespodence != null && classCorrespodence.equals(dbPediaClass)) {
                            break outloop;
                        }
                        if (kb.getIdentifier().equals(corr.getSecondRecord().getIdentifier()) && classCorrespodence != null && !classCorrespodence.equals(dbPediaClass)) {

                            List<MatchableTableColumn> tmp = getColumnPerDBPediaTable(classesPerTable).get(classCorrespodence);
                            if (tmp == null) {
                                break outloop;
                            }
                            boolean thisColExistsInClassCorr = tmp.stream().anyMatch(x -> x.getIdentifier().equals(kb.getIdentifier()));
                            if (thisColExistsInClassCorr) {
                                break outloop;
                            }

                            boolean containsAll = true;
                            for (MatchableTableColumn classCorresPondenceColumn : classCorrespondenceColumns) {
                                boolean contain = false;
                                for (MatchableTableColumn kb2 : kbTable) {
                                    if (classCorresPondenceColumn.equals(kb2)) {
                                        contain = true;
                                        break;
                                    }
                                }
                                if (!contain) {
                                    containsAll = false;
                                    break;
                                }
                            }

                            if (containsAll) {
                                classCorrespondenceColumns.add(kb);
                                classCorrespodence = dbPediaClass;
                                break outloop;
                            }

                            System.out.println("!!ES WURDEN KORRESPONDENZEN AUS ZWEI KLASSEN GEFUNDEN!! " + corr.getFirstRecord().getTableId());
                        }
                    }
                }
            }
        }
    }

    private int calcAverageMatrix(Map<Integer, Set<String>> classesPerTable,
        Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>> correspondencesByWebTable) {
        int avgMatrixSize = 0;
        int matrixCount = 0;
        Map<Integer, List<MatchableTableColumn>> webTableTmp = web.getSchema().get().stream().collect(Collectors.groupingBy(MatchableTableColumn::getTableId));
        for (Entry<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>> entry : correspondencesByWebTable.entrySet()) {
            List<Correspondence<MatchableTableColumn, MatchableTableRow>> list = entry.getValue();

            List<MatchableTableColumn> classCorrespondenceColumns = new ArrayList<>();
            String classCorrespodence = null;
            for (Correspondence<MatchableTableColumn, MatchableTableRow> corr : list) {
                outloop:
                for (String dbPediaClass : classesPerTable.get(entry.getKey())) {
                    List<MatchableTableColumn> kbTable = getColumnPerDBPediaTable(classesPerTable).get(kb.getClassIds().get(dbPediaClass));

                    if (kbTable == null) {
                        continue;
                    }

                    for (MatchableTableColumn kb : kbTable) {
                        if (classCorrespodence == null && kb.getIdentifier().equals(corr.getSecondRecord().getIdentifier())) {
                            classCorrespondenceColumns.add(kb);
                            classCorrespodence = dbPediaClass;
                            break outloop;
                        }
                        if (kb.getIdentifier().equals(corr.getSecondRecord().getIdentifier()) && classCorrespodence != null && classCorrespodence.equals(dbPediaClass)) {
                            break outloop;
                        }
                        if (kb.getIdentifier().equals(corr.getSecondRecord().getIdentifier()) && classCorrespodence != null && !classCorrespodence.equals(dbPediaClass)) {

                            List<MatchableTableColumn> tmp = getColumnPerDBPediaTable(classesPerTable).get(classCorrespodence);
                            if (tmp == null) {
                                break outloop;
                            }
                            boolean thisColExistsInClassCorr = tmp.stream().anyMatch(x -> x.getIdentifier().equals(kb.getIdentifier()));
                            if (thisColExistsInClassCorr) {
                                break outloop;
                            }

                            boolean containsAll = true;
                            for (MatchableTableColumn classCorresPondenceColumn : classCorrespondenceColumns) {
                                boolean contain = false;
                                for (MatchableTableColumn kb2 : kbTable) {
                                    if (classCorresPondenceColumn.equals(kb2)) {
                                        contain = true;
                                        break;
                                    }
                                }
                                if (!contain) {
                                    containsAll = false;
                                    break;
                                }
                            }

                            if (containsAll) {
                                classCorrespondenceColumns.add(kb);
                                classCorrespodence = dbPediaClass;
                                break outloop;
                            }
                        }
                    }
                }
            }
            List<MatchableTableColumn> kbTable = getColumnPerDBPediaTable(classesPerTable).get(kb.getClassIds().get(classCorrespodence));

            if (kbTable != null) {
                avgMatrixSize += (kbTable.size() * webTableTmp.get(entry.getKey()).size());
                matrixCount++;
            }
        }
        return avgMatrixSize / matrixCount;
    }

    protected Map<Integer, List<MatchableTableColumn>> getColumnPerDBPediaTable(Map<Integer, Set<String>> classesPerTable) {
        // first invert the direct of class indices, such that we can obtain a table id given a class name
        Map<String, Integer> nameToId = MapUtils.invert(kb.getClassIndices());

        // first translate class names to table ids and convert the map into a list of pairs
        // no need to use DataProcessingEngine as both variables are local
        Processable<Pair<Integer, Integer>> tablePairs = new ProcessableCollection<>();
        for (Integer webTableId : classesPerTable.keySet()) {

            Set<String> classesForTable = classesPerTable.get(webTableId);

            for (String className : classesForTable) {
                Pair<Integer, Integer> p = new Pair<Integer, Integer>(webTableId, nameToId.get(className));
                tablePairs.add(p);
            }

        }

        final Map<Integer, Set<Integer>> classesPerColumnId = new HashMap<>();
        for (Integer tableId : kb.getPropertyIndices().keySet()) {

            // PropertyIndices maps a table id to a map of global property id -> local column index
            // here we are only interested in the global id
            Set<Integer> propertyIds = kb.getPropertyIndices().get(tableId).keySet();

            for (Integer columnId : propertyIds) {
                Set<Integer> tablesForColumnId = MapUtils.get(classesPerColumnId, columnId, new HashSet<Integer>());

                tablesForColumnId.add(tableId);
            }
        }

        //TODO the steps before this line should be done once in the driver program, so we don't have to transfer the knowledge base to the workers

        // now we join all web table columns with the just created pairs via the columns' table id and the first object of the pairs (which is the web table id)
        Function<Integer, MatchableTableColumn> tableColumnToTableId = new Function<Integer, MatchableTableColumn>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer execute(MatchableTableColumn input) {
                return input.getTableId();
            }
        };

        Function<Integer, Pair<Integer, Integer>> pairToFirstObject = new Function<Integer, Pair<Integer, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer execute(Pair<Integer, Integer> input) {
                return input.getFirst();
            }
        };

        // this join results in: <web table column, <web table id, dbpedia table id>>
        Processable<Pair<MatchableTableColumn, Pair<Integer, Integer>>> tableColumnsWithClassIds = web.getSchema().join(tablePairs, tableColumnToTableId, pairToFirstObject);

        // then we join the result with all dbpedia columns via the pairs' second object (which is the dbpedia table id) and the dbpedia columns' table id
        Function<Integer, Pair<MatchableTableColumn, Pair<Integer, Integer>>> tableColumnsWithClassIdsToClassId = new Function<Integer, Pair<MatchableTableColumn, Pair<Integer, Integer>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer execute(Pair<MatchableTableColumn, Pair<Integer, Integer>> input) {
                // input.getSecond() returns the pair that we created in the beginning
                // so that pair's second is the dbpedia table id
                return input.getSecond().getSecond();
            }
        };

        // for dbpedia columns we have to consider which properties exist for which class (a property can exist for multiple classes)
        // to make it work, we create pairs of <dbpedia table id, dbpedia column> for all tables where a property exists
        RecordMapper<MatchableTableColumn, Pair<Integer, MatchableTableColumn>> dbpediaColumnToTableIdMapper = new RecordMapper<MatchableTableColumn, Pair<Integer, MatchableTableColumn>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void mapRecord(MatchableTableColumn record,
                DataIterator<Pair<Integer, MatchableTableColumn>> resultCollector) {

                for (Integer tableId : classesPerColumnId.get(record.getColumnIndex())) {
                    Pair<Integer, MatchableTableColumn> tableWithColumn = new Pair<Integer, MatchableTableColumn>(tableId, record);

                    resultCollector.next(tableWithColumn);
                }

            }
        };

        Processable<Pair<Integer, MatchableTableColumn>> dbpediaColumnsForAllTables = kb.getSchema().map(dbpediaColumnToTableIdMapper);
        Map<Integer, List<Pair<Integer, MatchableTableColumn>>> kbSchema = dbpediaColumnsForAllTables.get().stream().collect(Collectors.groupingBy(Pair::getFirst));
        Map<Integer, List<MatchableTableColumn>> result = new HashMap<>();

        for (Entry<Integer, List<Pair<Integer, MatchableTableColumn>>> entry : kbSchema.entrySet()) {
            List<MatchableTableColumn> tmp = new ArrayList<>();
            for (Pair<Integer, MatchableTableColumn> pair : entry.getValue()) {
                MatchableTableColumn oldColumn = pair.getSecond();
                tmp.add(oldColumn);
            }
            result.put(entry.getKey(), tmp);
        }

        return result;
    }

    private static Map<Integer, Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>>> getSchemaCorrespondenceMatrix(
        Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
        Map<Integer, Map<Integer, List<Correspondence<MatchableTableColumn, MatchableTableRow>>>> schemaCorrespondenceMatrix = new HashMap<>();

        for (Correspondence<MatchableTableColumn, MatchableTableRow> corr : schemaCorrespondences.get()) {
            int firstTableId = corr.getFirstRecord().getTableId();
            if (!schemaCorrespondenceMatrix.containsKey(firstTableId)) {
                schemaCorrespondenceMatrix.put(firstTableId, new HashMap<>());
            }

            int secondTableId = corr.getSecondRecord().getTableId();
            if (!schemaCorrespondenceMatrix.get(firstTableId).containsKey(secondTableId)) {
                schemaCorrespondenceMatrix.get(firstTableId).put(secondTableId, new ArrayList<>());
            }
            schemaCorrespondenceMatrix.get(firstTableId).get(secondTableId).add(corr);
        }
        return schemaCorrespondenceMatrix;
    }

    protected void evaluateInstanceCorrespondences(Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, String name) {
        Performance instancePerf = null;
        if (instanceGs != null) {
            instanceCorrespondences.distinct();
            MatchingEvaluator<MatchableTableRow, MatchableTableColumn> instanceEvaluator = new MatchingEvaluator<>();
            Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondencesCollection = instanceCorrespondences.get();
            System.out.printf("%d %s instance correspondences%n", instanceCorrespondencesCollection.size(), name);
            instancePerf = instanceEvaluator.evaluateMatching(instanceCorrespondencesCollection, instanceGs);
        }

        if(instancePerf!=null) {
            System.out
                .printf(
                "Instance Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f%n",
                    instancePerf.getPrecision(), instancePerf.getRecall(),
                    instancePerf.getF1());
        }
    }
    
    protected void evaluateSchemaCorrespondences(Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, String name) {
        Performance schemaPerf = null;
        if(schemaGs!=null) {
            schemaCorrespondences.distinct();
            MatchingEvaluator<MatchableTableColumn, MatchableTableRow> schemaEvaluator = new MatchingEvaluator<>();
            Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondencesCollection = schemaCorrespondences.get();
            System.out.printf("%d %s schema correspondences%n", schemaCorrespondencesCollection.size(), name);
            schemaPerf = schemaEvaluator.evaluateMatching(schemaCorrespondencesCollection, schemaGs);
        }

        if(schemaPerf!=null) {
            System.out
                .printf(
                "Schema Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f%n",
                    schemaPerf.getPrecision(), schemaPerf.getRecall(),
                    schemaPerf.getF1());
        }
    }
    
    protected void evaluateClassCorrespondences(Processable<Correspondence<MatchableTable, MatchableTableColumn>> classCorrespondences, String name) {
        Performance classPerf = null;
        if(classGs!=null) {
            classCorrespondences.distinct();
            MatchingEvaluator<MatchableTable, MatchableTableColumn> classEvaluator = new MatchingEvaluator<>();
            Collection<Correspondence<MatchableTable, MatchableTableColumn>> classCorrespondencesCollection = classCorrespondences.get();
            System.out.printf("%d %s class correspondences%n", classCorrespondencesCollection.size(), name);
            classPerf = classEvaluator.evaluateMatching(classCorrespondencesCollection, classGs);
        }

        if(classPerf!=null) {
            System.out
                .printf(
                "Class Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f%n",
                    classPerf.getPrecision(), classPerf.getRecall(),
                    classPerf.getF1());
        }

    }
    
    protected Processable<Correspondence<MatchableTable, MatchableTableColumn>> createClassCorrespondences(Map<Integer, Set<String>> classesPerTable) {
        //TODO the class matching should be replaced by actual matchers that create correspondences, such that we don't need this method
        Processable<Correspondence<MatchableTable, MatchableTableColumn>> result = new ProcessableCollection<>();

        for(int tableId : classesPerTable.keySet()) {

            MatchableTable webTable = web.getTables().getRecord(web.getTableNames().get(tableId));

            for(String className : classesPerTable.get(tableId)) {

                MatchableTable kbTable = kb.getTables().getRecord(className);

                Correspondence<MatchableTable, MatchableTableColumn> cor = new Correspondence<MatchableTable, MatchableTableColumn>(webTable, kbTable, 1.0, null);
                result.add(cor);
            }

        }

        return result;
    }
    protected Processable<Correspondence<MatchableTable, MatchableTableColumn>> createClassCorrespondence(Map<Integer, String> classPerTable) {
        //TODO the class matching should be replaced by actual matchers that create correspondences, such that we don't need this method
        Processable<Correspondence<MatchableTable, MatchableTableColumn>> result = new ProcessableCollection<>();

        for(int tableId : classPerTable.keySet()) {

            MatchableTable webTable = web.getTables().getRecord(web.getTableNames().get(tableId));

            String className = classPerTable.get(tableId);

            MatchableTable kbTable = kb.getTables().getRecord(className);

            Correspondence<MatchableTable, MatchableTableColumn> cor = new Correspondence<MatchableTable, MatchableTableColumn>(webTable, kbTable, 1.0, null);
            result.add(cor);

        }

        return result;
    }
    
    protected void printCandidateStatistics(Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {

        RecordKeyValueMapper<String, Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>> groupBy = new RecordKeyValueMapper<String, Correspondence<MatchableTableRow,MatchableTableColumn>, Correspondence<MatchableTableRow,MatchableTableColumn>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void mapRecordToKey(Correspondence<MatchableTableRow, MatchableTableColumn> record,
                DataIterator<Pair<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> resultCollector) {

                String tableName = web.getTableNames().get(record.getFirstRecord().getTableId());

                resultCollector.next(new Pair<String, Correspondence<MatchableTableRow,MatchableTableColumn>>(tableName, record));

            }
        };
        Processable<Pair<String, Integer>> counts = instanceCorrespondences.aggregate(groupBy, new CountAggregator<String, Correspondence<MatchableTableRow, MatchableTableColumn>>());

        // get class distribution
        DataAggregator<String, Correspondence<MatchableTableRow, MatchableTableColumn>, Map<String, Integer>> classAggregator = new DataAggregator<String, Correspondence<MatchableTableRow,MatchableTableColumn>, Map<String,Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Pair<Map<String, Integer>, Object> initialise(String keyValue) {
                return stateless(new HashMap<>());
            }

            @Override
            public Pair<Map<String, Integer>, Object> aggregate(Map<String, Integer> previousResult,
                Correspondence<MatchableTableRow, MatchableTableColumn> record, Object state) {

                String className = kb.getClassIndices().get(record.getSecondRecord().getTableId());

                Integer cnt = previousResult.get(className);
                if(cnt==null) {
                    cnt = 0;
                }

                previousResult.put(className, cnt+1);

                return new Pair<>(previousResult, state);
            }

            @Override
            public Pair<Map<String, Integer>, Object> merge(Pair<Map<String, Integer>, Object> pair, Pair<Map<String, Integer>, Object> pair1) {
                Map<String, Integer> resultMap = pair.getFirst();

                for (Entry<String, Integer> entry : pair1.getFirst().entrySet()) {
                    if(resultMap.containsKey(entry.getKey())) {
                        resultMap.put(entry.getKey(), resultMap.get(entry.getKey()) + entry.getValue());
                    } else {
                        resultMap.put(entry.getKey(), entry.getValue());
                    }
                }

                return stateless(resultMap);
            }
        };

        Processable<Pair<String, Map<String, Integer>>> classDistribution = instanceCorrespondences.aggregate(groupBy, classAggregator);
        final Map<String, Map<String, Integer>> classesByTable = Pair.toMap(classDistribution.get());

        System.out.println("Candidates per Table:");
        for(final Pair<String, Integer> p : counts.get()) {
            System.out.printf("\t%s\t%d%n", p.getFirst(), p.getSecond());

            Collection<Pair<String, Integer>> classCounts = Q.sort(Pair.fromMap(classesByTable.get(p.getFirst())), new Comparator<Pair<String, Integer>>() {

                @Override
                public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
                    return -Integer.compare(o1.getSecond(), o2.getSecond());
                }
            });

            System.out.printf("\t\t%s%n", StringUtils.join(Q.project(classCounts, new Func<String, Pair<String, Integer>>() {

                @Override
                public String invoke(Pair<String, Integer> in) {
                    return String.format("%s: %.4f%%", in.getFirst(), in.getSecond()*100.0/(double)p.getSecond());
                }
            }), ", "));
        }

    }
}
