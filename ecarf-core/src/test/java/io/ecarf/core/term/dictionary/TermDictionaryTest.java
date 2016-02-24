package io.ecarf.core.term.dictionary;

import static org.junit.Assert.assertEquals;
import io.ecarf.core.compress.callback.ExtractTerms2PartCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.FilenameUtils;
import io.ecarf.core.utils.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import com.google.common.base.Stopwatch;

public class TermDictionaryTest {

    private static final String N_TRIPLES = 
            "<http://dbpedia.org/resource/Andorra> <http://dbpedia.org/ontology/wikiPageExternalLink> <https://www.cia.gov/library/publications/world-leaders-1/world-leaders-a/andorra.html> .\n" +
                    "<http://dbpedia.org/resource/Agriculture> <http://dbpedia.org/ontology/wikiPageExternalLink> <http://www.agronomy.org/> .\n" +
                    "<http://dbpedia.org/resource/American_National_Standards_Institute> <http://dbpedia.org/ontology/wikiPageExternalLink> <http://iso14000.ansi.org/> .\n" +
                    "<http://dbpedia.org/resource/Albania> <http://dbpedia.org/ontology/wikiPageExternalLink> <http://www.cia.gov/library/publications/the-world-factbook/geos/al.html> .\n" +
                    "<http://dbpedia.org/resource/Akira_Kurosawa> <http://dbpedia.org/ontology/wikiPageExternalLink> <http://sites.google.com/site/illustratedjapanesevocabulary/film/kurosawa> .\n" +
                    "<http://dbpedia.org/resource/Demographics_of_Armenia> <http://dbpedia.org/ontology/wikiPageExternalLink> <http://www.cia.gov/library/publications/the-world-factbook/geos/am.html> .\n" +
                    "<http://dbpedia.org/resource/Economy_of_Armenia> <http://dbpedia.org/ontology/wikiPageExternalLink> <http://www.cia.gov/library/publications/the-world-factbook/geos/am.html> .\n" +
                    "<http://dbpedia.org/resource/Artificial_intelligence> <http://dbpedia.org/ontology/wikiPageExternalLink> <http://www.researchgate.net/group/Artificial_Intelligence> .\n" +
                    "<http://dbpedia.org/resource/Arminianism> <http://dbpedia.org/ontology/wikiPageExternalLink> <http://oa.doria.fi/handle/10024/43883?locale=len> .\n" +
                    "<http://dbpedia.org/resource/Alfred_Russel_Wallace> <http://dbpedia.org/ontology/wikiPageExternalLink> <https://picasaweb.google.com/WallaceMemorialFund> .\n" +
                    "<http://dbpedia.org/resource/Omerio> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://picasaweb.google.com> .\n" +
                    //"<http://dbpedia.org/resource/Omerio> <http://www.w3.org/2000/01/rdf-schema#range> <http:///picasaweb.google.com> .\n" +
                    "<http://dblp.uni-trier.de/rec/bibtex/books/acm/kim95/AnnevelinkACFHK95> <http://lsdis.cs.uga.edu/projects/semdis/opus#author> _:B54825b3X3A145000e6696X3AX2D7fff .\n" +
                    "_:B54825b3X3A145000e6696X3AX2D7fff <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> <http://www.informatik.uni-trier.de/~ley/db/indices/a-tree/a/Annevelink:Jurgen.html> .\n" +
                    "_:B54825b3X3A145000e6696X3AX2D7fff <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> <http://www.informatik.uni-trier.de/~ley/db/indices/a-tree/a/Ahad:Rafiul.html> .\n" +
                    "<http://dblp.uni-trier.de/rec/bibtex/books/acm/kim95/BreitbartGS95> <http://lsdis.cs.uga.edu/projects/semdis/opus#pages> \"573-591\" .\n" +
                    "<http://dblp.uni-trier.de/rec/bibtex/books/acm/kim95/BreitbartGS95> <http://lsdis.cs.uga.edu/projects/semdis/opus#book_title> \"Modern Database Systems\" .\n" +
                    "<http://dblp.uni-trier.de/rec/bibtex/books/acm/kim95/BreitbartGS95> <http://lsdis.cs.uga.edu/projects/semdis/opus#chapter_of> <http://dblp.uni-trier.de/rec/bibtex/books/acm/Kim95> .\n" +
                    "<http://dblp.uni-trier.de/rec/bibtex/books/acm/kim95/BreitbartGS95> <http://purl.org/dc/elements/1.1/relation> \"http://www.informatik.uni-trier.de/~ley/db/books/collections/kim95.html#BreitbartGS95\" .\n" +
                    "<http://dblp.uni-trier.de/rec/bibtex/books/acm/kim95/BreitbartGS95> <http://lsdis.cs.uga.edu/projects/semdis/opus#year> \"1995\"^^<http://www.w3.org/2001/XMLSchema#gYear> .\n" +
                    "<http://dblp.uni-trier.de/rec/bibtex/books/acm/kim95/BreitbartR95> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://lsdis.cs.uga.edu/projects/semdis/opus#Book_Chapter> .\n" +
                    "<http://dbpedia.org/resource/Aachen> <http://dbpedia.org/ontology/wikiPageExternalLink> <https://www.createspace.com/282950> .";


    private TermDictionary dictionary;

    private Set<String> allTerms = new HashSet<>();

    private Set<String> blankNodes = new HashSet<>();

    @Before
    public void setUp() throws Exception {
        dictionary = TermDictionaryGuava.populateRDFOWLData(new TermDictionaryCore());

        NxParser nxp = new NxParser(new StringReader(N_TRIPLES));

        int count = 0;

        ExtractTerms2PartCallback callback = new ExtractTerms2PartCallback();
        callback.setSplitLocation(-1);
        callback.setCounter(new TermCounter());

        while (nxp.hasNext())  {

            Node[] ns = nxp.next();

            //We are only interested in triples, no quads
            if (ns.length == 3) {

                callback.processNTriple(ns);
                count++;

                for (int i = 0; i < ns.length; i++)  {

                    // we are not going to unscape literals, these can contain new line and 
                    // unscaping those will slow down the bigquery load, unless offcourse we use JSON
                    // instead of CSV https://cloud.google.com/bigquery/preparing-data-for-bigquery
                    if(!((i == 2) && (ns[i] instanceof Literal))) {

                        //TODO if we are creating a dictionary why unscape this term anyway?
                        //term = NxUtil.unescape(nodes[i].toN3());
                        if(!(ns[i] instanceof BNode)) {

                            allTerms.add(ns[i].toN3());
                        } 
                    }
                }
            } 
        }

        // we have finished populate the dictionary
        //System.out.println("Processed: " + count + " triples");
        assertEquals(21, count);
        assertEquals(4, callback.getLiteralCount());
        int numBlankNodes = callback.getBlankNodes().size();
        assertEquals(1, numBlankNodes);

        //System.out.println("Total URI parts: " + callback.getResources().size());

        for(String part: callback.getResources()) {
            dictionary.add(part);
        }

        for(String bNode: callback.getBlankNodes()) {
            dictionary.add(bNode);
        }

        ((TermDictionaryCore) dictionary).inverse();

        this.blankNodes = callback.getBlankNodes();

    }

    @Test
    public void testEncodeDecode() {
        String term = "<http://dblp.uni-trier.de/rec/bibtex/books/acm/kim95/BreitbartGS95>";
        this.validateRoundTrip(term, false);

        term = "<http://www.agronomy.org/>";
        this.validateRoundTrip(term, false);

        term = "<https://www.createspace.com/282950>";
        this.validateRoundTrip(term, false);

        term = "<https://dblp.uni-trier.de/rec/bibtex/books/acm/kim95/BreitbartGS95/>";
        this.validateRoundTrip(term, false);

        term = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
        this.validateRoundTrip(term, false);

        term = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
        this.validateRoundTrip(term, false);

        for(String tm: allTerms) {
            this.validateRoundTrip(tm, false);
        }

        for(String tm: blankNodes) {
            this.validateRoundTrip(tm, true);
        }
    }

    @Test
    public void testSerialize() throws IOException {

        this.dictionary.toFile(FilenameUtils.getLocalFilePath("dictionary.kryo.gz"), true);
    }



    private void validateRoundTrip(String term, boolean blankNode) {

        long id; 

        if(blankNode) {
            id = dictionary.encodeBlankNode(term);
        } else {
            id = dictionary.encode(term);
        }


        //System.out.println(id + "       " + term);

        String decterm = dictionary.decode(id);

        assertEquals(term, decterm);
    }

    /**
     * Test decode the schema then check with original, assumes files are in
     * @throws IOException 
     * @throws FileNotFoundException 
     * @throws ClassNotFoundException 
     */
    private static void testSwetodblpSchemaDecode() throws FileNotFoundException, IOException, ClassNotFoundException {

        String path = "/Users/omerio/SkyDrive/PhD/Experiments/Dictionary/";

        String encSchema = path + "opus_august2007_closure_encoded.csv";

        String decSchema = path + "opus_august2007_closure_dec.nt";

        TermDictionaryCore dictionary = Utils.objectFromFile(path + "ecarf-evm-1_dictionary.kryo.gz", TermDictionaryCore.class, true, false);
        
        // inverse the dictionary
        dictionary.inverse();


        try (BufferedReader r = new BufferedReader(new FileReader(encSchema), Constants.GZIP_BUF_SIZE);
                PrintWriter writer = 
                        new PrintWriter(new FileOutputStream(decSchema))) {

            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(r);

            try {

                for (CSVRecord record : records) {

                    String subject = dictionary.decode(Long.parseLong(record.get(0)));
                    String predicate = dictionary.decode(Long.parseLong(record.get(1)));

                    String object = record.get(2);

                    if(StringUtils.isBlank(object)) {
                        // must be literal, escape it
                        object = record.get(3);
                        object = "\"" + object + "\"";

                    } else {
                        object = dictionary.decode(Long.parseLong(record.get(2)));
                    }

                    writer.println(new StringBuilder(subject).append(' ')
                            .append(predicate).append(' ')
                            .append(object).append(" .").toString());

                }
            } catch(Exception e) {
                e.printStackTrace();
                throw new IOException(e);
                //failedTriples++;
            }
        }
    }


    public static void main(String [] args) throws Exception {
        /*int count = 400;

        TermDictionaryTest test = new TermDictionaryTest();
        Stopwatch stopwatch = Stopwatch.createUnstarted();

        long total = 0;

        for(int i = 0; i < count; i++) {
            stopwatch.start();
            test.setUp();
            test.testEncodeDecode();

            total += stopwatch.elapsed(TimeUnit.MILLISECONDS);

            stopwatch.reset();
        }

        System.out.println("Total iterations: " + count);
        System.out.println("Total time: " + total);
        System.out.println("Average iteration time in milliseconds: " + (total / count));*/

        //TermDictionaryCore dictionary = Utils.objectFromFile("/Users/omerio/Downloads/cloudex-processor-1443542175611_dictionary.gz", TermDictionaryCore.class, true, false);
        //System.out.println(dictionary.size());
        /*System.out.println("Reading new file");
        Set<String> parts = Utils.objectFromFile("/tmp/raw_infobox_property_definitions_en.nt.gz.kryo.gz", HashSet.class, true, false);

        System.out.println("Reading old file");
        Set<String> parts1 = Utils.objectFromFile("/tmp/raw_infobox_property_definitions_en1.nt.gz.kryo.gz", HashSet.class, true, false);

        for(String part: parts) {
            if(!parts1.contains(part)) {
                System.out.println("String not found in old file: " + part);
            }
        }

        for(String part: parts1) {
            if(!parts.contains(part)) {
                System.out.println("String not found in new file: " + part);
            }
        }*/

        testSwetodblpSchemaDecode();

        //cloudex-processor-1456247330660_dictionary.gz
    }


}
