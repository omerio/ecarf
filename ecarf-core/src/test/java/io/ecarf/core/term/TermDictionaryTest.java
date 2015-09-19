package io.ecarf.core.term;

import static org.junit.Assert.*;
import io.ecarf.core.compress.callback.ExtractTermsPartCallback;

import java.io.StringReader;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

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
            "<http://dbpedia.org/resource/Omerio> <http://www.w3.org/2000/01/rdf-schema#range> <http:///picasaweb.google.com> .\n" +
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

    @Before
    public void setUp() throws Exception {
        dictionary = TermDictionary.createRDFOWLDictionary();
        
        NxParser nxp = new NxParser(new StringReader(N_TRIPLES));
        
        int count = 0;
        
        ExtractTermsPartCallback callback = new ExtractTermsPartCallback();
        callback.setCounter(new TermCounter());

        while (nxp.hasNext())  {

            Node[] ns = nxp.next();

            //We are only interested in triples, no quads
            if (ns.length == 3) {

                callback.process(ns);
                count++;
            } 
        }
        
        // we have finished populate the dictionary
        System.out.println("Processed: " + count + " triples");
        assertEquals(22, count);
        assertEquals(4, callback.getLiteralCount());
        int numBlankNodes = callback.getBlankNodes().size();
        assertEquals(1, numBlankNodes);
        
        System.out.println("Total URI parts: " + callback.getResources().size());
        
        for(String part: callback.getResources()) {
            dictionary.add(part);
        }
        
    }

    @Test
    public void testEncodeDecode() {
        String term = "<http://dblp.uni-trier.de/rec/bibtex/books/acm/kim95/BreitbartGS95>";
        long id = dictionary.encode(term);
        
        System.out.println(id);
        
        String decterm = dictionary.decode(id);
        
        assertEquals(term, decterm);
    }


}
