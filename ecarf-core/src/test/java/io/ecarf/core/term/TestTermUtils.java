package io.ecarf.core.term;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestTermUtils {
    
    private static final Map<String, List<String>> URIS = new HashMap<>();
    
    static {
        
        List<String> temp = null;
        /*String uri = "<http://patft.uspto.gov/netacgi/nph-Parser?Sect1=PTO1&Sect2=HITOFF&d=PALL&p=1&u=/netahtml/PTO/srchnum.htm&r=1&f=G&l=50&s1=6348648.PN.&OS=PN/6348648&RS=PN/6348648/>";
        temp = Lists.newArrayList("patft.uspto.gov/netacgi", "nph-Parser?Sect1=PTO1&Sect2=HITOFF&d=PALL&p=1&u=/netahtml/PTO/srchnum.htm&r=1&f=G&l=50&s1=6348648.PN.&OS=PN/6348648&RS=PN/6348648");
        URIS.put(uri, temp);        

        uri = "<http://www.honda.lv/>";
        temp = Lists.newArrayList("www.honda.lv");
        URIS.put(uri, temp);   
            
        uri = "<http://gmail.com>";
        temp = Lists.newArrayList("gmail.com");
        URIS.put(uri, temp);   
            
        uri = "<http://gmail.com:8080/Test?id=test>";
        temp = Lists.newArrayList("gmail.com:8080", "Test?id=test");
        URIS.put(uri, temp);   
            
        uri = "<http://web.archive.org/web/20051031200142/http:/www.mkaz.com/ebeab/history/>";
        temp = Lists.newArrayList("web.archive.org/web/20051031200142", "http:/www.mkaz.com/ebeab/history");
        URIS.put(uri, temp);   
            
        uri = "<http://web.archive.org/web/200510312001421/?http:/www.mkaz.com/ebeab/history/>";
        temp = Lists.newArrayList("web.archive.org/web/200510312001421", "?http:/www.mkaz.com/ebeab/history");
        URIS.put(uri, temp);   
            
        uri = "<http://web.archive.org/web/20051031200142/http:/www.mkaz.com?id=ebeab/history/>";
        temp = Lists.newArrayList("web.archive.org/web/20051031200142", "http:/www.mkaz.com?id=ebeab/history");
        URIS.put(uri, temp);   
            
        uri = "<http://www.hel.fi/wps/portal/Helsinki_en/?WCM_GLOBAL_CONTEXT=/en/Helsinki/>";
        temp = Lists.newArrayList("www.hel.fi/wps/portal/Helsinki_en", "?WCM_GLOBAL_CONTEXT=/en/Helsinki");
        URIS.put(uri, temp);   
            
        uri = "<http://dbpedia.org/resource/Team_handball>";
        temp = Lists.newArrayList("dbpedia.org/resource", "Team_handball");
        URIS.put(uri, temp);   
            
        uri = "<http://dbpedia.org/ontology/wikiPageExternalLink>";
        temp = Lists.newArrayList("dbpedia.org/ontology", "wikiPageExternalLink");
        URIS.put(uri, temp);   
            
        uri = "<http://www.nfsa.gov.au/blog/2012/09/28/tasmanian-time-capsule/>";
        temp = Lists.newArrayList("www.nfsa.gov.au/blog/2012/09/28", "tasmanian-time-capsule");
        URIS.put(uri, temp);   
            
        uri = "<http://www.whereis.com/whereis/mapping/renderMapAddress.do?name=&streetNumber=&street=City%20Center&streetType=&suburb=Hobart&state=Tasmania&latitude=-42.881&longitude=147.3265&navId=$01006046X0OL9$&brandId=1&advertiserId=&requiredZoomLevel=3>";
        temp = Lists.newArrayList("www.whereis.com/whereis/mapping", "renderMapAddress.do?name=&streetNumber=&street=City%20Center&streetType=&suburb=Hobart&state=Tasmania&latitude=-42.881&longitude=147.3265&navId=$01006046X0OL9$&brandId=1&advertiserId=&requiredZoomLevel=3");
        URIS.put(uri, temp);   */
        
        String uri = "<http://www.Department12.University4000.edu/FullProfessor6>"; 
        URIS.put(uri, new ArrayList<String>());  
        uri = "<http://www.example.com/univ-bench.owl#teacherOf>";
        URIS.put(uri, new ArrayList<String>());  
        uri = "<http://www.Department12.University4000.edu/GraduateCourse10>";
        URIS.put(uri, new ArrayList<String>());  
        uri = "<http://www.Department12.University4000.edu/FullProfessor3/Publication0>";
        URIS.put(uri, new ArrayList<String>());  
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testSplitIntoTwo() {
        for(String key: URIS.keySet()) {
            List<String> parts = TermUtils.splitIntoTwo(key, true);
            
            System.out.println(key);
            for(String part: parts) {
                System.out.print(part + " -------- ");
                //assertTrue(URIS.get(key).contains(part));
            }
            System.out.println('\n');
        }
    }
    
    @Test
    public void testSplitIntoTwoFirstSlash() {
        System.out.println('\n');
        for(String key: URIS.keySet()) {
            System.out.println("\n--------------------------------\n"+key);
            List<String> parts = TermUtils.splitIntoTwo(key, true, 0);
            
            for(String part: parts) {
                System.out.print(part + " ");
                //assertTrue(URIS.get(key).contains(part));
            }
        }
        
    }
    
    
    @Test
    public void testSplitIntoTwoSecondSlash() {
        System.out.println('\n');
        for(String key: URIS.keySet()) {
            System.out.println("\n--------------------------------\n"+key);
            List<String> parts = TermUtils.splitIntoTwo(key, true, 1);
            
            for(String part: parts) {
                System.out.print(part + " ");
                //assertTrue(URIS.get(key).contains(part));
            }
        }
    }

}
