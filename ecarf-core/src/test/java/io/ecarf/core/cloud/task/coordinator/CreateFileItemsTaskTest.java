/**
 * The contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2014, Ecarf.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.ecarf.core.cloud.task.coordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import io.cloudex.framework.partition.entities.Item;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;
import io.ecarf.core.triple.TriplesFileStats;
import io.ecarf.core.utils.FilenameUtils;
import io.ecarf.core.utils.TestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class CreateFileItemsTaskTest {
    
    private EcarfGoogleCloudServiceImpl service;
    
    Map<String, Long> triplesStat = new HashMap<>(); 
    
    Map<String, Double> triplesTimeStat = new HashMap<>();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        this.service = new EcarfGoogleCloudServiceImpl();
        TestUtils.prepare(service);
        
        this.updateMaps();
    }

    /**
     * Test method for {@link io.ecarf.core.cloud.task.coordinator.CreateFileItemsTask#run()}.
     * @throws IOException 
     */
    @Test
    public void testRun() throws IOException {
        CreateFileItemsTask task = new CreateFileItemsTask();
        task.setCloudService(service);
        task.setBucket("dbpedia");
        
        task.run();
        
        Map<String, Object> output = task.getOutput();
        
        
        
        assertNotNull(output);
        assertEquals(1, output.size());
        
        List<Item> items = (List<Item>) output.get("fileItems");
        
        assertEquals(items.size(), this.triplesStat.size());
        assertEquals(items.size(), this.triplesTimeStat.size());
        
        for(Item item: items) {
            System.out.println("Filename: " + item.getKey());
            System.out.println("Weight: " + item.getWeight() + "\n");
            
            Long statements = this.triplesStat.get(item.getKey());
            Double time = this.triplesTimeStat.get(item.getKey());
            
            assertNotNull(statements);
            
            //assertEquals(statements, item.getWeight());
            assertEquals(time, item.getWeight(), 0.9);
            
        }
    }
    
    @Test
    public void testCreateTriplesFilesStats() throws IOException {
          
        List<TriplesFileStats> stats = new ArrayList<>();
        
        for(Entry<String, Long> entry: triplesStat.entrySet()) {
            TriplesFileStats stat = new TriplesFileStats();
            stat.setFilename(entry.getKey());
            stat.setStatements(entry.getValue());
            stat.setProcessingTime(triplesTimeStat.get(entry.getKey()));
            stats.add(stat);
        }
        
        String filename = FilenameUtils.getLocalFilePath(FilenameUtils.TRIPLES_FILES_STATS_JSON);
        
        TriplesFileStats.toJsonFile(filename, stats, false);
        
        System.out.println("Successfully created: " + filename);
    }
    
    private void updateMaps() {
        triplesStat.put("raw_infobox_properties1_en.nt.gz",              35099568L);
        triplesStat.put("raw_infobox_properties2_en.nt.gz",              35099567L);
        triplesStat.put("wikipedia_links_en.nt.gz",                      30424452L);
        triplesStat.put("mappingbased_properties_cleaned_en.nt.gz",      25896865L);
        triplesStat.put("yago_types1.nt.gz"                           ,  20595070L);
        triplesStat.put("yago_types2.nt.gz"                           ,  20595068L);
        triplesStat.put("article_categories_en.nt.gz"                 ,  16599811L);
        triplesStat.put("instance_types_en.nt.gz"                     ,  15893902L);
        triplesStat.put("page_ids_en.nt.gz"                           ,  12461351L);
        triplesStat.put("revision_ids_en.nt.gz"                       ,  12461351L);
        triplesStat.put("revision_uris_en.nt.gz"                      ,  12461335L);
        triplesStat.put("interlanguage_links_chapters_en.nt.gz"       ,  12297519L);
        triplesStat.put("labels_en.nt.gz"                             ,  10141499L);
        triplesStat.put("images_en.nt.gz"                             ,  8005475L);
        triplesStat.put("external_links_en.nt.gz"                     ,  6950895L);
        triplesStat.put("persondata_en.nt.gz"                         ,  6670069L);
        triplesStat.put("redirects_transitive_en.nt.gz"               ,  5975754L);
        triplesStat.put("flickrwrappr_links.nt.gz"                    ,  4041585L);
        triplesStat.put("skos_categories_en.nt.gz"                    ,  4012746L);
        triplesStat.put("long_abstracts_en.nt.gz"                     ,  4004478L);
        triplesStat.put("short_abstracts_en.nt.gz"                    ,  4004478L);
        triplesStat.put("freebase_links.nt.gz"                        ,  3873430L);
        triplesStat.put("instance_types_heuristic_en.nt.gz"           ,  3402114L);
        triplesStat.put("yago_links.nt.gz"                            ,  2886306L);
        triplesStat.put("geo_coordinates_en.nt.gz"                    ,  1987960L);
        triplesStat.put("disambiguations_en.nt.gz"                    ,  1246884L);
        triplesStat.put("category_labels_en.nt.gz"                    ,  995911L);
        triplesStat.put("umbel_links.nt.gz"                           ,  896423L);
        triplesStat.put("iri_same_as_uri_en.nt.gz"                    ,  783373L);
        triplesStat.put("specific_mappingbased_properties_en.nt.gz"   ,  706186L);
        triplesStat.put("homepages_en.nt.gz"                          ,  500282L);
        triplesStat.put("wordnet_links.nt.gz"                         ,  467101L);
        triplesStat.put("yago_taxonomy.nt.gz"                         ,  455028L);
        triplesStat.put("geonames_links.nt.gz"                        ,  424739L);
        triplesStat.put("linkedgeodata_links.nt.gz"                   ,  103633L);
        triplesStat.put("raw_infobox_property_definitions_en.nt.gz"   ,  103472L);
        triplesStat.put("gadm_links.nt.gz"                            ,  38793L);
        triplesStat.put("opencyc_links.nt.gz"                         ,  27107L);
        triplesStat.put("musicbrainz_links.nt.gz"                     ,  22981L);
        triplesStat.put("geospecies_links.nt.gz"                      ,  15974L);
        triplesStat.put("linkedmdb_links.nt.gz"                       ,  13758L);
        triplesStat.put("uscensus_links.nt.gz"                        ,  12592L);
        triplesStat.put("eunis_links.nt.gz"                           ,  11235L);
        triplesStat.put("bricklink_links.nt.gz"                       ,  10090L);
        triplesStat.put("nytimes_links.nt.gz"                         ,  9678L);
        triplesStat.put("bookmashup_links.nt.gz"                      ,  9078L);
        triplesStat.put("wikicompany_links.nt.gz"                     ,  8348L);
        triplesStat.put("italian_public_schools_links.nt.gz"          ,  5822L);
        triplesStat.put("drugbank_links.nt.gz"                        ,  4845L);
        triplesStat.put("gutenberg_links.nt.gz"                       ,  2510L);
        triplesStat.put("diseasome_links.nt.gz"                       ,  2301L);
        triplesStat.put("sider_links.nt.gz"                           ,  1969L);
        triplesStat.put("tcm_links.nt.gz"                             ,  904L);
        triplesStat.put("dailymed_links.nt.gz"                        ,  894L);
        triplesStat.put("dbtune_links.nt.gz"                          ,  838L);
        triplesStat.put("openei_links.nt.gz"                          ,  678L);
        triplesStat.put("amsterdammuseum_links.nt.gz"                 ,  627L);
        triplesStat.put("factbook_links.nt.gz"                        ,  545L);
        triplesStat.put("bbcwildlife_links.nt.gz"                     ,  444L);
        triplesStat.put("cordis_links.nt.gz"                          ,  314L);
        triplesStat.put("eurostat_linkedstatistics_links.nt.gz"       ,  253L);
        triplesStat.put("dblp_links.nt.gz"                            ,  196L);
        triplesStat.put("gho_links.nt.gz"                             ,  196L);
        triplesStat.put("eurostat_wbsg_links.nt.gz"                   ,  137L);
        triplesStat.put("pnd_en.nt.gz"                                ,  42L);
        triplesStat.put("revyu_links.nt.gz"                           ,  6L);
        
        // add the time
        
        triplesTimeStat.put("raw_infobox_properties1_en.nt.gz", 131220.0);
        triplesTimeStat.put("raw_infobox_properties2_en.nt.gz", 208560.0);
        triplesTimeStat.put("wikipedia_links_en.nt.gz", 156840.0);
        triplesTimeStat.put("mappingbased_properties_cleaned_en.nt.gz", 185700.0);
        triplesTimeStat.put("yago_types1.nt.gz", 103320.0);
        triplesTimeStat.put("yago_types2.nt.gz", 127500.0);
        triplesTimeStat.put("article_categories_en.nt.gz", 127200.0);
        triplesTimeStat.put("instance_types_en.nt.gz", 91020.0);
        triplesTimeStat.put("page_ids_en.nt.gz", 134520.0);
        triplesTimeStat.put("revision_ids_en.nt.gz", 158520.0);
        triplesTimeStat.put("revision_uris_en.nt.gz", 284700.0);
        triplesTimeStat.put("interlanguage_links_chapters_en.nt.gz", 136620.0);
        triplesTimeStat.put("labels_en.nt.gz", 95040.0);
        triplesTimeStat.put("images_en.nt.gz", 94980.0);
        triplesTimeStat.put("external_links_en.nt.gz", 124980.0);
        triplesTimeStat.put("persondata_en.nt.gz", 26710.0);
        triplesTimeStat.put("redirects_transitive_en.nt.gz", 108360.0);
        triplesTimeStat.put("flickrwrappr_links.nt.gz", 55640.0);
        triplesTimeStat.put("skos_categories_en.nt.gz", 23990.0);
        triplesTimeStat.put("long_abstracts_en.nt.gz", 90840.0);
        triplesTimeStat.put("short_abstracts_en.nt.gz", 62280.0);
        triplesTimeStat.put("freebase_links.nt.gz", 68280.0);
        triplesTimeStat.put("instance_types_heuristic_en.nt.gz", 15200.0);
        triplesTimeStat.put("yago_links.nt.gz", 37880.0);
        triplesTimeStat.put("geo_coordinates_en.nt.gz", 8938.0);
        triplesTimeStat.put("disambiguations_en.nt.gz", 12180.0);
        triplesTimeStat.put("category_labels_en.nt.gz", 7839.0);
        triplesTimeStat.put("umbel_links.nt.gz", 6167.0);
        triplesTimeStat.put("iri_same_as_uri_en.nt.gz", 13600.0);
        triplesTimeStat.put("specific_mappingbased_properties_en.nt.gz", 6166.0);
        triplesTimeStat.put("homepages_en.nt.gz", 7843.0);
        triplesTimeStat.put("wordnet_links.nt.gz", 4289.0);
        triplesTimeStat.put("yago_taxonomy.nt.gz", 8831.0);
        triplesTimeStat.put("geonames_links.nt.gz", 5974.0);
        triplesTimeStat.put("linkedgeodata_links.nt.gz", 1546.0);
        triplesTimeStat.put("raw_infobox_property_definitions_en.nt.gz", 932.3);
        triplesTimeStat.put("gadm_links.nt.gz", 632.0);
        triplesTimeStat.put("opencyc_links.nt.gz", 746.0);
        triplesTimeStat.put("musicbrainz_links.nt.gz", 836.9);
        triplesTimeStat.put("geospecies_links.nt.gz", 578.3);
        triplesTimeStat.put("linkedmdb_links.nt.gz", 541.1);
        triplesTimeStat.put("uscensus_links.nt.gz", 965.8);
        triplesTimeStat.put("eunis_links.nt.gz", 1329.0);
        triplesTimeStat.put("bricklink_links.nt.gz", 313.8);
        triplesTimeStat.put("nytimes_links.nt.gz", 650.7);
        triplesTimeStat.put("bookmashup_links.nt.gz", 1131.0);
        triplesTimeStat.put("wikicompany_links.nt.gz", 924.5);
        triplesTimeStat.put("italian_public_schools_links.nt.gz", 436.1);
        triplesTimeStat.put("drugbank_links.nt.gz", 391.3);
        triplesTimeStat.put("gutenberg_links.nt.gz", 484.7);
        triplesTimeStat.put("diseasome_links.nt.gz", 602.6);
        triplesTimeStat.put("sider_links.nt.gz", 412.7);
        triplesTimeStat.put("tcm_links.nt.gz", 279.6);
        triplesTimeStat.put("dailymed_links.nt.gz", 309.1);
        triplesTimeStat.put("dbtune_links.nt.gz", 277.4);
        triplesTimeStat.put("openei_links.nt.gz", 409.2);
        triplesTimeStat.put("amsterdammuseum_links.nt.gz", 4592.0);
        triplesTimeStat.put("factbook_links.nt.gz", 297.1);
        triplesTimeStat.put("bbcwildlife_links.nt.gz", 308.8);
        triplesTimeStat.put("cordis_links.nt.gz", 4154.0);
        triplesTimeStat.put("eurostat_linkedstatistics_links.nt.gz", 305.4);
        triplesTimeStat.put("dblp_links.nt.gz", 315.9);
        triplesTimeStat.put("gho_links.nt.gz", 358.9);
        triplesTimeStat.put("eurostat_wbsg_links.nt.gz", 859.0);
        triplesTimeStat.put("pnd_en.nt.gz", 280.6);
        triplesTimeStat.put("revyu_links.nt.gz", 278.3);
        
       /* triplesTimeStat.put("raw_infobox_properties1_en.nt.gz", 116520.0);
        triplesTimeStat.put("raw_infobox_properties2_en.nt.gz", 192240.0);
        triplesTimeStat.put("wikipedia_links_en.nt.gz", 123960.0);
        triplesTimeStat.put("mappingbased_properties_cleaned_en.nt.gz", 163500.0);
        triplesTimeStat.put("yago_types1.nt.gz", 87540.0);
        triplesTimeStat.put("yago_types2.nt.gz", 116760.0);
        triplesTimeStat.put("article_categories_en.nt.gz", 104880.0);
        triplesTimeStat.put("instance_types_en.nt.gz", 77940.0);
        triplesTimeStat.put("page_ids_en.nt.gz", 72900.0);
        triplesTimeStat.put("revision_ids_en.nt.gz", 99420.0);
        triplesTimeStat.put("revision_uris_en.nt.gz", 128100.0);
        triplesTimeStat.put("interlanguage_links_chapters_en.nt.gz", 69240.0);
        triplesTimeStat.put("labels_en.nt.gz", 51610.0);
        triplesTimeStat.put("images_en.nt.gz", 57270.0);
        triplesTimeStat.put("external_links_en.nt.gz", 85020.0);
        triplesTimeStat.put("persondata_en.nt.gz", 21100.0);
        triplesTimeStat.put("redirects_transitive_en.nt.gz", 49490.0);
        triplesTimeStat.put("flickrwrappr_links.nt.gz", 27620.0);
        triplesTimeStat.put("skos_categories_en.nt.gz", 18660.0);
        triplesTimeStat.put("long_abstracts_en.nt.gz", 53860.0);
        triplesTimeStat.put("short_abstracts_en.nt.gz", 33230.0);
        triplesTimeStat.put("freebase_links.nt.gz", 33130.0);
        triplesTimeStat.put("instance_types_heuristic_en.nt.gz", 12470.0);
        triplesTimeStat.put("yago_links.nt.gz", 26450.0);
        triplesTimeStat.put("geo_coordinates_en.nt.gz", 5975.0);
        triplesTimeStat.put("disambiguations_en.nt.gz", 6653.0);
        triplesTimeStat.put("category_labels_en.nt.gz", 3526.0);
        triplesTimeStat.put("umbel_links.nt.gz", 3476.0);
        triplesTimeStat.put("iri_same_as_uri_en.nt.gz", 4077.0);
        triplesTimeStat.put("specific_mappingbased_properties_en.nt.gz", 3862.0);
        triplesTimeStat.put("homepages_en.nt.gz", 2061.0);
        triplesTimeStat.put("wordnet_links.nt.gz", 2109.0);
        triplesTimeStat.put("yago_taxonomy.nt.gz", 6756.0);
        triplesTimeStat.put("geonames_links.nt.gz", 2087.0);
        triplesTimeStat.put("linkedgeodata_links.nt.gz", 390.9);
        triplesTimeStat.put("raw_infobox_property_definitions_en.nt.gz", 269.7);
        triplesTimeStat.put("gadm_links.nt.gz", 142.1);
        triplesTimeStat.put("opencyc_links.nt.gz", 114.8);
        triplesTimeStat.put("musicbrainz_links.nt.gz", 104.6);
        triplesTimeStat.put("geospecies_links.nt.gz", 70.88);
        triplesTimeStat.put("linkedmdb_links.nt.gz", 61.88);
        triplesTimeStat.put("uscensus_links.nt.gz", 68.55);
        triplesTimeStat.put("eunis_links.nt.gz", 424.4);
        triplesTimeStat.put("bricklink_links.nt.gz", 50.76);
        triplesTimeStat.put("nytimes_links.nt.gz", 50.37);
        triplesTimeStat.put("bookmashup_links.nt.gz", 378.6);
        triplesTimeStat.put("wikicompany_links.nt.gz", 46.91);
        triplesTimeStat.put("italian_public_schools_links.nt.gz", 34.95);
        triplesTimeStat.put("drugbank_links.nt.gz", 33.87);
        triplesTimeStat.put("gutenberg_links.nt.gz", 27.63);
        triplesTimeStat.put("diseasome_links.nt.gz", 25.39);
        triplesTimeStat.put("sider_links.nt.gz", 25.62);
        triplesTimeStat.put("tcm_links.nt.gz", 21.88);
        triplesTimeStat.put("dailymed_links.nt.gz", 21.64);
        triplesTimeStat.put("dbtune_links.nt.gz", 20.91);
        triplesTimeStat.put("openei_links.nt.gz", 20.26);
        triplesTimeStat.put("amsterdammuseum_links.nt.gz", 20.55);
        triplesTimeStat.put("factbook_links.nt.gz", 40.06);
        triplesTimeStat.put("bbcwildlife_links.nt.gz", 18.75);
        triplesTimeStat.put("cordis_links.nt.gz", 21.18);
        triplesTimeStat.put("eurostat_linkedstatistics_links.nt.gz", 18.25);
        triplesTimeStat.put("dblp_links.nt.gz", 18.36);
        triplesTimeStat.put("gho_links.nt.gz", 19.68);
        triplesTimeStat.put("eurostat_wbsg_links.nt.gz", 18.0);
        triplesTimeStat.put("pnd_en.nt.gz", 16.63);
        triplesTimeStat.put("revyu_links.nt.gz", 17.85);
        
        */
    }

}
