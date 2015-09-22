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

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        this.service = new EcarfGoogleCloudServiceImpl();
        TestUtils.prepare(service);
        
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
        
        for(Item item: items) {
            System.out.println("Filename: " + item.getKey());
            System.out.println("Weight: " + item.getWeight() + "\n");
            
            Long statements = this.triplesStat.get(item.getKey());
            assertNotNull(statements);
            
            assertEquals(statements, item.getWeight());
            
        }
    }
    
    @Test
    public void testCreateTriplesFilesStats() throws IOException {
          
        List<TriplesFileStats> stats = new ArrayList<>();
        
        for(Entry<String, Long> entry: triplesStat.entrySet()) {
            TriplesFileStats stat = new TriplesFileStats();
            stat.setFilename(entry.getKey());
            stat.setStatements(entry.getValue());
            stats.add(stat);
        }
        
        String filename = FilenameUtils.getLocalFilePath(FilenameUtils.TRIPLES_FILES_STATS_JSON);
        
        TriplesFileStats.toJsonFile(filename, stats, false);
        
        System.out.println("Successfully created: " + filename);
    }

}
