/**
 * 
 */
package io.ecarf.core.triple;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An enum of all the schema URIs and their classification
 * into TBox and ABox
 * @author omerio
 *
 */
public enum SchemaURIType {
    
	// tbox, abox, rdf, owl
	
	RDF_TYPE("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", false, true, true, false, 0),
	RDF_PROPERTY("<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>", false, true, true, false, 1),
	RDF_SEQ("http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq>", false, false, true, false, 2),
	RDF_NIL("<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>", false, false, true, false, 3),
    RDF_LIST("<http://www.w3.org/1999/02/22-rdf-syntax-ns#List>", false, false, true, false, 4),
    RDF_FIRST("<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>", false, false, true, false, 5),
    RDF_REST("<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>", false, false, true, false, 6),
	
	RDFS_RANGE("<http://www.w3.org/2000/01/rdf-schema#range>", true, false, true, false, 7),
	RDFS_DOMAIN("<http://www.w3.org/2000/01/rdf-schema#domain>", true, false, true, false, 8),
	RDFS_SUBPROPERTY("<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>", true, false, true, false, 9),
	RDFS_SUBCLASS("<http://www.w3.org/2000/01/rdf-schema#subClassOf>", true, false, true, false, 10),
	RDFS_MEMBER("<http://www.w3.org/2000/01/rdf-schema#member>", false, false, true, false, 11),
	RDFS_LITERAL("<http://www.w3.org/2000/01/rdf-schema#Literal>", false, false, true, false, 12),
	RDFS_CONTAINER_MEMBERSHIP_PROPERTY("<http://www.w3.org/2000/01/rdf-schema#ContainerMembershipProperty>", false, false, true, false, 13),
	RDFS_DATATYPE("<http://www.w3.org/2000/01/rdf-schema#Datatype>", false, false, true, false, 14),
	RDFS_CLASS("<http://www.w3.org/2000/01/rdf-schema#Class>", false, false, true, false, 15),
	RDFS_RESOURCE("<http://www.w3.org/2000/01/rdf-schema#Resource>", false, false, true, false, 16),
	
	RDFS_LABEL("http://www.w3.org/2000/01/rdf-schema#label", false, false, true, false, 17),
	RDFS_COMMENT("http://www.w3.org/2000/01/rdf-schema#comment", false, false, true, false, 18),
	
	OWL_CLASS("<http://www.w3.org/2002/07/owl#Class>", false, false, false, true, 19),
	OWL_FUNCTIONAL_PROPERTY("<http://www.w3.org/2002/07/owl#FunctionalProperty>", false, false, false, true, 20),
	OWL_INVERSE_FUNCTIONAL_PROPERTY("<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>", false, false, false, true, 21),
	OWL_SYMMETRIC_PROPERTY("<http://www.w3.org/2002/07/owl#SymmetricProperty>", false, false, false, true, 22),
	OWL_TRANSITIVE_PROPERTY("<http://www.w3.org/2002/07/owl#TransitiveProperty>", false, false, false, true, 23),
	OWL_SAME_AS("<http://www.w3.org/2002/07/owl#sameAs>", false, false, false, true, 24),
	OWL_INVERSE_OF("<http://www.w3.org/2002/07/owl#inverseOf>", false, false, false, true, 25),
	OWL_EQUIVALENT_CLASS("<http://www.w3.org/2002/07/owl#equivalentClass>", false, false, false, true, 26),
	OWL_EQUIVALENT_PROPERTY("<http://www.w3.org/2002/07/owl#equivalentProperty>", false, false, false, true, 27),
	OWL_HAS_VALUE("<http://www.w3.org/2002/07/owl#hasValue>", false, false, false, true, 28),
	OWL_ON_PROPERTY("<http://www.w3.org/2002/07/owl#onProperty>", false, false, false, true, 29),
	OWL_SOME_VALUES_FROM("<http://www.w3.org/2002/07/owl#someValuesFrom>", false, false, false, true, 30),
	OWL_ALL_VALUES_FROM("<http://www.w3.org/2002/07/owl#allValuesFrom>", false, false, false, true, 31),
	OWL_NAMED_INDIVIDUAL("<http://www.w3.org/2002/07/owl#NamedIndividual>", false, false, false, true, 32),
	
	// TODO review the OWL & OWL2 URIs
	OWL_DATA_PROPERTY("http://www.w3.org/2002/07/owl#DatatypeProperty", false, false, false, true, 33),
	OWL_OBJECT_PROPERTY("http://www.w3.org/2002/07/owl#ObjectProperty", false, false, false, true, 34),
	
	OWL_ONTOLOGY("<http://www.w3.org/2002/07/owl#Ontology>", false, false, false, true, 35),
	OWL_VERSION_INFO("http://www.w3.org/2002/07/owl#versionInfo", false, false, false, true, 36),
	
	OWL2_PROPERTY_CHAIN_AXIOM("<http://www.w3.org/2002/07/owl#propertyChainAxiom>", false, false, false, true, 37),
    OWL2_HAS_KEY("<http://www.w3.org/2002/07/owl#hasKey>", false, false, false, true, 38),
	
	// XML Schema 
	XML_STRING("http://www.w3.org/2001/XMLSchema#string", false, false, false, false, 39),
	XML_MONTH("http://www.w3.org/2001/XMLSchema#gMonth", false, false, false, false, 40),
	XML_YEAR("http://www.w3.org/2001/XMLSchema#gYear", false, false, false, false, 41),
	;
	
	public final String uri;
	
	public final boolean tbox;
	public final boolean abox;
	public final boolean rdf;
	public final boolean owl; 
	public final int id;

	/**
	 * 
	 * @param uri
	 * @param tbox
	 * @param abox
	 * @param rdf
	 * @param owl
	 */
	private SchemaURIType(String uri, boolean tbox, boolean abox, boolean rdf, boolean owl, int id) {
		this.uri = uri;
		this.tbox = tbox;
		this.abox = abox;
		this.rdf = rdf;
		this.owl = owl;
		this.id = id;
	}
	
	/**
	 * Mappings of schema enum and uri
	 */
	private static final Map<String, SchemaURIType> MAPPINGS = new HashMap<>();
	static {
		for(SchemaURIType type: SchemaURIType.values()) {
			MAPPINGS.put(type.getUri(), type);
		}
	}
	
	/**
	 * Get mappings of the schema enums keyed by uri
	 * @return
	 */
	public static Map<String, SchemaURIType> mappings() {
		return Collections.unmodifiableMap(MAPPINGS);
	}
	
	/**
	 * 
	 * @param uri
	 * @return
	 */
	public static SchemaURIType getByUri(String uri) {
		return MAPPINGS.get(uri);
	}
	
	/**
	 * 
	 * @param uri
	 * @return
	 */
	public static boolean isSchemaUri(String uri) {
		return (MAPPINGS.get(uri) != null);
	}
	
	/**
	 * @return true if the provided uri is for a rdf axiom
	 */
	public static boolean isRdf(String uri) {
		SchemaURIType type = getByUri(uri);
		return (type != null) ? type.isRdf() : false;
	}
	
	
	/**
	 * @return true if the provided uri is for a owl axiom
	 */
	public static boolean isOwl(String uri) {
		SchemaURIType type = getByUri(uri);
		return (type != null) ? type.isOwl() : false;
	}
	

	/**
	 * @return true if the provided uri is for a TBox axiom
	 */
	public static boolean isTbox(String uri) {
		SchemaURIType type = getByUri(uri);
		return (type != null) ? type.isTbox() : false;
	}
	
	/**
	 * @return true if the provided uri is for a ABox axiom
	 */
	public static boolean isAbox(String uri) {
		SchemaURIType type = getByUri(uri);
		return (type != null) ? type.isAbox() : false;
	}
	
	/**
	 * @return true if the provided uri is for a rdf TBox axiom
	 */
	public static boolean isRdfTbox(String uri) { 
		return isRdf(uri) && isTbox(uri);
	}
	
	/**
	 * @return true if the provided uri is for a rdf ABox axiom
	 */
	public static boolean isRdfAbox(String uri) {
		return isRdf(uri) && isAbox(uri);
	}
	
	/**
	 * 
	 * @return
	 */
	public String getUri() {
		return uri;
	}

	/**
	 * @return the tbox
	 */
	public boolean isTbox() {
		return tbox;
	}

	/**
	 * @return the abox
	 */
	public boolean isAbox() {
		return abox;
	}

	/**
	 * @return the rdf
	 */
	public boolean isRdf() {
		return rdf;
	}

	/**
	 * @return the owl
	 */
	public boolean isOwl() {
		return owl;
	}

	
	
}
