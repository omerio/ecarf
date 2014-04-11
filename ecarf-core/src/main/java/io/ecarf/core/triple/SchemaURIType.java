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
	
	RDF_TYPE("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", false, true, true, false),
	RDF_PROPERTY("<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>", false, true, true, false),
	RDF_SEQ("http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq>", false, false, true, false),
	
	RDFS_RANGE("<http://www.w3.org/2000/01/rdf-schema#range>", true, false, true, false),
	RDFS_DOMAIN("<http://www.w3.org/2000/01/rdf-schema#domain>", true, false, true, false),
	RDFS_SUBPROPERTY("<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>", true, false, true, false),
	RDFS_SUBCLASS("<http://www.w3.org/2000/01/rdf-schema#subClassOf>", true, false, true, false),
	RDFS_MEMBER("<http://www.w3.org/2000/01/rdf-schema#member>", false, false, true, false),
	RDFS_LITERAL("<http://www.w3.org/2000/01/rdf-schema#Literal>", false, false, true, false),
	RDFS_CONTAINER_MEMBERSHIP_PROPERTY("<http://www.w3.org/2000/01/rdf-schema#ContainerMembershipProperty>", false, false, true, false),
	RDFS_DATATYPE("<http://www.w3.org/2000/01/rdf-schema#Datatype>", false, false, true, false),
	RDFS_CLASS("<http://www.w3.org/2000/01/rdf-schema#Class>", false, false, true, false),
	RDFS_RESOURCE("<http://www.w3.org/2000/01/rdf-schema#Resource>", false, false, true, false),
	
	RDFS_LABEL("http://www.w3.org/2000/01/rdf-schema#label", false, false, true, false),
	RDFS_COMMENT("http://www.w3.org/2000/01/rdf-schema#comment", false, false, true, false),
	
	OWL_CLASS("<http://www.w3.org/2002/07/owl#Class>", false, false, false, true),
	OWL_FUNCTIONAL_PROPERTY("<http://www.w3.org/2002/07/owl#FunctionalProperty>", false, false, false, true),
	OWL_INVERSE_FUNCTIONAL_PROPERTY("<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>", false, false, false, true),
	OWL_SYMMETRIC_PROPERTY("<http://www.w3.org/2002/07/owl#SymmetricProperty>", false, false, false, true),
	OWL_TRANSITIVE_PROPERTY("<http://www.w3.org/2002/07/owl#TransitiveProperty>", false, false, false, true),
	OWL_SAME_AS("<http://www.w3.org/2002/07/owl#sameAs>", false, false, false, true),
	OWL_INVERSE_OF("<http://www.w3.org/2002/07/owl#inverseOf>", false, false, false, true),
	OWL_EQUIVALENT_CLASS("<http://www.w3.org/2002/07/owl#equivalentClass>", false, false, false, true),
	OWL_EQUIVALENT_PROPERTY("<http://www.w3.org/2002/07/owl#equivalentProperty>", false, false, false, true),
	OWL_HAS_VALUE("<http://www.w3.org/2002/07/owl#hasValue>", false, false, false, true),
	OWL_ON_PROPERTY("<http://www.w3.org/2002/07/owl#onProperty>", false, false, false, true),
	OWL_SOME_VALUES_FROM("<http://www.w3.org/2002/07/owl#someValuesFrom>", false, false, false, true),
	OWL_ALL_VALUES_FROM("<http://www.w3.org/2002/07/owl#allValuesFrom>", false, false, false, true),
	OWL_NAMED_INDIVIDUAL("<http://www.w3.org/2002/07/owl#NamedIndividual>", false, false, false, true),
	
	OWL_DATA_PROPERTY("http://www.w3.org/2002/07/owl#DatatypeProperty", false, false, false, true),
	OWL_OBJECT_PROPERTY("http://www.w3.org/2002/07/owl#ObjectProperty", false, false, false, true),
	
	OWL_ONTOLOGY("<http://www.w3.org/2002/07/owl#Ontology>", false, false, false, true),
	OWL_VERSION_INFO("http://www.w3.org/2002/07/owl#versionInfo", false, false, false, true),
	
	// XML Schema 
	XML_STRING("http://www.w3.org/2001/XMLSchema#string", false, false, false, false),
	XML_MONTH("http://www.w3.org/2001/XMLSchema#gMonth", false, false, false, false),
	XML_YEAR("http://www.w3.org/2001/XMLSchema#gYear", false, false, false, false),
	;
	
	private final String uri;
	
	private final boolean tbox;
	private final boolean abox;
	private final boolean rdf;
	private final boolean owl; 

	/**
	 * 
	 * @param uri
	 * @param tbox
	 * @param abox
	 * @param rdf
	 * @param owl
	 */
	private SchemaURIType(String uri, boolean tbox, boolean abox, boolean rdf, boolean owl) {
		this.uri = uri;
		this.tbox = tbox;
		this.abox = abox;
		this.rdf = rdf;
		this.owl = owl;
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
