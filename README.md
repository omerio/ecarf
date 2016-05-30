# ECARF

[![Join the chat at https://gitter.im/omerio/ecarf](https://badges.gitter.im/omerio/ecarf.svg)](https://gitter.im/omerio/ecarf?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[ECARF](http://ecarf.io) is an elastic cloud-based RDF triplestore. ECARF is implemented using the [CloudEx](http://cloudex.io) framework. ECARF uses cloud virtual machines, cloud storage and cloud-based big data columnar databases to provide an elastic RDF triplestore.

The current implementation of ECARF provides the following features:

* Implementation for the [Google Cloud Platform](https://cloud.google.com/) APIs.
* Pre-processing and analysis of RDF dataset files in [Google Cloud Storage](https://cloud.google.com/storage/).
* Efficient dictionary encoding for RDF URIRefs.
* Loading of RDF datasets into [Google BigQuery](https://cloud.google.com/bigquery/) (Big Data columnar database).
* Forward rule-based reasoning using a subset of [RDFS entailment rules](https://www.w3.org/TR/rdf11-mt/#patterns-of-rdfs-entailment-informative) (rdfs2, rdfs3, rdfs7 & rdfs9)

  ## Getting Started

  To build the libraries:

  ```bash
      git clone https://github.com/omerio/ecarf.git
      cd ecarf
      mvn install
  ```   

  ### Requirements

  Java 7 or above.

  ### Usage

  TODO

  ## Documentations

  To find out more about ECARF, check out the [documentation](https://github.com/omerio/ecarf/wiki).

  ## Contributing

  See the [CONTRIBUTING Guidelines](https://github.com/omerio/ecarf/blob/master/CONTRIBUTING.md)

  ## Support
  If you have any problem or suggestion please open an issue [here](https://github.com/omerio/ecarf/issues).

  ## License
  Apache 2.0 - See [LICENSE](https://github.com/omerio/ecarf/blob/master/LICENSE) for more information.
