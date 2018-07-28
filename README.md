# CerebralCortex-DataAnalysis

Cerebral Cortex is the big data cloud companion of mCerebrum designed to support population-scale data analysis, visualization, model development, and intervention design for mobile sensor data.

You can find more information about MD2K software on our [software website](https://md2k.org/software) or the MD2K organization on our [MD2K website](https://md2k.org/).

## Installation

#### Setup environment
Minimum requirements:
* [Python3.5](https://www.python.org/downloads/release/python-350/)
* [CerebralCortex-DockerCompose](https://github.com/MD2Korg/CerebralCortex-DockerCompose)
* [Apache Spark 2.2.0](https://spark.apache.org/releases/spark-release-2-2-0.html) 
* [CerebralCortex](https://github.com/MD2Korg/CerebralCortex-2.0.git)

#### Clone and configure CerebralCortex-DataAnalysis
* `clone https://github.com/MD2Korg/CerebralCortex-DataAnalysis.git`
* Configure database url, username, and passwords in [cc_configuration.yml](https://github.com/MD2Korg/CerebralCortex-2.0/blob/master/cerebralcortex/core/resources/cc_configuration.yml)
    * Please do not change other options in configurations unless you have changed them in CerebralCortex-DockerCompose   
* Install dependencies:
    * `pip install -r requirements.txt`


## Contributing
Please read our [Contributing Guidelines](https://md2k.org/contributing/contributing-guidelines.html) for details on the process for submitting pull requests to us.

We use the [Python PEP 8 Style Guide](https://www.python.org/dev/peps/pep-0008/).

Our [Code of Conduct](https://md2k.org/contributing/code-of-conduct.html) is the [Contributor Covenant](https://www.contributor-covenant.org/).

Bug reports can be submitted through [JIRA](https://md2korg.atlassian.net/secure/Dashboard.jspa).

Our discussion forum can be found [here](https://discuss.md2k.org/).

## Versioning

We use [Semantic Versioning](https://semver.org/) for versioning the software which is based on the following guidelines.

MAJOR.MINOR.PATCH (example: 3.0.12)

  1. MAJOR version when incompatible API changes are made,
  2. MINOR version when functionality is added in a backwards-compatible manner, and
  3. PATCH version when backwards-compatible bug fixes are introduced.

For the versions available, see [this repository's tags](https://github.com/MD2Korg/CerebralCortex-DataAnalysis/tags).

## Contributors

Link to the [list of contributors](https://github.com/MD2Korg/CerebralCortex-DataAnalysis/graphs/contributors) who participated in this project.

## License

This project is licensed under the BSD 2-Clause - see the [license](https://md2k.org/software-under-the-hood/software-uth-license) file for details.

## Acknowledgments

* [National Institutes of Health](https://www.nih.gov/) - [Big Data to Knowledge Initiative](https://datascience.nih.gov/bd2k)
  * Grants: R01MD010362, 1UG1DA04030901, 1U54EB020404, 1R01CA190329, 1R01DE02524, R00MD010468, 3UH2DA041713, 10555SC
* [National Science Foundation](https://www.nsf.gov/)
  * Grants: 1640813, 1722646
* [Intelligence Advanced Research Projects Activity](https://www.iarpa.gov/)
  * Contract: 2017-17042800006

