# CerebralCortex-DataAnalysis

## How to run?
### Setup environment
Minimum requirements:
* [Python3.5](https://www.python.org/downloads/release/python-350/)
* [CerebralCortex-DockerCompose](https://github.com/MD2Korg/CerebralCortex-DockerCompose)
* [Apache Spark 2.2.0](https://spark.apache.org/releases/spark-release-2-2-0.html) 
* [CerebralCortex](https://github.com/MD2Korg/CerebralCortex)

### Clone and configure CerebralCortex-DataAnalysis
* clone https://github.com/MD2Korg/CerebralCortex-DataAnalysis.git
* Configure database url, username, and passwords in [cc_configuration.yml](https://github.com/MD2Korg/CerebralCortex-2.0/blob/master/cerebralcortex/core/resources/cc_configuration.yml)
    * Please do not change other options in configurations unless you have changed them in CerebralCortex-DockerCompose   
* Install dependencies:
    * pip install -r [requirements.txt](https://github.com/MD2Korg/CerebralCortex-DataAnalysis/requirements.txt)
