# mDebugger - Classify missing mobile sensor data
mDebugger is part of CerebralCortex data analysis tool suite. mDebugger helps to identify causes of missing data and classify them. For example, data was missing due to wireless disconnection, sensor powered off, and/or packet loss etc. For more details please read our [mDebugger book chapter](https://www.researchgate.net/publication/318384686_mDebugger_Assessing_and_Diagnosing_the_Fidelity_and_Yield_of_Mobile_Sensor_Data). 

## How to run?
### Setup environment
To run mdebugger, Clone/install and configure:
* [Python3.5](https://www.python.org/downloads/release/python-350/)
* [CerebralCortex-DockerCompose](https://github.com/MD2Korg/CerebralCortex-DockerCompose)
* [Apache Spark 2.2.0](https://spark.apache.org/releases/spark-release-2-2-0.html) 
* [CerebralCortex](https://github.com/MD2Korg/CerebralCortex-2.0.git)

### Clone and configure 
* clone https://github.com/MD2Korg/CerebralCortex-DataAnalysis.git
* Configure database url, username, and passwords in [cc_configuration.yml](https://github.com/MD2Korg/CerebralCortex-2.0/blob/master/cerebralcortex/core/resources/cc_configuration.yml)
    * Please do not change other options in configurations unless you have changed them in CerebralCortex-DockerCompose   
* Install dependencies:
    * pip install -r [requirements.txt](https://github.com/MD2Korg/CerebralCortex-DataAnalysis/requirements.txt)
* Run following command to start mDebugger:
    * sh run.sh (Please update the paths in run.sh, read comments)