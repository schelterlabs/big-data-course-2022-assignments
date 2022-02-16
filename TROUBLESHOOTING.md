## Known problems and solutions

* ### Problem: `docker run` crashes  
  * __Seen on__: Windows  
  * __Solution__: Make sure that your machine has enough diskspace available (e.g., via [spacesniffer](http://www.uderzo.it/main_products/space_sniffer/))

 * ### Problem: `docker run` works, however the 'work' directory is empty in the Jupyer notebook
   * __Seen on__: Windows10, Windows11 and MacOs
   * __Solution__: For some students `$(pwd)` returns an empty string. Fix this by hardcoding the location containining the Assignments, for example:
 ```
 docker run -p 8888:8888 -v "c:\users\jo\my documents\big data":/home/jovyan/work  -e JUPYTER_ENABLE_LAB=yes mtasnim/jupyter-pyspark-duckdb
 ```
 
 * ### Problem: Warning that Docker cannot be used unless Virtualization is enabled in the bios. 
   * __Seen on__: Windows  
   * __Solution__: Enable virtualization in the bios

 * ### Problem: Docker does not start on a new Mac M1
   * __Seen on__: MacOs (M1)
   * Solution: Make sure that you installed Docker for M1 processors, and not Docker for Intel processors.

 * ### Problem: Docker crashes under Ubuntu with a message like `exec user process caused: exec format error`
   * __Seen on__: Ubuntu 20
   * __Solution__: Follow the instructions [outlined here](https://github.com/schelterlabs/big-data-course-2022-assignments/issues/1) to install additional  virtualization software

 * ### Problem: Multiple Jupyter instances running at the same time
   * __Seen on__: various operating systems
   * __Solution__: First open localhost:8888 and exit the notebook and then run the docker container (and the Jupyter notebook inside it)

 * ### Problem: Docker error `invalid reference format: repository name must be lowercase`
   * __Solution__: Follow the hints outlined in this [stackoverflow post](https://stackoverflow.com/questions/45682010/docker-invalid-reference-format) 


 * ### Problem: Webbrowser halts when connecting to docker 
   * __Seen on__: MacOs (M1)
   * __Solution__: Settings (MAC) -> 'Complete DiskAccess' -> and Select Docker, then restart Docker


## Manual installation of the environment without Docker under Windows

We strongly recommend everyone to use the Docker image, in order to ensure that there are no version conflicts and that your code can interact correctly with the grading server. If you cannot get Docker to work under Windows, there is also the possibility of manually installing the programming environment without Docker. Please follow the steps outlined below:

To run the assignments without Docker, you will need the follow dependencies:

  * __OpenJDK 11__ – [download page](https://jdk.java.net/archive/) and [installation instructions](https://stackoverflow.com/questions/52511778/how-to-install-openjdk-11-on-windows)
  * __Python 3.9 with Anaconda__, [download and installation instructions](https://docs.anaconda.com/anaconda/install/windows/)
 
Additionally, you will need the following Python libraries:
 
 * __Jupyterlab__, which you can install using the following conda command `conda install -c conda-forge jupyterlab`
 * __DuckDB__, which you can install with `pip install duckdb==0.3.2`
 * __PySpark (version 3.2.0)__ - Follow the [installation instructions](https://sparkbyexamples.com/pyspark/how-to-install-and-run-pyspark-on-windows/) here. 
Since you would already have OpenJDK 11 and Anaconda installed, skip to the section “PySpark Install on Windows”. If you are still having problems on Windows after these steps, step 5 from [this guide for using PySpark from Jupyter in Windows](https://bigdata-madesimple.com/guide-to-install-spark-and-use-pyspark-from-jupyter-in-windows/) might help.

_Note that the instructions in the linked webpage are given for PySpark version 3.0.0, please adapt them accordingly for downloading and installing PySpark version 3.2.0. To find version 3.2.0 in the [Spark Downloads](https://spark.apache.org/downloads.html) page, navigate to [Spark release archives](https://archive.apache.org/dist/spark/) and download the file `spark-3.2.0-bin-hadoop3.2.tgz`_
 




