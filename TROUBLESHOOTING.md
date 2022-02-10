
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
