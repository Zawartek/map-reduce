Run the server Master :
Run Configuration -> Java Application
	Class Main : org.isep.mapReduce.ServerMain
	Arguments : currentServer
	Arguments example : localhost:7200

Run the server Slave :
Run Configuration -> Java Application
	Class Main : org.isep.mapReduce.ServerMain
	Arguments : currentServer MasterServer
	Arguments example : localhost:7300 localhost:7200


Run the Client :
Run Configuration -> Java Application
	Class Main : org.isep.mapReduce.ClientMain
	Arguments : address port pathToFileList
	Arguments example : localhost 7200 ./MapReduce/filelist_500.txt
	
To run the Map/Reduce program you need to run :
1) The Master Server
2) One or More Slave Server
3) The Client Server

The files in the filelist are to huge to be in a Git and can provocate many errors due to file names.
The link to download the data files to use :
https://drive.google.com/file/d/0B2Mzhc7popBga2RkcWZNcjlRTGM/edit

The repository Gutenberg need to be in the MapReduce repository for the program to perform with the filelist files in the project.