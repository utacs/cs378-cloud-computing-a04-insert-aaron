# Please add your team members' names here. 

## Team members' names 

1. Student Name:

   Student UT EID:

2. Student Name:

   Student UT EID:

 ...

##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515
    


# Add your Project REPORT HERE 

Task 1 Results: 
   00      66378
   01      47585
   02      38488
   03      30002
   04      23398
   05      20852
   06      43037
   07      76217
   08      96432
   09      95573
   10      88381
   11      93047
   12      99217
   13      98879
   14      102711
   15      101213
   16      93689
   17      112113
   18      131502
   19      130576
   20      115844
   21      109770
   22      104044
   23      81051

Task 2 Results:
   


# Project Template

# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.cs.utexas.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 

# Running:




## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```	mvn clean package ```



## Run your application
Inside your shell with Hadoop

Running as Java Application:

```java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar SOME-Text-Fiel.txt  output``` 

``` Task 1 Run: ```
java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar taxi-data-sorted-small.csv t1_result.txt 

``` Task 2 Run: ```
java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar taxi-data-sorted-large.csv task2map task2red ---> large data



java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar taxi-data-sorted-small.csv task2map task2red ---> small data


 ## Task 3
java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar taxi-data-sorted-small.csv task3map task3red


Or has hadoop application

```hadoop jar your-hadoop-application.jar edu.cs.utexas.HadoopEx.WordCount arg0 arg1 ... ```



## Create a single JAR File from eclipse



Create a single gar file with eclipse 

*  File export -> export  -> export as binary ->  "Extract generated libraries into generated JAR"
