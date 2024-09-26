# Please add your team members' names here. 

## Team members' names 

1. Student Name: Danny Tran

   Student UT EID: dtt633

2. Student Name: Aaron Alvarez

   Student UT EID: aa88379

3. Student Name: Zak Noffel

   Student UT EID: zmn232

 ...

##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515
    


# Add your Project REPORT HERE 

Task 1 Results: 

00    116043
01    89509
02    69267
03    54430
04    42131
05    36295
06    67423
07    108464
08    127904
09    130772
10    127075
11    131952
12    139352
13    140259
14    147596
15    145644
16    127390
17    149506
18    174764
19    179497
20    168319
21    167168
22    160905
23    142804

Task 2 Results:

02510B3B0E797E51AF73361185F62D0B    100.0
067D656CDED3EEFD77F33BB8F67DC655    100.0
022B8DF4D6D7C4DCF11233DD74C9E189    100.0
0EE3FFCBDFD8B2979E87F38369A28FD9    100.0
0A0C3F3F29F62642A6DD9D9A087BFBBF    100.0

Task 3 Results:

74071A673307CA7459BCF75FBD024E09    350.0
96E79218965EB72C92A549DD5A330112    340.0
42AB6BEE456B102C1CF8D9D8E71E845A    319.25
DF9627DB375322E65F4648CA72F4C630    233.33333
7A7F0E9935777504427E4FB32C535A0D    83.33333
7034B759F78036EA3A326263E1997F80    30.645163
66D58AB619387EBB945E0E3ABE5E0A1C    29.62963
205E73579F21C2ED134DBD6CE7E4A1EA    29.166666
10B4945ABE2E627DB646B3C5226A4E50    38.88889
21B203A02C91D5272135DBBEBE6AFC00    20.0


YARN ResourceManager Screenshot for all Tasks

Task 1
![image](https://github.com/user-attachments/assets/251590fd-f868-41fd-bd8d-df7738eb169e)

Task 2
![image](https://github.com/user-attachments/assets/032d5150-631b-45f7-88d3-f2ed26113696)

Task 3
![image](https://github.com/user-attachments/assets/ff662ce1-e0f9-41d0-bbc3-e36a0ab3e6cb)


Google Cloud - Dataproc screenshot of machines running

![image](https://github.com/user-attachments/assets/6f7708b7-5d50-4efe-81b6-e08b3bfedeed)




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
