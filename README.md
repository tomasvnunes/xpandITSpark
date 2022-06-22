# XpandITSpark
Project developed to solve XpandIT's Big Data Software Engineer challenge. 

The goal of the challenge is to process two CSV files regarding Google Playstore apps e reviews using Spark and Scala.
The processing of these files includes various operations: filtering, re-mapping of types and names of columns, grouping, etc. 
A full description of the requirements and desired outputs can be checked [here](https://github.com/bdu-xpand-it/BDU-Recruitment-Challenges/wiki/Spark-2-Recruitment-Challenge).

## **Running the project:**
- Import it as a Maven project in your preferred IDE
- Add a run configuration to the Main class where you pass at least two arguments:
  - path to user reviews CSV (which must be inside this project's resources)
  - path to apps info CSV (which must be inside this project's resources)
  - **optionally** you can also pass a third argument which is the master options set in spark, 
  where you can specify whether to run locally or remote, specify number of threads, etc. 
(check [this link](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) to see more info on this). 
When not passed, it defaults to _**local[\*]**_
- Run it :)

### **Outputs**

Besides the logs provided, which allow to verify the correctness of each part of the challenge, two files are generated
through the execution of this project. This files can be found inside the **/output folder**:
- best_apps.csv -> contains info about all apps with rating >= 4
- googleplaystore_cleaned -> contains info about each app category (count, average rating and average sentiment polarity)

As said above, a complete description of what these files should contain can be seen [here](https://github.com/bdu-xpand-it/BDU-Recruitment-Challenges/wiki/Spark-2-Recruitment-Challenge).

## **Project structure**

The project contains a **Main class**, responsible for reading the arguments and verifying they were correctly passed,
loading file's paths and creating the Spark Session.

The **DataProcessor** is responsible for the entirety of the CSVs processing as specified. It uses an auxiliary class, 
**FileUtils** to load and save files, as well as rename and delete artifact files created by spark when saving DataFrames.

There is also a **Constants** class that is used by the others, which contains all the constants used in this project, 
including the default master option, all columns names and output file names. This class allows to easily change these
frequently accessed values in a centralized way.

---

### Notes: 
While developing this solution I found that one of the apps in the original CSV had a rating of 19.
For data integrity sake I chose to keep it as it was not said in the description that there is an upper limit on an app's rating
but given that most likely it is 5, in a real-case scenario I would have reached someone responsible to make sure that this was correct