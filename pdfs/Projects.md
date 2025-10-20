Projects
The goal of the project is implementing a few full stack data pipelines that go collect raw data, clean them, transform them, and make them accessible via to simple visualisations.

You should identify a domain, two different data sources, and formulate 2-3 questions in natural language that you would like to answer. Such question will be necessary for the data modelling effort (i.e., creating a database).

“Different” means different formats, access patterns, frequency of update, etc. In practice, you have to justify your choice!

If you cannot identify a domain yourself, you will be appointed assigned on by the curt teacher.

pipeline_physical

The final frontend can be implemented using Jupyter notebook, Grafana, StreamLit, or any software of preference to showcase the results. THIS IS NOT PART OF THE EVALUATION!

The project MUST include all the three areas discussed in class (see figure above), i.e., ingestion of (raw) data, staging zone for cleaned and enriched data, and a curated zone for production data analytics. To connect the various zones, you should implement the necessary data pipelines using Apache Airflow. Any alternative should be approved by the teacher. The minimum number of pipelines is 3:

A first pipeline is responsible to bring raw data to the landing zone. Such pipeline is supposed to ingest data from the source identified at first and bring them to a transient storage.
A second pipeline is responsible to migrate raw data from the landing zone and move them into the staging area. In practice, the second pipeline is supposed to
clean the data according to some simple techniques saw in class (extra techniques are welcome yet not necessary)
wrangle/transform the data according to the analysis needs (the questions formulated at first)
enrich the data by joining multiple datasets into a single one.
persist data for durability (the staging zone is a permanent): i.e., resit to a reboot of your (docker) environment.
The third pipeline is responsible to move the data from the staging zone into the production zone and trigger the update of data marts (views). Such pipeline shall perform some additional transformation and feed the data systems of choice for populate the analysis.
The production zone is also permanent and data shall be stored to prevent loss.
such pipeline is also responsible to launch the queries implemented according to one of the analytics languages of choice (SQL/Cypher)
If you are using SQL, the final database should follow the star schema principles
for the graph database instead it is sufficient to implement the queries.
The figure below is meant to depict the structure of the project using the meme dataset as an example.

project pipeline.jpg

Project Minimal Submission Checklist
repository with the code, well documented, including:
docker-compose file to run the environment
detailed description of the various steps
report in the Repository README with the project design steps (divided per area), a guide how to run it, and consideration relevant to understand it.
Example dataset: the project testing should work offline, i.e., you need to have some sample data points.
slides for the project poster. You can do them too in markdown too.
Project grading 0-10 + 5 report (accuracy, using proper terminology, etc) + 5 for the poster.

Increasing Project Grade
The project grading follows a portfolio-based approach, once you achieved the minimum level (described above), you can start enchancing and collect extra points. How?

+1: Include in the pipelines any tool discussed during the course
Examples: Redis for cashing, MongoDB for ingestion, Neo4j as alternative to the star schema, etc.
Requirement: It has to be used properly, always better to double check with teacher.
+1: Discuss one of the theoretical topics from the course, e.g., add considerations about governance, privacy, etc
Requirement: It should be included in the report and the poster.
+2: Using any Data Engineering tool not explained during the course
Requirement: It should be state of the art tooling.
Every extra should be approved
Example: using Kafka for ingestion and staging (also + docker)
+X: Creativity section
Ask questions: can get extra points if we do this?
Examples: data viz, serious analysis, performance analysis.
pipeline_physical_all.png

:bangbang::bangbang: Project Registration Form (courtesy of Kevin Kanaan) :bangbang::bangbang:

Pre-Approved Datasets
Meme Dataset NOTE: analyses MUST SUBSTANTIALLY differ from the example!
China Labour Bulleting
GDELT (Real-Time)
Aria Database for Technological Accidents (French Government, requires registration)
Fatality Inspection Data
Wikimedia Recent Changes (or others) (Real Time)
Wikipedia:
List of Oil Spills
Mining Accidents
Industrial Disasters
Hackathon Dataset (Courtesy of Prof Alex Nolte)
Data Is Plural — 2000+ Dataset Archive
Example Projects From Previous Years
Dota Pipeline
Goonies
DISSING
FAQ
While ingesting the data do we pull it from the internet or download it on our computer and then import it?
The ingestion part is about downloading and fetching the data so go get it on internet.
Do we filter the data in ingestion?
It depends, sometimes you may want to filter the data, especially when sensitive data is collected adn should be censored / removed.
Should data be persistent in the staging phase?
Yes, if staging stops at any moment, progress should not be lost/
Should data be persistent in the ingestion phase?
It’s not mandatory, but keep in mind that persisting data comes with its own challenges, you might risk ending up with duplicate saved data.
What is the data goal at the end of the staging phase?
At the end of the staging phase you should only have clean data. ie: no data in pulled from there leads to an error