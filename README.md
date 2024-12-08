# Lakehouse-Project
Implementation of a Lakehouse Solution with Medallion Data Architecture

## Project Description
This project is about implementing Lakehouse solution for a wearable tech company (let's call it SBIT). SBIT produces smart watches that can monitor heart rate of the users so that they can evaluate their performance during the workouts in the gyms. Now we want to implement a Lakehouse solution for the company with medallion data architecture, collect and ingest data from data sources to the Lakehouse, and create reports for users in the below format:

*Workout BPM Summary*
|User id|Date|Workout id|Session id|Age|Sex|City|State|Min bpm|Avg bpm|Max bpm|
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
|12140|2019-12-11|5|478|18-25|M|Pear blossom|CA|205|86.97|114.89|176.84
|12140|2019-12-12|6|479|18-25|M|Pear blosoom|CA|206|77.76|112.73|181.24

*Gym Summary*
|Gym|Mac Address|Date|Minutes in Gym|Minutes Exercising|
|:---|:---|:---|:---|:---|
|4|32:60:3b:80:ed:8e|2019-12-11|107.23|96.00|
|4|32:60:3b:80:ed:8e|2019-12-12|108.60|100.26|

Beyond these basic requirements, we also want to conduct some good practices in the project, including:

1. Decouple data integration from data procesing
2. Support batch and stream workflows
3. Govern structured and unstructured data with Unity Catalog

This project is [inspired by Prashant Kumar Pandey](https://www.linkedin.com/in/prashant-kumar-pandey), I try to provide a detailed explanation for it and the process of approaching the solution.

## Data Sources

Relational Database:
- User Registration Table
- Gym Login Logout Info Table

Kafka Topic:
- User Profile Update
- Heart Rate Stream
- Workout Session

To decouple operational workloads from our Lakehouse design, we assume that User Profile Update stream can query the User Registration Table, and there assumes to be a table in place to combine the login and logout events of users in gyms.

Sample data:





