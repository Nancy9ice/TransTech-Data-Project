# TransTech Data Project
This project is a proof of my ability to work with big data using Pyspark(python API of Spark), PostGIS, and SQL on Amazon Redshift

![](https://images.unsplash.com/photo-1554260570-83dc2f46ef79?ixlib=rb-4.0.3&ixid=MnwxMjA3fDB8MHxzZWFyY2h8OXx8VHJhbnNwb3J0YXRpb24lMjBUZWNobm9sb2d5fGVufDB8fDB8fA%3D%3D&auto=format&fit=crop&w=500&q=60 "Image by @austindistel on Unsplash.com")

## Introduction

This project is based on the Transportation industry (specifically Uber) in United States to show my skills in handling big data.

*Disclaimer: This dataset was gotten from the [shared uber dataset on github](https://github.com/fivethirtyeight/uber-tlc-foil-response).*

## Skills/concepts demonstrated

The following tools and skills were used in this project:
* **Pyspark** (Python API of Spark) used to transform the data.
* **PostGIS** used to geocode the spatial data.
* **PostgreSQL** hosted the PostGIS extension used.
* **Jupyter Notebook** used to editand run my pyspark scripts
* **Amazon S3** used to store the already transformed data
* **Amazon Redshift** used to query the data to answer questions

NOTE: While working on this project, I learnt beyond these skills. However, the above listed are the skills demonstrated in this project. You can read more details here.

## Cleaned Datasets

The transformed datasets are found [in this link](https://drive.google.com/file/d/1wa1IaFUIJFqvB7G6rZWNFIq8D38qKn0D/view?usp=sharing) that contains two csv files. 

The one named "cleaned_dataset2.csv" is the immediate dataset after geocoding while the one named "uber.csv" is the one uploaded to Amazon Redshift that contains only relevant columns. 

## Questions 

1. What are the earliest and latest times of the rides?
2. How many rides were there in each month?
3. What were the total rides for the top 10 pickup counties?
4. What were the total rides for the bottom 10 pickup counties?
5. What were the total rides for the respective pickup locations?
6. What were the total rides by day?
7. How many vehicles are allocated to the registered base stations and locations?
8. According to your answer in number 7, is it advisable to regsiter more base stations or stick to the already registered base stations?

## Methodology

The following steps were carried out to go from to my unclean, unusable dataset to a clean and usable one: 

* Downloading of the various datasets from the [uber github respository](https://github.com/fivethirtyeight/uber-tlc-foil-response).
* Data transformation using pyspark where:
    * column names were made more readable
    * datatypes were established for relevant columns
    * null and duplicate values were dropped
    * columns were rearranged in a reasonable order
* Transformed data was written into PostgreSQL
* Longitude and Latitude coordinates were geocoded using PostGIS. You can read my step by step guide for this [here](https://medium.com/@amandinancy16/reverse-geocoding-using-postgis-and-tiger-dataset-b59b60ca071b).
* Relevant columns were read into a CSV file.
* CSV file was uploaded to Amazon S3.
* CSV file was loaded into Amazon Redshift.
* Data was queried from Amazon Redshift to answer questions.

## Answers to Stated Questions

### 1. What are the earliest and latest times of the rides?

```sql
SELECT Min(time) AS earliest_time, Max(time) AS latest_time
FROM uber2014
```

**Results**

According to the results, the uber rides went round the clock.

![](Earliest%20and%20Latest%20Time.PNG)


### 2. How many rides were there in each month?

```sql
SELECT TO_CHAR(date, 'Month') AS month, COUNT(*)
FROM uber2014
GROUP BY month
ORDER BY COUNT(*) DESC
```

**Results**

The rides increased as time advanced. This could be a proof that the marketing strategies of Uber are working or customers are satisfied with the services and refer their friends and family. 

![](Rides%20by%20Month.PNG)

### 3. What were the total rides for the top 10 pickup counties?

```sql
SELECT DISTINCT(county), COUNT(*)
FROM uber2014
GROUP BY county
ORDER BY COUNT(*) DESC
LIMIT 10
```

**Results**

For context, New York County and Kings County are the same as Manhattan and Brooklyn respectively. According to the results, Manhattan has the most rides.

![](Top%2010%20Rides%20by%20County.PNG)

Let's do a little comparison with the 2014 New York population in some counties.

![](New%20York%20Population.PNG)

With this comparison, we can't just conclude that Uber is mostly used by the Manhattan people because our data doesn't provide the number of unique users unlike the population census results that is per head count. 

However, I would advise that Uber focuses more on regions that have relatively less population and population density so they won't attract attention from the government that they are causing traffic. These regions could be Bronx, Brooklyn(Kings) and Queens counties in the New York City. 

They can also extend their tentacles outside New York City. 

### 4. What were the total rides for the bottom 10 pickup counties?

```sql
SELECT DISTINCT(county), COUNT(*)
FROM uber2014
GROUP BY county
ORDER BY COUNT(*) 
LIMIT 10
```

**Results**

Depending on the goals of Uber, they could either close down theur services in these regions that have extremely low patronage or strategize on how to increase awareness and take out any competition that might be there. 

![](Bottom%2010%20Rides%20by%20County.PNG)

### 5. What were the total rides for the respective pickup locations?

```sql
SELECT DISTINCT(location), COUNT(*)
FROM uber2014
GROUP BY location
ORDER BY COUNT(*) DESC
```

**Results**

Take note that New York City is in New York State and are somewhat different. New York City comprises five boroughs: Brooklyn (Kings), Queens, Manhattan, the Bronx, and Staten Island (Richmond). 

According to the results, Uber is well known in New York City than other states in United States. 

![](Rides%20by%20Locations.PNG)

### 6. What were the total rides by day?
```sql
SELECT TO_CHAR(date, 'day') AS day, COUNT(*)
FROM uber2014
GROUP BY day
ORDER BY COUNT(*) DESC
```

**Results**

Looking at these results, there's not much gap amongst the total rides for the different days so we can't just outrightly conclude that most people use Uber on certain days. 

![](Rides%20by%20Day.PNG)

### 7. How many vehicles are allocated to the registered base stations and locations?

```sql
SELECT DISTINCT(base_region), base_name, COUNT(*)
FROM uber2014
GROUP BY base_region, base_name
ORDER BY COUNT(*) DESC
```

**Results**

For comtext, let me briefly explain what Base stations mean. 

See Base stations as the administrative houses of the uber riders. Just like how you go to your workplace when you want to answer to a physical meeting hosted by your employer, that's how uber riders see the base stations. 

These base stations provide support to the uber riders, pass information, and also track the movement of the registered riders. 

The base_name are the names for each  company associated with the assigned base codes while base regions are where they are located. 

![](Rides%20by%20Bases.PNG)

### 8. According to your answer in number 7, is it advisable to register more base stations or stick to the already registered base stations?

According to the results, it's obvious that the base stations are only located in New York City. So what about the regions outside New York City(NYC? 

What happens to the uber riders there?

To make informed decisions, it's best to look at how often riders request for support both within and outside NYC and also revise the goals of uber. 

If uber wants to stick to providing services only within NYC, then there's no need to have base stations outside NYC. 

However, if the request for support is high, then it's best to register more base stations outside NYC so that the riders would be satistfied which also increases the number of riders through referrals and indirectly increases the number of those that would also request for rides. 

In this case, it's a win-win for everyone. 

##  Conclusion/Recommendations

This is a quick reminder that this data is data from 2014. Based on the findings from this data, Uber shouldn't rush into decisions to increase the awareness of their services. 

Seeing that their services is in a crucial industry as Transportation, they should ensure that decisions made should be in not just their favour but also the favour of the masses by considering factors like population, population density, request for support from riders, economic state of regions in the US, and many other factors. 

Lastly, the decisions made should not put them in the tight spot with government regarding traffic congestion and security of customers. 

## Youtube Video

In the youtube video below, I showed how to load and query data from Amazon redshift. You can click it to watch.

[![](Youtube%20Thumbnail.png)](https://youtu.be/UaCwUbjDtyE)










