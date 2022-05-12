# Data Engineer Take-home Assignment  
A take-home assignent for DE profiles interviewing with the Analytics &amp; Automation team

## Introduction

The goal of this take-home assignment is for us to get a better understanding of 
your technical skill set and how you approach and solve a task.

The assignment is intended to be worked on for no more than 2 hours.
Feel free to stop working on the assignment if takes longer.
We can still have a fruitful discussion about your solution, even if not all parts of the instructions are implemented.

As this is an early iteration of the assignment, you may find that some details in the instructions are unclear or lacking. If that is the case, you can either:
1. Choose an interpretation of the instructions that allows you to move forward with your solution, and take a note of it.
2. Contact us for clarification


When we meet up (digitally or in person) to discuss your solution, we will be interested in hearing about your thought process while solving the problem. We will also go through the code together.  
Some example questions we might ask include:
- What was your over all impression of the assignment? Easy/Difficult, Clear/Unclear instructions 
- How did you start working on the assignment? 
- How did you motivate design or implementation decision X?
- In hindsight, would you have done something differently if you were to start fresh? 
## Instructions summary

In this assignment, you'll be working with the [Amazon Musical Instruments Reviews](https://www.kaggle.com/datasets/eswarchandt/amazon-music-reviews) dataset. 
The goal is to extract some insights from this dataset. To this end, we'd like you implement an ETL job using the AWS Glue service.  

The job should do the following:
1. Read the input dataset (available in either CSV or JSON format) into a PySpark DataFrame/Glue DynamicFrame. See [Input data](#input-data) section for more details.
2. Transform the data set according to the instructions provided in the section [Transform data](#transform)
3. Write the transformed dataset back to S3. Also ensure that two Glue tables are created, so that the tables can be queried via Athena. See [Output data](#output-data) section for more details. 

### Tips
- It is completely fine to trigger the Glue Job runs manually via the Glue Console, so you don't have to consider how the job will be scheduled/triggered.
- The dataset comes from Kaggle, but we have separate assignment instructions compared to what's stated on Kaggle. In other words, you can ignore the *Task* section on Kaggle.

## Input data

The dataset comes from Kaggle, and is available in both CSV and JSON format (choose the one you prefer). It has already been uploaded to S3 in both formats:

JSON: `s3://analytics-automation-take-home-assignment/input/Musical_Instruments_5.json`  
CSV: `s3://analytics-automation-take-home-assignment/input/Musical_instruments_reviews.csv`   

You can find more information about the [Amazon Musical Instruments Reviews](https://www.kaggle.com/datasets/eswarchandt/amazon-music-reviews) dataset on Kaggle. However, we have copied an excerpt of its description below, that should be sufficient for the purposes of this assignment:

>This file has reviewer ID , User ID, Reviewer Name, Reviewer text, helpful, Summary(obtained from Reviewer text),Overall Rating on a scale 5, Review time
>
>Description of columns in the file:
>
> - reviewerID - ID of the reviewer, e.g. A2SUAM1J3GNN3B
> - asin - ID of the product, e.g. 0000013714
> - reviewerName - name of the reviewer
> - helpful - helpfulness rating of the review, e.g. 2/3 users found the review helpful. It's a thumbs up / thumbs down rating of how helpful the review was, i.e. "X of Y users found this review helpful."
> - reviewText - text of the review
> - overall - rating of the product
> - summary - summary of the review
> - unixReviewTime - time of the review (unix time)
> - reviewTime - time of the review (raw)



**Note: The dataset comes from Kaggle, but we have separate assignment instructions compared to what's stated on Kaggle. In other words, you can ignore the *Task* section on Kaggle.**
## Transform data
Your task is to transform the input data into 2 tables, each described below.
The input data may or may not have duplicate rows, so please add some code to remove any duplicates rows that have the same values in the fields: `reviewerId`, `asin`, `reviewTime`.

#### **Table 1: Exploring time related trends among users reading reviews**

We'd like to know if the time of day affects whether users tend to like/dislike the reviews of the products (i.e. find them helpful or not).
To this end, we'd like to have a dataset that can answer the question: Are users more prone to find reviews helpful early in the daytime vs later in the evening?
To answer this question, we request a table with the following properties:  

- partitioned by `unixReviewTime` down to hourly granularity, e.g. 'YYYMMMDD HH'
- average `helpful` rating of each hourly partition 
- median `helpful` rating of each hourly partition 
- average `overall` rating of the product by product ID (asin), of each hourly partition
- median `overall` rating of the product by product ID (asin), of each hourly partition

#### **Table 2: Statistics by product**
We'd also like to know some summarizing statistics for each product, not considering time of day:

- average `overall` rating of the product by product ID (asin)
- median `overall` rating of the product by product ID (asin)
- number of unique reviewers of the product, using `reviewerId`
- average length of `summary` i.e. average number of characters in each product's reviews
- medianlength of `summary` i.e. median number of characters in each product's reviews

## Output data

Once you're done with the transformation step, the next step is to write the data to S3 as well as to a Glue database that we have created for you. Having the metadata written directly to the Glue DB allows us to query the tables using Athena without having to crawl in between.

The s3 prefix to combine with the suffix (given for each table):  
`s3://analytics-automation-take-home-assignment/` 

The glue database:  
`amzn-music-reviews-curated`  

You can create a new Glue table in `amzn-music-reviews-curated` directly from the Glue job, by using the `glueContext.getSink` method documented [here](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-glue-context.html#aws-glue-api-crawler-pyspark-extensions-glue-context-get-sink). Below is an example snippet of code:
```
    sink = glue_context.getSink(
        connection_type="s3",
        path=target_s3_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys,
        transformation_ctx="write_to_output_sink",
    )
    sink.setFormat("parquet", useGlueParquetWriter=True)
    sink.setCatalogInfo(
        catalogDatabase=target_database,
        catalogTableName=target_table)
    sink.writeFrame(dynamic_frame)
```
#### **Table 1: Exploring time related trends among users reading reviews**
We'd like this table to be partitioned in S3 by the `unixReview` time like this:  
`<s3-bucket-prefix>/output/table_1/<version>/<year>/<month>/<day>/<hour>`   

In other words, the s3 path suffix is `output/table_1/<version>/<year>/<month>/<day>/<hour>`, and you can hardcode `<version>` to `v1`, or bump the version as you please..  

The schema is provided below:

```
- version: string // partition column
- year: string // partition column
- month: string // partition column
- day: string // partition column
- hour: string // partition column
- average_helpful_rating: double 
- median_helpful_rating: double 
- average_overall_rating: double 
- median_overall_rating: double 
```
#### **Table 2: Statistics by product**
This table doesn't have to be partitioned in S3.  
Please write it to path: `<s3-bucket-prefix>/output/table_2/<version>`  
You can hardcode `<version>` to `v1`, or bump the version as you please.
The schema is provided below:
```
- product_id: string
- average_overall_rating: double 
- median_overall_rating: double 
- average `overall` rating of the product by product ID (asin)
- median `overall` rating of the product by product ID (asin)
- number of unique reviewers of the product, using `reviewerId`
- average length of `summary` i.e. average number of characters in each product's reviews
- medianlength of `summary` i.e. median number of characters in each product's reviews
```


#### **File format**

Choose any output file format that is supported by the AWS Glue service, so that we can query it with SQL using AWS Athena.

## AWS & Github
You will need access to an AWS account as well as this Github repository in order to complete the assignment.

### Github

This repository is public, but we'd like your implementation to live in a private repository. To do so, follow the instructions below.

**TLDR**

1. Do a bare clone of the public repo.
2. Create a new private one.
3. Do a mirror push to the new private one.

**More detailed instructions can be found on Stack Overflow**:   
 https://stackoverflow.com/a/30352360/9202803
  
Please contact us if these instructions are insufficient/not working :) 
### AWS
You should receive an email with credentials for an AWS User belonging to an AWS account that we have prepared.
Users with AWS Management Console access can sign-in at: https://<account-id>.signin.aws.amazon.com/console
## Submission
Please first read  the instructions in the [Github](#github) section.
When you are done with your assignment (or suspend it due to taking too much time), please make a pull-request back to the repository owned by Analytics & Automation. We will be automatically notified, and send you an email as response to confirm that we have received the PR.

## Contact
If you have any questions, don't hesitate to reach out to us :)  
Felix - felix.liljefors@postnord.com  
Amarnath - amarnath.das@postnord.com

## Changelog

2022-05-12: First version