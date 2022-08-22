## User Added Tags
----
There are 3 ways to add tags to our data!

#### <u>1. Add a SQL statement to our GitHub</u>

You can use a Flipside query to create a tag set that will run on a reoccurring basis. This is a very powerful and scalable way to create a dynamic tag set that can update regularly. 

In order to submit a flipside query for tagging:
  - [Submit a github issue](https://github.com/FlipsideCrypto/crosschain-models/issues/new) on this repo. 
  - Title the issue: "Tags: " and the name of the tag(s) you wish to include in our data model. 
  - In the comment section, paste the query that can be run to isolate the addresses you want to tag. 
  - Assign the issue to gronders for review.
  - Submit!

We will review your code and get back to you if there are any questions or modifications to your code. 
Please make sure your table includes a valid:
  - blockchain
  - creator (your name, or identifying name, NOT flipside)
  - address
  - tag_name
  - tag_type
  - start_date
  - end_date

#### <u>2. Add a DBT seed file to our GitHub</u>

If you have a static list of addresses that need a tag, a DBT seed file is the best route. This is the most efficient method to tag a list of addresses that will not change and don't rely on a SQL query. 

In order to submit a DBT seed file, we will be using a Pull Request (PR). Please see the [docs](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request) on how to create a pull request!
Once you are familiar with PR's, to add your tags:
  - Create your DBT seed file. (A DBT seed file is a csv file with the same column names as our table that can be uploaded via DBT.) 
  - Place the seed file within the ```data/``` folder of the repo. 
  - Create your PR to add your seed file to our data model. 
  - Assign the PR to gronders for review.

We will review your seed file and get back to you if there are any questions or modifications that are necessary.  
Please make sure your table includes a valid:
  - blockchain
  - creator (your name, or identifying name, NOT flipside)
  - address
  - tag_name
  - tag_type
  - start_date
  - end_date

#### <u>3. I know what I want but I don't know how to tag</u>

Flipside has a very active community and extraordinarily helpful employees. Reach out to the community, or to @gto, in Discord and someone will help you set up your tags. 


#### Use the following within profiles.yml 
----

```yml
crosschain:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <ACCOUNT>
      role: <ROLE>
      user: <USERNAME>
      password: <PASSWORD>
      region: <REGION>
      database: CROSSCHAIN_DEV
      warehouse: <WAREHOUSE>
      schema: silver
      threads: 4
      client_session_keep_alive: False
      query_tag: <TAG>
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices