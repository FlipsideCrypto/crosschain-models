## How to Add Tags to Flipside's Data
----
There are 3 ways to add tags to our data!

Be sure to [read our docs on Tags](https://docs.flipsidecrypto.com/our-data/data-models/tags).

#### <ins>1. Add a SQL statement to our GitHub</ins>

You can use a Flipside query to create a tag set that will run on a reoccurring basis. This is a very powerful and scalable way to create a dynamic tag set that can update regularly. 

To submit a Flipside query for tagging:
  - [Submit a Github issue](https://github.com/FlipsideCrypto/crosschain-models/issues/new) on this repo. 
  - Title the issue: "Tags: " and the name of the tag(s) you wish to include in our data model. 
  - In the comment section, paste the query SQL that can be run to isolate the addresses you want to tag (also include a link to the query in the Flipside app). 
  - Assign the issue to gronders for review.
  - Submit!

We will review your query and get back to you if there are any questions or changes. 

Your tags query must return these 7 columns:

  Column Name | Data type | Description
  --- | --- | --- 
  blockchain | string | The blockchain that the address belongs to.
  creator | string | Who created the tag. Use your Flipside username, shown in your Flipside profile URL, for tags you create.
  address | string | The address of the contract or wallet the tag describes.
  tag_name | string | Tag name (sub-category).
  tag_type | string | Tag type (high-level category).
  start_date | timestamp | Date the tag first applies. For tags that are permanent, this might be the date the address had its first behavior that warrants its tag, or the addresses' first transaction (e.g. if the tag identifies a celebrity NFT address).
  end_date | timestamp | Date the tag no longer applies (for tags that are permanent or currently active, end_date can be NULL).

#### <ins>2. Add a DBT seed file to our GitHub</ins>

If you have a static list of addresses that need a tag, a DBT seed file is the best route. This is the most efficient method to tag a list of addresses that will not change and don't rely on a SQL query. 

In order to submit a DBT seed file, we will be using a Pull Request (PR). Please see the [docs](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request) on how to create a pull request!
Once you are familiar with PR's, to add your tags:
  - Create your DBT seed file. (A DBT seed file is a csv file with the same column names as our table that can be uploaded via DBT.) 
  - Place the seed file within the ```data/``` folder of the repo. 
  - Create your PR to add your seed file to our data model. 
  - Assign the PR to gronders for review.

We will review your seed file and get back to you if there are any questions or changes.  

Your seed file must contain these 7 columns:

  Column Name | Data type | Description
  --- | --- | --- 
  blockchain | string | The blockchain that the address belongs to.
  creator | string | Who created the tag. Use your Flipside username, shown in your Flipside profile URL, for tags you create.
  address | string | The address of the contract or wallet the tag describes.
  tag_name | string | Tag name (sub-category).
  tag_type | string | Tag type (high-level category).
  start_date | timestamp | Date the tag first applies. For tags that are permanent, this might be the date the address had its first behavior that warrants its tag, or the addresses' first transaction (e.g. if the tag identifies a celebrity NFT address).
  end_date | timestamp | Date the tag no longer applies (for tags that are permanent or currently active, end_date can be NULL).

#### <ins>3. I know what I want but I don't know how to tag</ins>

Flipside has a very active community and extraordinarily helpful employees. Reach out to the community, or to @gto, in Discord and someone will help you set up your tags. 




## How to Set Up a DBT Profile for this Repo
This info is for contributors who plan to use [DBT](https://docs.getdbt.com/docs/introduction) to contribute to Flipside's data models. A DBT profile is _not_ required to add tags via a seed file (to add tags, follow the instructions above).

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

### DBT Learning Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
