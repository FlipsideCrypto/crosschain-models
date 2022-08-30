---
name: 'Add tag(s) '
about: Describe this issue template's purpose here.
title: ''
labels: ''
assignees: ''

---

When submitting, please include 3 itmes:
  - Brief description of your tag (what are you trying to tag)?
  - What is the cadence of refresh this needs?
  - SQL query. Reminder, your SQL MUST include:

  Column Name | Description
  --- | --- 
  blockchain | The blockchain that the address belongs to.
  creator | Who created the tag. Use your Flipside username, shown in your Flipside profile URL, for tags you create.
  address | The address of the contract or wallet the tag describes.
  tag_name | Tag name (sub-category).
  tag_type | Tag type (high-level category).
  start_date | Date the tag first applies. For tags that are permanent, this might be the date the address had its first behavior that warrants its tag, or the addresses' first transaction (e.g. if the tag identifies a celebrity NFT address).
  end_date | Date the tag no longer applies (for tags that are permanent or currently active, end_date can be NULL).
