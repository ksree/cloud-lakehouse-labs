# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Ensuring Governance and security for our C360 lakehouse
# MAGIC
# MAGIC Data governance and security is hard when it comes to a complete Data Platform. SQL GRANT on tables isn't enough and security must be enforced for multiple data assets (dashboards, Models, files etc).
# MAGIC
# MAGIC To reduce risks and driving innovation, Emily's team needs to:
# MAGIC
# MAGIC - Unify all data assets (Tables, Files, ML models, Features, Dashboards, Queries)
# MAGIC - Onboard data with multiple teams
# MAGIC - Share & monetize assets with external Organizations
# MAGIC
# MAGIC <style>
# MAGIC .box{
# MAGIC   box-shadow: 20px -20px #CCC; height:300px; box-shadow:  0 0 10px  rgba(0,0,0,0.3); padding: 5px 10px 0px 10px;}
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
# MAGIC .badge_b { 
# MAGIC   height: 35px}
# MAGIC </style>
# MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
# MAGIC <div style="padding: 20px; font-family: 'DM Sans'; color: #1b5162">
# MAGIC   <div style="width:200px; float: left; text-align: center">
# MAGIC     <div class="box" style="">
# MAGIC       <div style="font-size: 26px;">
# MAGIC         <strong>Team A</strong>
# MAGIC       </div>
# MAGIC       <div style="font-size: 13px">
# MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/da.png" style="" width="60px"> <br/>
# MAGIC         Data Analysts<br/>
# MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/ds.png" style="" width="60px"> <br/>
# MAGIC         Data Scientists<br/>
# MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="" width="60px"> <br/>
# MAGIC         Data Engineers
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="box" style="height: 80px; margin: 20px 0px 50px 0px">
# MAGIC       <div style="font-size: 26px;">
# MAGIC         <strong>Team B</strong>
# MAGIC       </div>
# MAGIC       <div style="font-size: 13px">...</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="float: left; width: 400px; padding: 0px 20px 0px 20px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on queries, dashboards</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on tables, columns, rows</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on features, ML models, endpoints, notebooks…</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on files, jobs</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC   </div>
# MAGIC   
# MAGIC   <div class="box" style="width:550px; float: left">
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/gov.png" style="float: left; margin-right: 10px;" width="80px"> 
# MAGIC     <div style="float: left; font-size: 26px; margin-top: 0px; line-height: 17px;"><strong>Emily</strong> <br />Governance and Security</div>
# MAGIC     <div style="font-size: 18px; clear: left; padding-top: 10px">
# MAGIC       <ul style="line-height: 2px;">
# MAGIC         <li>Central catalog - all data assets</li>
# MAGIC         <li>Data exploration & discovery to unlock new use-cases</li>
# MAGIC         <li>Permissions cross-teams</li>
# MAGIC         <li>Reduce risk with audit logs</li>
# MAGIC         <li>Measure impact with lineage</li>
# MAGIC       </ul>
# MAGIC       + Monetize & Share data with external organization (Delta Sharing)
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   
# MAGIC   
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Implementing a global data governance and security with Unity Catalog
# MAGIC
# MAGIC <img style="float: right; margin-top: 30px" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-2.png" />
# MAGIC
# MAGIC Let's see how the Lakehouse can solve this challenge leveraging Unity Catalog.
# MAGIC
# MAGIC Our Data has been saved as Delta Table by our Data Engineering team.  The next step is to secure this data while allowing cross team to access it. <br>
# MAGIC A typical setup would be the following:
# MAGIC
# MAGIC * Data Engineers / Jobs can read and update the main data/schemas (ETL part)
# MAGIC * Data Scientists can read the final tables and update their features tables
# MAGIC * Data Analyst have READ access to the Data Engineering and Feature Tables and can ingest/transform additional data in a separate schema.
# MAGIC * Data is masked/anonymized dynamically based on each user access level
# MAGIC
# MAGIC This is made possible by Unity Catalog. When tables are saved in the Unity Catalog, they can be made accessible to the entire organization, cross-workpsaces and cross users.
# MAGIC
# MAGIC Unity Catalog is key for data governance, including creating data products or organazing teams around datamesh. It brings among other:
# MAGIC
# MAGIC * Fined grained ACL
# MAGIC * Audit log
# MAGIC * Data lineage
# MAGIC * Data exploration & discovery
# MAGIC * Sharing data with external organization (Delta Sharing)
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Flakehouse_churn%2Fuc&dt=LAKEHOUSE_RETAIL_CHURN">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exploring our Customer360 database
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
# MAGIC
# MAGIC Let's review the data created.
# MAGIC
# MAGIC Unity Catalog works with 3 layers:
# MAGIC
# MAGIC * CATALOG
# MAGIC * SCHEMA (or DATABASE)
# MAGIC * TABLE
# MAGIC
# MAGIC All unity catalog is available with SQL (`CREATE CATALOG IF NOT EXISTS my_catalog` ...)
# MAGIC
# MAGIC To access one table, you can specify the full path: `SELECT * FROM &lt;CATALOG&gt;.&lt;SCHEMA&gt;.&lt;TABLE&gt;`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Let's review the tables we created under our schema
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-data-explorer.gif" style="float: right" width="800px"/> 
# MAGIC
# MAGIC Unity Catalog provides a comprehensive Data Explorer that you can access on the left menu.
# MAGIC
# MAGIC You'll find all your tables, and can use it to access and administrate your tables.
# MAGIC
# MAGIC They'll be able to create extra table into this schema.
# MAGIC
# MAGIC ### Discoverability 
# MAGIC
# MAGIC In addition, Unity catalog also provides explorability and discoverability. 
# MAGIC
# MAGIC Anyone having access to the tables will be able to search it and analyze its main usage. <br>
# MAGIC You can use the Search menu (⌘ + P) to navigate in your data assets (tables, notebooks, queries...)

# COMMAND ----------

# MAGIC %run ./includes/SetupLab

# COMMAND ----------

spark.sql("use catalog main")
spark.sql("use database "+databaseName)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This lab has been set up to use a specific catalog, if it has been set up, or "main". 
# MAGIC SELECT CURRENT_CATALOG();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shows the schemas (databases) in the the current catalog
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shows the tables in the current database
# MAGIC SHOW TABLES;

# COMMAND ----------

# DBTITLE 1,Give access to your schema to other users / or groups
# MAGIC %sql
# MAGIC -- FILL IN <SCHEMA> and <TABLE>
# MAGIC GRANT USE SCHEMA ON SCHEMA  <SCHEMA> TO `account users`;
# MAGIC --GRANT SELECT ON TABLE <SCHEMA>.<TABLE> TO `account users`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## The table was created without restriction, all users can access all the ![rows](path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM churn_users

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirming that we can see all countries (FR, USA, SPAIN) prior to setting up row-filters:
# MAGIC SELECT DISTINCT(country) FROM churn_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0 Implementing row-level security on our table
# MAGIC
# MAGIC  row-level security allows you to automatically hide rows in your table from users, based on their identity or group assignment.
# MAGIC
# MAGIC Lets see how you can enforce a policy where an analyst can only access data related to customers in their country.
# MAGIC
# MAGIC To capture the current user and check their membership to a particular group, Databricks provides you with 2 built-in functions:
# MAGIC
# MAGIC - current_user()
# MAGIC - and is_account_group_member()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- get the current user (for informational purposes)
# MAGIC SELECT current_user(), is_account_group_member('account users');
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1. Define the access rule
# MAGIC  To declare an access control rule, you will need to create a SQL function that returns a boolean. Unity Catalog will then hide the row if the function returns False.
# MAGIC
# MAGIC Inside your SQL function, you can define different conditions and implement complex logic to create this boolean return value. (e.g : IF(condition)-THEN(view)-ELSE)
# MAGIC
# MAGIC Here, we will apply the following logic :
# MAGIC
# MAGIC if the user is a bu_admin group member, then they can access data from all countries. (we will use is_account_group_member('group_name') we saw earlier)
# MAGIC if the user is not a bu_admin group member, we'll restrict access to only the rows pertaining to regions US as our default regions. All other customers will be hidden!
# MAGIC Note that columns within whatever table that this function will be applied on, can also be referred to inside the function's conditions. You can do so by using parameters.

# COMMAND ----------

# DBTITLE 1,Create a SQL function for a simple row-filter:
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION region_filter(region_param STRING) 
# MAGIC RETURN 
# MAGIC   is_account_group_member('bu_admin') or  -- bu_admin can access all regions
# MAGIC   region_param like "US%";                -- non bu_admin's can only access regions containing US
# MAGIC
# MAGIC -- Grant access to all users to the function for the demo by making all account users owners.  Note: Don't do this in production!
# MAGIC GRANT ALL PRIVILEGES ON FUNCTION region_filter TO `account users`; 
# MAGIC
# MAGIC -- Let's try our filter. As expected, we can access USA but not SPAIN.
# MAGIC SELECT region_filter('USA'), region_filter('SPAIN')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2. Apply the access rule
# MAGIC
# MAGIC With our rule function declared, all that's left to do is apply it on a table and see it in action! A simple SET ROW FILTER followed by a call to the function is all it takes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply access rule to customers table.
# MAGIC -- country will be the column sent as parameter to our SQL function (region_param)
# MAGIC ALTER TABLE churn_users SET ROW FILTER region_filter ON (country);

# COMMAND ----------

# DBTITLE 1,Confirm only customers in USA are visible:
# MAGIC %sql
# MAGIC SELECT * FROM churn_users

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We should see only USA and Canada here, unless the user is a member of bu_admin:
# MAGIC SELECT DISTINCT(country) FROM churn_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ### This is working as expected!
# MAGIC
# MAGIC
# MAGIC We secured our table, and dynamically filter the results to only keep rows with country=USA.
# MAGIC
# MAGIC Let's drop the current filter, and demonstrate a more dynamic version.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE churn_users DROP ROW FILTER;
# MAGIC -- Confirming that we can once again see all countries:
# MAGIC SELECT DISTINCT(country) FROM churn_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 More advanced dynamic filters.
# MAGIC Let's imagine we have a few regional user groups defined as : ANALYST_USA, ANALYST_SPAIN, etc... and we want to use these groups to dynamically filter on a country value.
# MAGIC
# MAGIC This can easily be done by checking the group based on the region value.

# COMMAND ----------

# DBTITLE 1,test cell
# MAGIC %sql
# MAGIC -- If this request is failing, you're missing some groups. Make sure you are part of ANALYST_USA and not bu_admin!
# MAGIC SELECT 
# MAGIC   assert_true(is_account_group_member('account users'),      'You must be part of account users for this demo'),
# MAGIC   assert_true(is_account_group_member('ANALYST_USA'),        'You must be part of ANALYST_USA for this demo'),
# MAGIC   assert_true(not is_account_group_member('ANALYST_FR'),       'You must NOT be part of bu_admin for this demo');
# MAGIC
# MAGIC -- Cleanup any row-filters or masks that may have been added in previous runs of the demo
# MAGIC ALTER TABLE churn_users DROP ROW FILTER;
# MAGIC ALTER TABLE churn_users ALTER COLUMN address DROP MASK;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION region_filter_dynamic(country_param STRING) 
# MAGIC RETURN 
# MAGIC   is_account_group_member('bu_admin') or                           -- bu_admin can access all regions
# MAGIC   is_account_group_member(CONCAT('ANALYST_', country_param)); --regional admins can access only if the region (country column) matches the regional admin group suffix.
# MAGIC   
# MAGIC GRANT ALL PRIVILEGES ON FUNCTION region_filter_dynamic TO `account users`; --only for demo, don't do that in prod as everybody could change the function
# MAGIC
# MAGIC -- apply the new access rule to the customers table:
# MAGIC ALTER TABLE churn_users SET ROW FILTER region_filter_dynamic ON (country);
# MAGIC
# MAGIC SELECT region_filter_dynamic('USA'), region_filter_dynamic('SPAIN')

# COMMAND ----------

# DBTITLE 1,Check the rule. We can only access USA
# MAGIC %sql
# MAGIC -- Since our current user is a member of ANALYST_USA, now they see only USA from our query:
# MAGIC SELECT DISTINCT(country) FROM churn_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Column-level access control
# MAGIC ## 
# MAGIC
# MAGIC Declaring a rule to implement column-level access control is very similar to what we did earlier for our row-level access control rule.
# MAGIC
# MAGIC In this example, we'll create a SQL function with the following IF-THEN-ELSE logic:
# MAGIC
# MAGIC if the current user is member of the group bu_admin, then return the column value as-is (here ssn),
# MAGIC if not, mask it completely with a constant string (here ****)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create a SQL function for a simple column mask:
# MAGIC CREATE OR REPLACE FUNCTION simple_mask(column_value STRING)
# MAGIC RETURN 
# MAGIC   IF(is_account_group_member('bu_admin'), column_value, "****");
# MAGIC    
# MAGIC GRANT ALL PRIVILEGES ON FUNCTION simple_mask TO `account users`; --only for demo, don't do that in prod as everybody could change the function

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2. Apply the access rule
# MAGIC ## 
# MAGIC To change things a bit, instead of applying a rule on an existing table, we'll demonstrte here how we can apply a rule upon the creation of a new table.
# MAGIC
# MAGIC Note: In this demo we have only one column mask function to apply. In real life, you may want to apply different column masks on different columns within the same table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- applying our simple masking function to the 'address' column in the 'customers' table:
# MAGIC ALTER TABLE
# MAGIC   churn_users
# MAGIC ALTER COLUMN
# MAGIC   address
# MAGIC SET
# MAGIC   MASK simple_mask;

# COMMAND ----------

# DBTITLE 1,🥷 Confirm Addresses have been masked
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM churn_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Change the definition of the access control rules
# MAGIC If the business ever decides to change a rule's conditions or the way they want the data to be returned in response to these conditions, it is easy to adapt with Unity Catalog.
# MAGIC
# MAGIC Since the function is the central element, all you need to do is update it and the effects will automatically be reflected on all the tables that it has been attached to.
# MAGIC
# MAGIC In this example, we'll rewrite our simple_mask column mask function and change the way we anonymse data from the rather simplistic ****, to using the built-in sql MASK function (see documentation)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Updating the existing simple_mask function to provide more advanced masking options via the built-in SQL MASK function:
# MAGIC CREATE OR REPLACE FUNCTION simple_mask(maskable_param STRING)
# MAGIC    RETURN 
# MAGIC       IF(is_account_group_member('bu_admin'), maskable_param, MASK(maskable_param, '*', '*'));
# MAGIC       -- You can also create custom mask transformations, such as concat(substr(maskable_param, 0, 2), "..."))
# MAGIC    
# MAGIC  -- grant access to all user to the function for the demo  
# MAGIC ALTER FUNCTION simple_mask OWNER TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM churn_users;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Going further with Data governance & security
# MAGIC
# MAGIC By bringing all your data assets together, Unity Catalog let you build a complete and simple governance to help you scale your teams.
# MAGIC
# MAGIC Unity Catalog can be leveraged from simple GRANT to building a complete datamesh organization.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/lineage/lineage-table.gif" style="float: right; margin-left: 10px"/>
# MAGIC
# MAGIC ### Fine-grained ACL
# MAGIC
# MAGIC Need more advanced control? You can chose to dynamically change your table output based on the user permissions.
# MAGIC
# MAGIC ### Secure external location (S3/ADLS/GCS)
# MAGIC
# MAGIC Unity Catatalog let you secure your managed table but also your external locations.
# MAGIC
# MAGIC ### Lineage 
# MAGIC
# MAGIC UC automatically captures table dependencies and let you track how your data is used, including at a row level.
# MAGIC
# MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
# MAGIC
# MAGIC
# MAGIC ### Audit log
# MAGIC
# MAGIC UC captures all events. Need to know who is accessing which data? Query your audit log.
# MAGIC
# MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
# MAGIC
# MAGIC ### Upgrading to UC
# MAGIC
# MAGIC Already using Databricks without UC? Upgrading your tables to benefit from Unity Catalog is simple.
# MAGIC
# MAGIC ### Sharing data with external organization
# MAGIC
# MAGIC Sharing your data outside of your Databricks users is simple with Delta Sharing, and doesn't require your data consumers to use Databricks.
