Mobilize-Hive
===============

Mobilize-Hive adds the power of hive to [mobilize-hdfs][mobilize-hdfs].
* read, write, and copy hive files through Google Spreadsheets.

Table Of Contents
-----------------
* [Overview](#section_Overview)
* [Install](#section_Install)
  * [Mobilize-Hive](#section_Install_Mobilize-Hive)
  * [Install Dirs and Files](#section_Install_Dirs_and_Files)
* [Configure](#section_Configure)
  * [Hive](#section_Configure_Hive)
* [Start](#section_Start)
  * [Create Job](#section_Start_Create_Job)
  * [Run Test](#section_Start_Run_Test)
* [Meta](#section_Meta)
* [Special Thanks](#section_Special_Thanks)
* [Author](#section_Author)

<a name='section_Overview'></a>
Overview
-----------

* Mobilize-hive adds Hive methods to mobilize-hdfs.

<a name='section_Install'></a>
Install
------------

Make sure you go through all the steps in the
[mobilize-base][mobilize-base], 
[mobilize-ssh][mobilize-ssh],
[mobilize-hdfs][mobilize-hdfs],
install sections first.

<a name='section_Install_Mobilize-Hive'></a>
### Mobilize-Hive

add this to your Gemfile:

``` ruby
gem "mobilize-hive"
```

or do

  $ gem install mobilize-hive

for a ruby-wide install.

<a name='section_Install_Dirs_and_Files'></a>
### Dirs and Files

### Rakefile

Inside the Rakefile in your project's root dir, make sure you have:

``` ruby
require 'mobilize-base/tasks'
require 'mobilize-ssh/tasks'
require 'mobilize-hdfs/tasks'
require 'mobilize-hive/tasks'
```

This defines rake tasks essential to run the environment.

### Config Dir

run

  $ rake mobilize_hive:setup

This will copy over a sample hive.yml to your config dir.

<a name='section_Configure'></a>
Configure
------------

<a name='section_Configure_Hive'></a>
### Configure Hive

* Hive is big data. That means we need to be careful when reading from
the cluster as it could easily fill up our mongodb instance, RAM, local disk
space, etc.
* To achieve this, all hive operations, stage outputs, etc. are
executed and stored on the cluster only. 
  * The exceptions are:
    * writing to the cluster from an external source, such as a google
sheet. Here there
is no risk as the external source has much more strict size limits than
hive.
    * reading from the cluster, such as for posting to google sheet. In
this case, the read_limit parameter dictates the maximum amount that can
be read. If the data is bigger than the read limit, an exception will be
raised.

The Hive configuration consists of:
* clusters - this defines aliases for clusters, which are used as
parameters for Hive stages. They should have the same name as those
in hadoop.yml. Each cluster has:
  * max_slots - defines the total number of simultaneous slots to be
    used for hive jobs on this cluster
  * output_db - defines the db which should be used to hold stage outputs.
    * This db must have open permissions (777) so any user on the system can
write to it -- the tables inside will be owned by the users themselves.
  * exec_path - defines the path to the hive executable

Sample hive.yml:

``` yml
---
development:
  clusters:
    dev_cluster:
      max_slots: 5
      output_db: mobilize
      exec_path: /path/to/hive
test:
  clusters:
    test_cluster:
      max_slots: 5
      output_db: mobilize
      exec_path: /path/to/hive
production:
  clusters:
    prod_cluster:
      max_slots: 5
      output_db: mobilize
      exec_path: /path/to/hive
```

<a name='section_Start'></a>
Start
-----

<a name='section_Start_Create_Job'></a>
### Create Job

* For mobilize-hive, the following stages are available. 
  * cluster and user are optional for all of the below.
    * cluster defaults to the first cluster listed;
    * user is treated the same way as in [mobilize-ssh][mobilize-ssh].
  * params are also optional for all of the below. They replace HQL in sources.
    * params are passed as a YML or JSON, as in:
      * `hive.run source:<source_path>, params:{'date': '2013-03-01', 'unit': 'widgets'}`
        * this example replaces all the keys, preceded by '@' in all source hqls with the value.
          * The preceding '@' is used to keep from replacing instances
            of "date" and "unit" in the HQL; you should have `@date` and `@unit` in your actual HQL 
            if you'd like to replace those tokens.
    * in addition, the following params are substituted automatically:
      * `$utc_date` - replaced with YYYY-MM-DD date, UTC
      * `$utc_time` - replaced with HH:MM time, UTC
      * any occurrence of these values in HQL will be replaced at runtime.
  * hive.run `hql:<hql> || source:<gsheet_path>, user:<user>, cluster:<cluster>`, which executes the
      script in the hql or source sheet and returns any output specified at the
      end. If the cmd or last query in source is a select statement, column headers will be
      returned as well.
  * hive.write `hql:<hql> || source:<source_path>, target:<hive_path>, partitions:<partition_path>, user:<user>, cluster:<cluster>, schema:<gsheet_path>, drop:<true/false>`, 
      which writes the source or query result to the selected hive table.
    * hive_path 
      * should be of the form `<hive_db>/<table_name>` or `<hive_db>.<table_name>`.  
    * source:
      * can be a gsheet_path, hdfs_path, or hive_path (no partitions)
      * for gsheet and hdfs path, 
        * if the file ends in .*ql, it's treated the same as passing hql
        * otherwise it is treated as a tsv with the first row as column headers
    * target:
      * Should be a hive_path, as in `<hive_db>/<table_name>` or `<hive_db>.<table_name>`.
    * partitions: 
      * Due to Hive limitation, partition names CANNOT be reserved keywords when writing from tsv (gsheet or hdfs source)
      * Partitions should be specified as a path, as in  partitions:`<partition1>/<partition2>`.
    * schema:
      * optional. gsheet_path to column schema. 
        * two columns: name, datatype
        * Any columns not defined here will receive "string" as the datatype
        * partitions can have their datatypes overridden here as well
        * columns named here that are not in the dataset will be ignored
    * drop:
      * optional. drops the target table before performing write
      * defaults to false

<a name='section_Start_Run_Test'></a>
### Run Test

To run tests, you will need to 

1) go through [mobilize-base][mobilize-base], [mobilize-ssh][mobilize-ssh], [mobilize-hdfs][mobilize-hdfs] tests first

2) clone the mobilize-hive repository 

From the project folder, run

3) $ rake mobilize_hive:setup

Copy over the config files from the mobilize-base, mobilize-ssh,
mobilize-hdfs projects into the config dir, and populate the values in the hive.yml file.

Make sure you use the same names for your hive clusters as you do in
hadoop.yml.

3) $ rake test

* The test runs these jobs:
  * hive_test_1:
    * `hive.write target:"mobilize/hive_test_1/act_date",source:"Runner_mobilize(test)/hive_test_1.in", schema:"hive_test_1.schema", drop:true`
    * `hive.run source:"hive_test_1.hql"`
    * `hive.run cmd:"show databases"`
    * `gsheet.write source:"stage2", target:"hive_test_1_stage_2.out"`
    * `gsheet.write source:"stage3", target:"hive_test_1_stage_3.out"`
    * hive_test_1.hql runs a select statement on the table created in the
      write command.
    * at the end of the test, there should be two sheets, one with a
        sum of the data as in your write query, one with the results of the show
        databases command.
  * hive_test_2:
    * `hive.write source:"hdfs://user/mobilize/test/test_hdfs_1.out", target:"mobilize.hive_test_2", drop:true`
    * `hive.run cmd:"select * from mobilize.hive_test_2"`
    * `gsheet.write source:"stage2", target:"hive_test_2.out"`
    * this test uses the output from the first hdfs test as an input, so make sure you've run that first.
  * hive_test_3:
    * `hive.write source:"hive://mobilize.hive_test_1",target:"mobilize/hive_test_3/date/product",drop:true`
    * `hive.run hql:"select act_date as ```date```,product,category,value from mobilize.hive_test_1;"`
    * `hive.write source:"stage2",target:"mobilize/hive_test_3/date/product", drop:false`
    * `gsheet.write source:"hive://mobilize/hive_test_3", target:"hive_test_3.out"`


<a name='section_Meta'></a>
Meta
----

* Code: `git clone git://github.com/dena/mobilize-hive.git`
* Home: <https://github.com/dena/mobilize-hive>
* Bugs: <https://github.com/dena/mobilize-hive/issues>
* Gems: <http://rubygems.org/gems/mobilize-hive>

<a name='section_Special_Thanks'></a>
Special Thanks
--------------
* This release goes to Toby Negrin, who championed this project with
DeNA and gave me the support to get it properly architected, tested, and documented.
* Also many thanks to the Analytics team at DeNA who build and maintain
our Big Data infrastructure. 

<a name='section_Author'></a>
Author
------

Cassio Paes-Leme :: cpaesleme@dena.com :: @cpaesleme

[mobilize-base]: https://github.com/dena/mobilize-base
[mobilize-ssh]: https://github.com/dena/mobilize-ssh
[mobilize-hdfs]: https://github.com/dena/mobilize-hdfs
