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
  * max_slots - defines the total number of simultaneous slots to be used for hive jobs
  * temp_table_db - defines the db which should be used to hold temp
tables used in partition inserts.
  * exec_path - defines the path to the hive executable

Sample hive.yml:

``` yml
---
development:
  clusters:
    dev_cluster:
      max_slots: 5
      temp_table_db: mobilize
      exec_path: /path/to/hive
test:
  clusters:
    test_cluster:
      max_slots: 5
      temp_table_db: mobilize
      exec_path: /path/to/hive
production:
  clusters:
    prod_cluster:
      max_slots: 5
      temp_table_db: mobilize
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
  * hive.run `cmd:<hql> || source:<gsheet_path>, user:<user>, cluster:<cluster>`, which executes the
      script in the cmd or source sheet and returns any output specified at the
      end. If the cmd or last query in source is a select statement, column headers will be
      returned as well.
  * hive.write `source:<source_path>, target:<hive_path>, user:<user>, cluster:<cluster>, schema:<gsheet_path>`, 
      which writes the source sheet to the selected hive table.
    * hive_path 
      * should be of the form `<hive_db>/<table_name>` or `<hive_db>.<table_name>`.  
    * source:
      * can be a gsheet_path, hdfs_path, or hive_path (no partitions)
      * for gsheet and hdfs path, first row is used for column headers
    * target:
      * Partitions can optionally be added to the hive_path, as in `<hive_db>/<table_name>/<partition1>/<partition2>`. 
    * schema:
      * optional. gsheet_path to column schema. 
        * two columns: name, datatype
        * Any columns not defined here will receive "string" as the datatype
        * partitions are considered columns for this purpose
        * columns named here that are not in the dataset will be ignored 

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
    * `hive.write target:"mobilize/hive_test_1/date/product",source:"Runner_mobilize(test)/hive_test_1.in", schema:"hive_test_1.schema`
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
    * `hive.write source:"hdfs://user/mobilize/test/test_hdfs_1.out", target:"mobilize.hive_test_2"`
    * `hive.run cmd:"select * from mobilize.hive_test_2"`
    * `gsheet.write source:"stage2", target:"hive_test_2.out"`
    * this test uses the output from the first hdfs test as an input, so
make sure you've run that first.
  * hive_test_3:
    * `hive.write source:"hive://mobilize.hive_test_1", target:"mobilize/hive_test_1_copy/date/product"`
    * `gsheet.write source:"hive://mobilize.hive_test_1_copy", target:"hive_test_3.out"`


<a name='section_Meta'></a>
Meta
----

* Code: `git clone git://github.com/ngmoco/mobilize-hive.git`
* Home: <https://github.com/ngmoco/mobilize-hive>
* Bugs: <https://github.com/ngmoco/mobilize-hive/issues>
* Gems: <http://rubygems.org/gems/mobilize-hive>

<a name='section_Author'></a>
Author
------

Cassio Paes-Leme :: cpaesleme@ngmoco.com :: @cpaesleme

[mobilize-base]: https://github.com/ngmoco/mobilize-base
[mobilize-ssh]: https://github.com/ngmoco/mobilize-ssh
