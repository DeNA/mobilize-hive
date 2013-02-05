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
  * exec_path - defines the path to the hive executable

Sample hive.yml:

``` yml
---
development:
  clusters:
    dev_cluster:
      exec_path: /path/to/hive
test:
  clusters:
    test_cluster:
      exec_path: /path/to/hive
production:
  clusters:
    prod_cluster:
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
  * hive.run `source:<gsheet_path>, user:<user>, cluster:<cluster>`, which executes the
      script in the source sheet and returns any output specified at the
      end. If the last query is a select statement, column headers will be
      returned as well.
  * hive.write `source:<gsheet_path>, target:<hive_path> user:<user>, cluster:<cluster>`, 
      which writes the source sheet to the selected hive table. 
  * The hive_path should be of the form `<hive_db>/<table_name>`,
or `<hive_db>.<table_name>`.  Partitions can optionally be added to the
path, as in `<hive_db>/<table_name>/<partition1>/<partition2>`
  * The hive_full_path is the cluster alias followed by full path on the cluster. 
    * if a full path is supplied without a preceding cluster alias (e.g. "/user/mobilize/test/test_hive_1.in"), 
      the output cluster will be used.
    * The test uses "/user/mobilize/test/test_hive_1.in" for the initial
write, then "test_cluster_2/user/mobilize/test/test_hive_copy.out" for
the copy and subsequent read.
  * both cluster arguments and user are optional. If copying from
one cluster to another, your source_cluster gateway_node must be able to
access both clusters.

<a name='section_Start_Run_Test'></a>
### Run Test

To run tests, you will need to 

1) go through the [mobilize-base][mobilize-base] and [mobilize-ssh][mobilize-ssh] tests first

2) clone the mobilize-hive repository 

From the project folder, run

3) $ rake mobilize_hive:setup

Copy over the config files from the mobilize-base and mobilize-ssh
projects into the config dir, and populate the values in the hadoop.yml file.

If you don't have two clusters, you can populate test_cluster_2 with the
same cluster as your first.

3) $ rake test

* The test runs a 4 stage job:
  * test_hive_1:
    * `hive.write target:"/user/mobilize/test/test_hive_1.out", source:"Runner_mobilize(test)/test_hive_1.in"`
    * `hive.copy source:"/user/mobilize/test/test_hive_1.out",target:"test_cluster_2/user/mobilize/test/test_hive_1_copy.out"`
    * `hive.read source:"/user/mobilize/test/test_hive_1_copy.out"`
    * `gsheet.write source:"stage3", target:"Runner_mobilize(test)/test_hive_1_copy.out"`
  * at the end of the test, there should be a sheet named "test_hive_1_copy.out" with the same data as test_hive_1.in

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
