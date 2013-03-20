module Mobilize
  module Hive
    def Hive.config
      Base.config('hive')
    end

    def Hive.exec_path(cluster)
      Hive.clusters[cluster]['exec_path']
    end

    def Hive.output_db(cluster)
      Hive.clusters[cluster]['output_db']
    end

    def Hive.output_db_user(cluster)
      output_db_node = Hadoop.gateway_node(cluster)
      output_db_user = Ssh.host(output_db_node)['user']
      output_db_user
    end

    def Hive.clusters
      Hive.config['clusters']
    end

    def Hive.slot_ids(cluster)
      (1..Hive.clusters[cluster]['max_slots']).to_a.map{|s| "#{cluster}_#{s.to_s}"}
    end

    def Hive.slot_worker_by_cluster_and_path(cluster,path)
      working_slots = Mobilize::Resque.jobs.map{|j| begin j['args'][1]['hive_slot'];rescue;nil;end}.compact.uniq
      Hive.slot_ids(cluster).each do |slot_id|
        unless working_slots.include?(slot_id)
          Mobilize::Resque.set_worker_args_by_path(path,{'hive_slot'=>slot_id})
          return slot_id
        end
      end
      #return false if none are available
      return false
    end

    def Hive.unslot_worker_by_path(path)
      begin
        Mobilize::Resque.set_worker_args_by_path(path,{'hive_slot'=>nil})
        return true
      rescue
        return false
      end
    end

    def Hive.databases(cluster,user_name)
      Hive.run(cluster,"show databases",user_name)['stdout'].split("\n")
    end

    # converts a source path or target path to a dst in the context of handler and stage
    def Hive.path_to_dst(path,stage_path)
      has_handler = true if path.index("://")
      s = Stage.where(:path=>stage_path).first
      params = s.params
      target_path = params['target']
      is_target = true if path == target_path
      red_path = path.split("://").last
      first_path_node = red_path.gsub(".","/").split("/").first
      cluster =  Hadoop.clusters.include?(first_path_node) ? first_path_node : Hadoop.default_cluster
      user_name = Hdfs.user_name_by_stage_path(stage_path,cluster)
      databases = Hive.databases(cluster,user_name)
      #is user has a handler, is specifying a target,
      #or their first path node is a cluster name
      #or their first path node is actually a database
      #assume it's a hive pointer
      if is_target or
        has_handler or
        Hadoop.clusters.include?(first_path_node) or
        databases.include?(first_path_node)
        #make sure cluster is legit
        hive_url = Hive.url_by_path(red_path,user_name,is_target)
        return Dataset.find_or_create_by_url(hive_url)
      end
      #otherwise, use hdfs convention
      return Ssh.path_to_dst(path,stage_path)
    end

    def Hive.url_by_path(path,user_name,is_target=false)
      red_path = path.gsub(".","/")
      cluster = red_path.split("/").first.to_s
      if Hadoop.clusters.include?(cluster)
        #cut node out of path
        red_path = red_path.split("/")[1..-1].join("/")
      else
        cluster = Hadoop.default_cluster
      end
      db, table = red_path.split("/")[0..-1]
      url = "hive://#{cluster}/#{db}/#{table}"
      begin
        stat_response = Hive.table_stats(cluster, db, table, user_name)
        if is_target or stat_response['stderr'].to_s.length == 0
          return url
        else
          raise "Unable to find #{url} with error: #{stat_response['stderr']}"
        end
      rescue => exc
        raise Exception, "Unable to find #{url} with error: #{exc.to_s}", exc.backtrace
      end
    end

    #get field names and partition datatypes and size of a hive table
    def Hive.table_stats(cluster,db,table,user_name)
      describe_sql = "use #{db};describe extended #{table};"
      describe_response = Hive.run(cluster, describe_sql,user_name)
      return describe_response if describe_response['stdout'].length==0
      describe_output = describe_response['stdout']
      describe_output.split("location:").last.split(",").first
      #get location, fields, partitions
      result_hash = {}
      result_hash['location'] = describe_output.split("location:").last.split(",").first
      #get fields
      field_defs = describe_output.split(" \nDetailed Table Information").first.split(
                                         "\n").map{|f|
                                         f.strip.split("\t").ie{|fa|
                                         {"name"=>fa.first,"datatype"=>fa.second} if fa.first}}.compact
      #check for partititons
      if describe_output.index("partitionKeys:[FieldSchema")
        part_field_string = describe_output.split("partitionKeys:[").last.split("]").first
        #parse weird schema using yaml plus gsubs
        yaml_fields = "---" + part_field_string.gsub("FieldSchema","\n").gsub(
                                                     ")","").gsub(
                                                     ",","\n ").gsub(
                                                     "(","- ").gsub(
                                                     "null","").gsub(
                                                     ":",": ")
        #return partitions without the comment part
        result_hash['partitions'] = YAML.load(yaml_fields).map{|ph| ph.delete('comment');ph}
        #get rid of fields in fields section that are also partitions
        result_hash['partitions'].map{|p| p['name']}.each{|n| field_defs.delete_if{|f| f['name']==n}}
      end
      #assign field defs after removing partitions
      result_hash['field_defs'] = field_defs
      #get size
      result_hash['size'] = Hadoop.run(cluster,"fs -dus #{result_hash['location']}",user_name)['stdout'].split("\t").last.strip.to_i
      return result_hash
    end

    #run a generic hive command, with the option of passing a file hash to be locally available
    def Hive.run(cluster,hql,user_name,file_hash=nil)
      # no TempStatsStore
      hql = "set hive.stats.autogather=false;#{hql}"
      filename = hql.to_md5
      file_hash||= {}
      file_hash[filename] = hql
      #silent mode so we don't have logs in stderr; clip output
      #at hadoop read limit
      command = "#{Hive.exec_path(cluster)} -S -f #{filename} | head -c #{Hadoop.read_limit}"
      gateway_node = Hadoop.gateway_node(cluster)
      Ssh.run(gateway_node,command,user_name,file_hash)
    end

    def Hive.run_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      params = s.params
      cluster = params['cluster'] || Hive.clusters.keys.first
      user_name = Hdfs.user_name_by_stage_path(stage_path,cluster)
      #slot Hive worker if available
      slot_id = Hive.slot_worker_by_cluster_and_path(cluster,stage_path)
      return false unless slot_id

      #output table stores stage output
      output_db,output_table = [Hive.output_db(cluster),stage_path.gridsafe]
      output_path = [output_db,output_table].join(".")
      out_url = "hive://#{cluster}/#{output_db}/#{output_table}"

      #get hql
      if params['hql']
        hql = params['hql']
      else
        #user has passed in a gsheet hql
        return nil unless gdrive_slotdd
        source = s.sources.first
        hql = source.read(user_name)
      end

      #check for select at end
      hql_array = hql.split(";").map{|hc| hc.strip}.reject{|hc| hc.length==0}
      if hql_array.last.downcase.starts_with?("select")
        #nil if no prior commands
        prior_hql = hql_array[0..-2].join(";") if hql_array.length > 1
        select_hql = hql_array.last
        output_table_hql = ["drop table if exists #{output_path}",
                            "create table #{output_path} as #{select_hql};"].join(";")
        full_hql = [prior_hql, output_table_hql].compact.join(";")
        result = Hive.run(cluster,full_hql, user_name)
        Dataset.find_or_create_by_url(out_url)
      else
        result = Hive.run(cluster, hql, user_name)
      end
      #unslot worker
      Hive.unslot_worker_by_path(stage_path)
      response = {}
      response['out_url'] = out_url
      response['err_url'] = Dataset.write_by_url("gridfs://#{s.path}/err",result['stderr'].to_s,Gdrive.owner_name) if result['stderr'].to_s.length>0
      response['signal'] = result['exit_code']
      response
    end

    def Hive.schema_hash(schema_path,user_name,gdrive_slot)
      if schema_path.index("/")
        #slashes mean sheets
        out_tsv = Gsheet.find_by_path(schema_path,gdrive_slot).read(user_name)
      else
        u = User.where(:name=>user_name).first
        #check sheets in runner
        r = u.runner
        runner_sheet = r.gbook(gdrive_slot).worksheet_by_title(schema_path)
        out_tsv = if runner_sheet
                    runner_sheet.read(user_name)
                  else
                    #check for gfile. will fail if there isn't one.
                    Gfile.find_by_path(schema_path).read(user_name)
                  end
      end
      #use Gridfs to cache gdrive results
      file_name = schema_path.split("/").last
      out_url = "gridfs://#{schema_path}/#{file_name}"
      Dataset.write_by_url(out_url,out_tsv,user_name)
      schema_tsv = Dataset.find_by_url(out_url).read(user_name)
      schema_hash = {}
      schema_tsv.tsv_to_hash_array.each do |ha|
        schema_hash[ha['name']] = ha['datatype']
      end
      schema_hash
    end

    def Hive.path_params(cluster, path, user_name)
      db, table, partitions = path.gsub(".","/").split("/").ie{|sp| [sp.first, sp.second, sp[2..-1]]}
      #get existing table stats if any
      curr_stats = begin
                     Hive.table_stats(cluster, db, table, user_name)
                   rescue
                     nil
                   end
      {"db"=>db,
       "table"=>table,
       "partitions"=>partitions,
       "curr_stats"=>curr_stats}
    end

    def Hive.hql_to_table(cluster, source_hql, target_path, user_name, drop=false, schema_hash=nil)
      target_params = Hive.path_params(cluster, target_path, user_name)
      target_table_path = ['db','table'].map{|k| target_params[k]}.join(".")
      target_partitions = target_params['partitions'].to_a
      target_table_stats = target_params['curr_stats']

      source_hql_array = source_hql.split(";")
      last_select_i = source_hql_array.rindex{|hql| hql.downcase.strip.starts_with?("select")}
      #find the last select query -- it should be used for the temp table creation
      last_select_hql = (source_hql_array[last_select_i..-1].join(";")+";")
      #if there is anything prior to the last select, add it in prior to table creation
      prior_hql = ((source_hql_array[0..(last_select_i-1)].join(";")+";") if last_select_i and last_select_i>=1).to_s

      #create temporary table so we can identify fields etc.
      temp_db = Hive.output_db(cluster)
      temp_table_name = (source_hql+target_path).to_md5
      temp_table_path = [temp_db,temp_table_name].join(".")
      temp_drop_hql = "drop table if exists #{temp_table_path};"
      temp_create_hql = "#{prior_hql}#{temp_drop_hql}create table #{temp_table_path} as #{last_select_hql}"
      Hive.run(cluster,temp_create_hql,user_name)

      source_params = Hive.path_params(cluster, temp_table_path, user_name)
      source_table_path = ['db','table'].map{|k| source_params[k]}.join(".")
      source_table_stats = source_params['curr_stats']
      source_fields = source_table_stats['field_defs']

      if target_partitions.length == 0 and
        target_table_stats.ie{|tts| tts.nil? || drop || tts['partitions'].nil?}
        #no partitions in either user params or the target table

        target_headers = source_fields.map{|f| f['name']}

        target_field_stmt = target_headers.map{|h| "`#{h}`"}.join(",")

        field_defs = {}
        target_headers.each do |name|
          datatype = schema_hash[name] || "string"
          field_defs[name]=datatype
        end

        field_def_stmt = "(#{field_defs.map do |name,datatype|
                           "`#{name}` #{datatype}"
                         end.join(",")})"

        #always drop when no partititons
        target_drop_hql = "drop table if exists #{target_table_path};"

        target_create_hql = "create table if not exists #{target_table_path} #{field_def_stmt};"

        target_insert_hql = "insert overwrite table #{target_table_path} select #{target_field_stmt} from #{source_table_path};"

        target_full_hql = [target_drop_hql,target_create_hql,target_insert_hql,temp_drop_hql].join

        Hive.run(cluster, target_full_hql, user_name)

      elsif target_partitions.length > 0 and
        target_table_stats.ie{|tts| tts.nil? || drop || tts['partitions'].to_a.map{|p| p['name']} == target_partitions}
        #partitions and no target table or same partitions in both target table and user params

        target_headers = source_fields.map{|f| f['name']}.reject{|h| target_partitions.include?(h)}

        field_defs = {}
        target_headers.each do |name|
          datatype = schema_hash[name] || "string"
          field_defs[name]=datatype
        end

        field_def_stmt = "(#{field_defs.map do |name,datatype|
                           "`#{name}` #{datatype}"
                         end.join(",")})"

        part_defs = {}
        target_partitions.each do |name|
          datatype = schema_hash[name] || "string"
          part_defs[name] = datatype
        end

        part_def_stmt = "(#{part_defs.map do |name,datatype|
                           "`#{name}` #{datatype}"
                         end.join(",")})"

        target_field_stmt = target_headers.map{|h| "`#{h}`"}.join(",")

        target_part_stmt = target_partitions.map{|h| "`#{h}`"}.join(",")

        target_set_hql = ["set hive.exec.dynamic.partition.mode=nonstrict;",
                          "set hive.exec.max.dynamic.partitions.pernode=1000;",
                          "set hive.exec.dynamic.partition=true;",
                          "set hive.exec.max.created.files = 200000;",
                          "set hive.max.created.files = 200000;"].join

        if drop or target_table_stats.nil?
          target_drop_hql = "drop table if exists #{target_table_path};"
          target_create_hql = target_drop_hql +
                            "create table if not exists #{target_table_path} #{field_def_stmt} " +
                            "partitioned by #{part_def_stmt};"

        else
          target_db,target_table = target_table_path.split(".")
          #get all the permutations of possible partititons
          part_perm_hql = "set hive.cli.print.header=true;select distinct #{target_part_stmt} from #{source_table_path};"
          part_perm_tsv = Hive.run(cluster, part_perm_hql, user_name)
          #having gotten the permutations, ensure they are dropped
          part_hash_array = part_perm_tsv.tsv_to_hash_array
          part_drop_hql = part_hash_array.map do |h|
            part_drop_stmt = h.map do |name,value|
                               part_defs[name[1..-2]]=="string" ? "#{name}='#{value}'" : "#{name}=#{value}"
                             end.join(",")
                            "use #{target_db};alter table #{target_table} drop if exists partition (#{part_drop_stmt});"
                          end.join
          target_create_hql = part_drop_hql
        end

        target_insert_hql = "insert overwrite table #{target_table_path} " +
                            "partition (#{target_part_stmt}) " +
                            "select #{target_field_stmt},#{target_part_stmt} from #{source_table_path};"

        target_full_hql = [target_set_hql, target_create_hql, target_insert_hql, temp_drop_hql].join

        Hive.run(cluster, target_full_hql, user_name)
      else
        error_msg = "Incompatible partition specs"
        raise error_msg
      end
      return target_path
    end

    #turn a tsv into a hive table.
    #Accepts options to drop existing target if any
    #also schema with column datatype overrides
    def Hive.tsv_to_table(cluster, db, table, source_tsv, user_name, drop=false, schema_hash=nil)
      source_headers = source_tsv.tsv_header_array

      target_table_path = [db,table].join(".")
      target_params = Hive.path_params(cluster, target_table_path, user_name)
      target_partitions = target_params['partitions'].to_a
      target_table_stats = target_params['curr_stats']

      schema_hash ||= {}

      if target_partitions.length == 0 and
        target_table_stats.ie{|tts| tts.nil? || drop || tts['partitions'].nil?}
        #no partitions in either user params or the target table
        #or drop and start fresh

        #one file only, strip headers, replace tab with ctrl-a for hive
        #get rid of freaking carriage return characters
        source_rows = source_tsv.split("\n")[1..-1].join("\n").gsub("\t","\001")
        source_tsv_filename = "000000_0"
        file_hash = {source_tsv_filename=>source_rows}

        field_defs = source_headers.map do |name| 
          datatype = schema_hash[name] || "string"
          "`#{name}` #{datatype}"
        end.ie{|fs| "(#{fs.join(",")})"}

        #for single insert, use drop table and create table always
        target_drop_hql = "drop table if exists #{target_table_path}"

        target_create_hql = "create table #{target_table_path} #{field_defs}"

        #load source data
        target_insert_hql = "load data local inpath '#{source_tsv_filename}' overwrite into table #{target_table_path};"

        target_full_hql = [target_drop_hql,target_create_hql,target_insert_hql].join(";")

        Hive.run(cluster, target_full_hql, user_name, file_hash)

      elsif target_partitions.length > 0 and
        target_table_stats.ie{|tts| tts.nil? || drop || tts['partitions'].to_a.map{|p| p['name']} == target_partitions}
        #partitions and no target table
        #or same partitions in both target table and user params
        #or drop and start fresh

        target_headers = source_headers.reject{|h| target_partitions.include?(h)}

        field_defs = "(#{target_headers.map do |name|
                       datatype = schema_hash[name] || "string"
                       "`#{name}` #{datatype}"
                     end.join(",")})"

        partition_defs = "(#{target_partitions.map do |name|
                           datatype = schema_hash[name] || "string"
                           "#{name} #{datatype}"
                         end.join(",")})"

        target_drop_hql = drop ? "drop table if exists #{target_table_path};" : ""

        target_create_hql = target_drop_hql +
                            "create table if not exists #{target_table_path} #{field_defs} " +
                            "partitioned by #{partition_defs}"

        #create target table early if not here
        Hive.run(cluster, target_create_hql, user_name)

        target_table_stats = Hive.table_stats(target_db, target_table, cluster, user_name)

        #create data hash from source hash array
        data_hash = {}
        source_hash_array = source_tsv.tsv_to_hash_array
        source_hash_array.each do |ha|
          tpmk = target_partitions.map{|pn| "#{pn}=#{ha[pn]}"}.join("/")
          tpmv = ha.reject{|k,v| target_partitions.include?(k)}.values.join("\001")
          if data_hash[tpmk]
            data_hash[tpmk] += "\n#{tpmv}"
          else
            data_hash[tpmk] = tpmv
          end
        end

        #go through completed data hash and write each key value to the table in question
        data_hash.each do |tpmk,tpmv|
          base_filename = "000000_0"
          part_pairs = tpmk.split("/").map{|p| p.split("=").ie{|pa| ["#{pa.first}","#{pa.second}"]}}
          part_dir = part_pairs.map{|pp| "#{pp.first}=#{pp.second}"}.join("/")
          part_stmt = part_pairs.map{|pp| "#{pp.first}='#{pp.second}'"}.join(",")
          hdfs_dir = "#{target_table_stats['location']}/#{part_dir}"
          hdfs_source_path = "/#{hdfs_dir.split("/")[3..-2].join("/")}/#{base_filename}"
          hdfs_target_path = "/#{hdfs_dir.split("/")[3..-1].join("/")}"
          #load partition into source path
          puts "Writing to #{hdfs_source_path} for #{user_name} at #{Time.now.utc}"
          Hdfs.write(hdfs_source_path,tpmv,user_name)
          #let Hive know where the partition is
          target_add_part_hql = "use #{target_db};alter table #{target_table} add if not exists partition (#{part_stmt}) location '#{hdfs_target_path}'"
          target_insert_part_hql   = "load data inpath '#{hdfs_source_path}' overwrite into table #{target_table} partition (#{part_stmt});"
          target_part_hql = [target_add_part_hql,target_insert_part_hql].join(";")
          puts "Adding partition #{tpmk} to #{target_table_path} for #{user_name} at #{Time.now.utc}"
          Hive.run(cluster, target_part_hql, user_name)
        end
      else
        error_msg = "Incompatible partition specs: " +
                    "target table:#{target_table_stats['partitions'].to_s}, " +
                    "user_params:#{target_partitions.to_s}"
        raise error_msg
      end
      return {'stdout'=>target_path}
    end

    def Hive.write_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      params = s.params
      source = s.sources.first
      target = s.target
      cluster = target.url.split("://").last.split("/").first
      #update stage with the node so we can use it
      user_name = Hdfs.user_name_by_stage_path(stage_path,cluster)

      #slot Hive worker if available
      slot_id = Hive.slot_worker_by_cluster_and_path(cluster,stage_path)
      return false unless slot_id

      schema_hash = if params['schema']
                      gdrive_slot = Gdrive.slot_worker_by_path(stage_path)
                      #return blank response if there are no slots available
                      return nil unless gdrive_slot
                      Hive.schema_hash(params['schema'],user_name,gdrive_slot)
                    else
                      {}
                    end
      Gdrive.unslot_worker_by_path(stage_path)
      #drop target before create/insert?
      drop = params['drop']

      #determine source
      source_tsv,source_hql = [nil]*2
      if params['hql']
        source_hql = params['hql']
      elsif source
        if source.handler == 'hive'
          #source table
          cluster,source_path = source.path.split("/").ie{|sp| [sp.first, sp[1..-1].join(".")]}
          source_hql = "select * from #{source_path};"
        elsif ['gsheet','gridfs','hdfs'].include?(source.handler)
          if source.path.ie{|sdp| sdp.index(/\.[A-Za-z]ql$/) or sdp.ends_with?(".ql")}
            source_hql = source.read(user_name)
          else
            #tsv from sheet
            source_tsv = source.read(user_name)
          end
        end
      end

      result = begin
                 if source_hql
                   Hive.hql_to_table(cluster, db, table, part_path, source_hql, user_name, drop, schema_hash)
                 elsif source_tsv
                   Hive.tsv_to_table(cluster, db, table, part_path, source_tsv, target_path, user_name, drop, schema_hash)
                 else
                   raise "Unable to determine source tsv or source hql"
                 end
                 {'stdout'=>target_path}
               rescue => exc
                 {'stderr'=>exc.to_s}
               end

      #unslot worker and write result
      Hive.unslot_worker_by_path(stage_path)

      #output table stores stage output
      output_db,output_table = [Hive.output_db(cluster),stage_path.gridsafe]
      out_url = "hive://#{cluster}/#{output_db}/#{output_table}"
      Dataset.find_or_create_by_url(out_url)

      response = {}
      response['out_url'] = out_url
      response['err_url'] = Dataset.write_by_url("gridfs://#{s.path}/err",result['stderr'].to_s,Gdrive.owner_name) if result['stderr'].to_s.length>0
      response['signal'] = result['exit_code']
      response
    end

    def Hive.read_by_dataset_path(dst_path,user_name)
      cluster,source_path = dst_path.split("/").ie do |sp|
                                                     if sp.length == 2
                                                       [Hive.clusters.first.first,sp.join(".")]
                                                     else
                                                       [sp.first, sp[1..-1].join(".")]
                                                     end
                                                   end
      hql = "set hive.cli.print.header=true;select * from #{source_path};"
      Hive.run(cluster, hql,user_name)
    end

    def Hive.write_by_dataset_path(dst_path,source_tsv,user_name)
      cluster,target_path = dst_path.split("/").ie do |sp|
                                                     if sp.length == 2
                                                       [Hive.clusters.first.first,sp.join(".")]
                                                     else
                                                       [sp.first, sp[1..-1].join(".")]
                                                     end
                                                   end
      drop = true
      Hive.tsv_to_table(cluster, source_tsv, target_path, user_name, drop)
    end
  end

end
