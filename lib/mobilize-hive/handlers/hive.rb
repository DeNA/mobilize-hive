module Mobilize
  module Hive
    #adds convenience methods
    require "#{File.dirname(__FILE__)}/../helpers/hive_helper"
    # converts a source path or target path to a dst in the context of handler and stage
    def Hive.path_to_dst(path,stage_path,gdrive_slot)
      has_handler = true if path.index("://")
      s = Stage.where(:path=>stage_path).first
      params = s.params
      target_path = params['target']
      cluster = params['cluster'] if Hadoop.clusters.include?(params['cluster'].to_s)
      is_target = true if path == target_path
      red_path = path.split("://").last
      first_path_node = red_path.gsub(".","/").split("/").first
      cluster ||= Hadoop.clusters.include?(first_path_node) ? first_path_node : Hadoop.default_cluster
      user_name = Hdfs.user_name_by_stage_path(stage_path,cluster)
      #save some time on targets
      databases = Hive.databases(cluster,user_name) unless is_target
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
      return Ssh.path_to_dst(path,stage_path,gdrive_slot)
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
        #add table stats check only if not target
        if is_target or Hive.table_stats(cluster, db, table, user_name)['stderr'].to_s.length == 0
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
      return nil if describe_response['stdout'].length==0
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
    def Hive.run(cluster,hql,user_name,params=nil,file_hash=nil,stage_path=nil)
      preps = Hive.prepends.map do |p|
                                  prefix = "set "
                                  suffix = ";"
                                  prep_out = p
                                  prep_out = "#{prefix}#{prep_out}" unless prep_out.starts_with?(prefix)
                                  prep_out = "#{prep_out}#{suffix}" unless prep_out.ends_with?(suffix)
                                  prep_out
                                end.join
      hql = "#{preps}#{hql}"
      filename = "hql"
      file_hash||= {}
      file_hash[filename] = hql
      params ||= {}
      #replace any params in the file_hash and command
      params.each do |k,v|
        file_hash.each do |name,data|
          data.gsub!("@#{k}",v)
        end
      end
      #add in default params
      Hive.default_params.each do |k,v|
        file_hash.each do |name,data|
          data.gsub!(k,v)
        end
      end
      #silent mode so we don't have logs in stderr; clip output
      #at hadoop read limit
      command = "#{Hive.exec_path(cluster)} -f #{filename}"
      gateway_node = Hadoop.gateway_node(cluster)
      response = Ssh.run(gateway_node,command,user_name,stage_path,file_hash)
      #override exit code 0 when stdout is blank and
      #stderror contains FAILED or KILLED
      if response['stdout'].to_s.length == 0 and
        response['stderr'].to_s.ie{|se| se.index("FAILED") or se.index("KILLED")}
        response['exit_code'] = 500
      end
      return response
    end

    def Hive.run_by_stage_path(stage_path)
      gdrive_slot = Gdrive.slot_worker_by_path(stage_path)
      #return blank response if there are no slots available
      return nil unless gdrive_slot
      s = Stage.where(:path=>stage_path).first
      params = s.params
      cluster = params['cluster'] || Hive.clusters.keys.first
      user_name = Hdfs.user_name_by_stage_path(stage_path,cluster)
      job_name = s.path.sub("Runner_","")
      #slot Hive worker if available
      slot_id = Hive.slot_worker_by_cluster_and_path(cluster,stage_path)
      return false unless slot_id

      #output table stores stage output
      output_db,output_table = [Hive.output_db(cluster),job_name.downcase.alphanunderscore]
      output_path = [output_db,output_table].join(".")
      out_url = "hive://#{cluster}/#{output_db}/#{output_table}"

      #get hql
      if params['hql']
        hql = params['hql'].strip
      else
        source = s.sources(gdrive_slot).first
        hql = source.read(user_name,gdrive_slot).strip
      end

      Gdrive.unslot_worker_by_path(stage_path)

      #check for select at end
      hql_array = hql.split("\n").reject{|l| l.starts_with?("--") or l.strip.length==0}.join("\n").split(";").map{|h| h.strip}
      last_statement = hql_array.last
      file_hash = nil
      if last_statement.to_s.downcase.starts_with?("select")
        #nil if no prior commands
        prior_hql = hql_array[0..-2].join(";") if hql_array.length > 1
        select_hql = hql_array.last
        output_table_hql = ["set mapred.job.name=#{job_name};",
                            "drop table if exists #{output_path}",
                            "create table #{output_path} as #{select_hql};"].join(";")
        full_hql = [prior_hql, output_table_hql].compact.join(";")
        result = Hive.run(cluster,full_hql, user_name,params['params'],file_hash,stage_path)
        Dataset.find_or_create_by_url(out_url)
      else
        result = Hive.run(cluster, hql, user_name,params['params'],file_hash,stage_path)
        Dataset.find_or_create_by_url(out_url)
        Dataset.write_by_url(out_url,result['stdout'],user_name) if result['stdout'].to_s.length>0
      end
      #unslot worker
      Hive.unslot_worker_by_path(stage_path)
      response = {}
      response['out_url'] = out_url
      response['err_url'] = Dataset.write_by_url("gridfs://#{s.path}/err",result['stderr'].to_s,Gdrive.owner_name) if result['stderr'].to_s.length>0
      response['signal'] = result['exit_code']
      response
    end

    def Hive.schema_hash(schema_path,stage_path,user_name,gdrive_slot)
      handler = if schema_path.index("://")
                  schema_path.split("://").first
                else
                  "gsheet"
                end
      dst = "Mobilize::#{handler.downcase.capitalize}".constantize.path_to_dst(schema_path,stage_path,gdrive_slot)
      out_raw = dst.read(user_name,gdrive_slot)
      #determine the datatype for schema; accept json, yaml, tsv
      if schema_path.ends_with?(".yml")
        out_ha = begin;YAML.load(out_raw);rescue ScriptError, StandardError;nil;end if out_ha.nil?
      else
        out_ha = begin;JSON.parse(out_raw);rescue ScriptError, StandardError;nil;end
        out_ha = out_raw.tsv_to_hash_array if out_ha.nil?
      end
      schema_hash = {}
      out_ha.each do |hash|
        schema_hash[hash['name']] = hash['datatype']
      end
      schema_hash
    end

    def Hive.hql_to_table(cluster, db, table, part_array, source_hql, user_name, stage_path, drop=false, schema_hash=nil, run_params=nil,compress=false)
      job_name = stage_path.sub("Runner_","")
      table_path = [db,table].join(".")
      table_stats = Hive.table_stats(cluster, db, table, user_name)
      url = "hive://" + [cluster,db,table,part_array.compact.join("/")].join("/")

      #decomment hql

      source_hql_array = source_hql.split("\n").reject{|l| l.starts_with?("--") or l.strip.length==0}.join("\n").split(";").map{|h| h.strip}
      last_select_i = source_hql_array.rindex{|s| s.downcase.starts_with?("select")}
      #find the last select query -- it should be used for the temp table creation
      last_select_hql = (source_hql_array[last_select_i..-1].join(";")+";")
      #if there is anything prior to the last select, add it in prior to table creation
      prior_hql = ((source_hql_array[0..(last_select_i-1)].join(";")+";") if last_select_i and last_select_i>=1).to_s

      #create temporary table so we can identify fields etc.
      temp_db = Hive.output_db(cluster)
      temp_table_name = "temp_#{job_name.downcase.alphanunderscore}"
      temp_table_path = [temp_db,temp_table_name].join(".")
      temp_set_hql = "set mapred.job.name=#{job_name} (temp table);"
      temp_drop_hql = "drop table if exists #{temp_table_path};"
      temp_create_hql = "#{temp_set_hql}#{prior_hql}#{temp_drop_hql}create table #{temp_table_path} as #{last_select_hql}"
      file_hash = nil
      response = Hive.run(cluster,temp_create_hql,user_name,run_params,file_hash,stage_path)
      raise response['stderr'] if response['stderr'].to_s.ie{|s| s.index("FAILED") or s.index("KILLED")}

      source_table_stats = Hive.table_stats(cluster,temp_db,temp_table_name,user_name)
      source_fields = source_table_stats['field_defs']

      if part_array.length == 0 and
        table_stats.ie{|tts| tts.nil? || drop || tts['partitions'].nil?}
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
        target_name_hql = "set mapred.job.name=#{job_name};"

        if compress
          target_name_hql = target_name_hql+["set hive.exec.compress.output=true;",
                          "set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;",
                          "set mapred.output.compression.type=BLOCK;"].join
        end

        target_drop_hql = "drop table if exists #{table_path};"

        target_create_hql = "create table if not exists #{table_path} #{field_def_stmt};"

        target_insert_hql = "insert overwrite table #{table_path} select #{target_field_stmt} from #{temp_table_path};"

        target_full_hql = [target_name_hql,
                           target_drop_hql,
                           target_create_hql,
                           target_insert_hql,
                           temp_drop_hql].join


        puts "FULL HQL QUERY: " + target_full_hql

        
        response = Hive.run(cluster, target_full_hql, user_name, run_params)

        raise response['stderr'] if response['stderr'].to_s.ie{|s| s.index("FAILED") or s.index("KILLED")}

      elsif part_array.length > 0 and
        table_stats.ie{|tts| tts.nil? || drop || tts['partitions'].to_a.map{|p| p['name']}.sort == part_array.sort}
        #partitions and no target table or same partitions in both target table and user params

        target_headers = source_fields.map{|f| f['name']}.reject{|h| part_array.include?(h)}

        field_defs = {}
        target_headers.each do |name|
          datatype = schema_hash[name] || "string"
          field_defs[name]=datatype
        end

        field_def_stmt = "(#{field_defs.map do |name,datatype|
                           "`#{name}` #{datatype}"
                         end.join(",")})"

        part_defs = {}
        part_array.each do |name|
          datatype = schema_hash[name] || "string"
          part_defs[name] = datatype
        end

        part_def_stmt = "(#{part_defs.map do |name,datatype|
                           "`#{name}` #{datatype}"
                         end.join(",")})"

        target_field_stmt = target_headers.map{|h| "`#{h}`"}.join(",")

        target_part_stmt = part_array.map{|h| "`#{h}`"}.join(",")

        target_set_hql = ["set mapred.job.name=#{job_name};",
                          "set hive.exec.dynamic.partition.mode=nonstrict;",
                          "set hive.exec.max.dynamic.partitions.pernode=10000;",
                          "set hive.exec.dynamic.partition=true;",
                          "set hive.exec.max.created.files = 200000;",
                          "set hive.max.created.files = 200000;"].join
        if compress
          target_set_hql = target_set_hql+["set hive.exec.compress.output=true;",
                          "set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;",
                          "set mapred.output.compression.type=BLOCK;"].join
        end

        if drop or table_stats.nil?
          target_drop_hql = "drop table if exists #{table_path};"
          target_create_hql = target_drop_hql +
                            "create table if not exists #{table_path} #{field_def_stmt} " +
                            "partitioned by #{part_def_stmt};"

        else
          #get all the permutations of possible partititons
          part_set_hql = "set hive.cli.print.header=true;set mapred.job.name=#{job_name} (permutations);"
          part_select_hql = "select distinct #{target_part_stmt} from #{temp_table_path};"
          part_perm_hql = part_set_hql + part_select_hql
          response = Hive.run(cluster, part_perm_hql, user_name, run_params)
          raise response['stderr'] if response['stderr'].to_s.ie{|s| s.index("FAILED") or s.index("KILLED")}
          part_perm_tsv = response['stdout']
          #having gotten the permutations, ensure they are dropped
          part_hash_array = part_perm_tsv.tsv_to_hash_array
          #make sure there is data
          if part_hash_array.first.nil? or part_hash_array.first.values.include?(nil)
            #blank result set, return url
            return url
          end

          part_drop_hql = part_hash_array.map do |h|
            part_drop_stmt = h.map do |name,value|
                               part_defs[name[1..-2]].downcase=="string" ? "#{name}='#{value}'" : "#{name}=#{value}"
                             end.join(",")
                            "use #{db};alter table #{table} drop if exists partition (#{part_drop_stmt});"
                          end.join
          target_create_hql = part_drop_hql
        end

        target_insert_hql = "insert overwrite table #{table_path} " +
                            "partition (#{target_part_stmt}) " +
                            "select #{target_field_stmt},#{target_part_stmt} from #{temp_table_path};"

        target_full_hql = [target_set_hql, target_create_hql, target_insert_hql, temp_drop_hql].join

        puts "FULL HQL QUERY: " + target_full_hql

        response = Hive.run(cluster, target_full_hql, user_name, run_params)
        raise response['stderr'] if response['stderr'].to_s.ie{|s| s.index("FAILED") or s.index("KILLED")}
      else
        error_msg = "Incompatible partition specs"
        raise error_msg
      end
      return url
    end

    #turn a tsv into a hive table.
    #Accepts options to drop existing target if any
    #also schema with column datatype overrides
    def Hive.tsv_to_table(cluster, table_path, user_name, source_tsv)
      #get rid of freaking carriage return characters
      if source_tsv.index("\r\n")
        source_tsv = source_tsv.gsub("\r\n","\n")
      elsif source_tsv.index("\r")
        source_tsv = source_tsv.gsub("\r","\n")
      end
      #nil if only header row, or no header row
      if source_tsv.strip.length==0 or source_tsv.strip.split("\n").length<=1
        puts "no data in source_tsv for #{cluster}/#{table_path}"
        return nil
      end
      source_headers = source_tsv.tsv_header_array

      #one file only, strip headers, replace tab with ctrl-a for hive
      source_rows = source_tsv.split("\n")[1..-1].join("\n").gsub("\t","\001")
      source_tsv_filename = "000000_0"
      file_hash = {source_tsv_filename=>source_rows}

      field_defs = source_headers.map do |name| 
        "`#{name}` string"
      end.ie{|fs| "(#{fs.join(",")})"}

      #for single insert, use drop table and create table always
      target_drop_hql = "drop table if exists #{table_path}"

      target_create_hql = "create table #{table_path} #{field_defs}"

      #load source data
      target_insert_hql = "load data local inpath '#{source_tsv_filename}' overwrite into table #{table_path};"

      target_full_hql = [target_drop_hql,target_create_hql,target_insert_hql].join(";")

      response = Hive.run(cluster, target_full_hql, user_name, nil, file_hash)
      raise response['stderr'] if response['stderr'].to_s.ie{|s| s.index("FAILED") or s.index("KILLED")}

      return true
    end

    def Hive.write_by_stage_path(stage_path)
      gdrive_slot = Gdrive.slot_worker_by_path(stage_path)
      #return blank response if there are no slots available
      return nil unless gdrive_slot
      s = Stage.where(:path=>stage_path).first
      job_name = s.path.sub("Runner_","")
      params = s.params
      source = s.sources(gdrive_slot).first
      target = s.target
      cluster, db, table = target.url.split("://").last.split("/")
      #slot Hive worker if available
      slot_id = Hive.slot_worker_by_cluster_and_path(cluster,stage_path)
      return false unless slot_id
      #update stage with the node so we can use it
      user_name = Hdfs.user_name_by_stage_path(stage_path,cluster)

      schema_hash = if params['schema']
                      Hive.schema_hash(params['schema'],stage_path,user_name,gdrive_slot)
                    else
                      {}
                    end
      #drop target before create/insert?
      drop = params['drop']
      compress = params['compress']

      #determine source
      source_tsv,source_hql = [nil]*2
      if params['hql']
        source_hql = params['hql']
      elsif source
        if source.handler == 'hive'
          #source table
          cluster,source_path = source.path.split("/").ie{|sp| [sp.first, sp[1..-1].join(".")]}
          source_hql = "select * from #{source_path};"
        else
          if source.path.ie{|sdp| sdp.index(/\.[A-Za-z]ql$/) or sdp.ends_with?(".ql")}
            source_hql = source.read(user_name,gdrive_slot)
          else
            #tsv from sheet or file
            source_tsv = source.read(user_name,gdrive_slot)
          end
        end
      end

      Gdrive.unslot_worker_by_path(stage_path)
      part_array = if params['partitions']
                    params['partitions'].to_a.map{|p| p.gsub(".","/").split("/")}.flatten
                  elsif params['target']
                    #take the end parts of the target, that are not the cluster, db, table
                    target_array = params['target'].gsub(".","/").split("/")
                    [cluster,db,table].each do |term|
                      target_array = target_array[1..-1] if target_array.first == term
                    end
                    target_array
                  else
                    []
                  end

      result = begin
                 url = if source_hql
                         #include any params (or nil) at the end
                         run_params = params['params']
                         Hive.hql_to_table(cluster, db, table, part_array, source_hql, user_name, stage_path,drop, schema_hash,run_params,compress)
                       elsif source_tsv
                         #first write tsv to temp table
                         temp_table_path = "#{Hive.output_db(cluster)}.temptsv_#{job_name.downcase.alphanunderscore}"
                         has_data = Hive.tsv_to_table(cluster, temp_table_path, user_name, source_tsv)
                         if has_data
                           #then do the regular insert, with source hql being select * from temp table
                           source_hql = "select * from #{temp_table_path}"
                           Hive.hql_to_table(cluster, db, table, part_array, source_hql, user_name, stage_path, drop, schema_hash,nil,compress)
                         else
                           nil
                         end
                       elsif source
                         #null sheet
                       else
                         raise "Unable to determine source tsv or source hql"
                       end
                 {'stdout'=>url,'exit_code'=>0}
               rescue => exc
                 {'stderr'=>"#{exc.to_s}\n#{exc.backtrace.join("\n")}", 'exit_code'=>500}
               end

      #unslot worker and write result
      Hive.unslot_worker_by_path(stage_path)

      response = {}
      response['out_url'] = Dataset.write_by_url("gridfs://#{s.path}/out",result['stdout'].to_s,Gdrive.owner_name) if result['stdout'].to_s.length>0
      response['err_url'] = Dataset.write_by_url("gridfs://#{s.path}/err",result['stderr'].to_s,Gdrive.owner_name) if result['stderr'].to_s.length>0
      response['signal'] = result['exit_code']
      response
    end

    def Hive.read_by_dataset_path(dst_path,user_name,*args)
      cluster, db, table = dst_path.split("/")
      source_path = [db,table].join(".")
      job_name = "read #{cluster}/#{db}/#{table}"
      set_hql = "set hive.cli.print.header=true;set mapred.job.name=#{job_name};"
      select_hql = "select * from #{source_path};"
      hql = [set_hql,select_hql].join
      response = Hive.run(cluster, hql,user_name)
      raise "Unable to read hive://#{dst_path} with error: #{response['stderr']}" if response['stderr'].to_s.ie{|s| s.index("FAILED") or s.index("KILLED")}
      return response['stdout']
    end

    def Hive.write_by_dataset_path(dst_path,source_tsv,user_name,*args)
      cluster,db,table = dst_path.split("/")
      table_path = "#{db}.#{table}"
      Hive.tsv_to_table(cluster, table_path, user_name, source_tsv)
    end
  end

end
