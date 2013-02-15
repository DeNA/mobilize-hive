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

    def Hive.clusters
      Hive.config['clusters']
    end

    def Hive.slot_ids(cluster)
      (1..Hive.clusters[cluster]['max_slots']).to_a.map{|s| "#{cluster}_#{s.to_s}"}
    end

    def Hive.slot_worker_by_cluster_and_path(cluster,path)
      working_slots = Mobilize::Resque.jobs('working').map{|j| j['hive_slot'] if (j and j['hive_slot'])}.compact
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

    #get field names and partition datatypes and size of a hive table
    def Hive.table_stats(db,table,cluster,user)
      describe_sql = "use #{db};describe extended #{table}"
      describe_output = Hive.run(describe_sql,cluster,user)
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
      result_hash['size'] = Hadoop.run("fs -dus #{result_hash['location']}",cluster,user).split("\t").last.strip.to_i
      return result_hash
    end

    #run a generic hive command, with the option of passing a file hash to be locally available
    def Hive.run(command,cluster,user,file_hash=nil)
      filename = command.to_md5
      file_hash||= {}
      file_hash[filename] = command
      #silent mode so we don't have logs in stderr
      exec_command = "#{Hive.exec_path(cluster)} -S -f #{filename}"
      gateway_node = Hadoop.gateway_node(cluster)
      Ssh.run(gateway_node,exec_command,user,file_hash)
    end

    def Hive.run_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      user = params['user']
      cluster = params['cluster'] || Hive.clusters.keys.first
      node = Hadoop.gateway_node(cluster)
      node_user = Ssh.host(node)['user']
      if user and !Ssh.sudoers(node).include?(u.name)
        raise "#{u.name} does not have su permissions for #{node}"
      elsif user.nil? and Ssh.su_all_users(node)
        user = u.name
      end

      #slot Hive worker if available
      slot_id = Hive.slot_worker_by_cluster_and_path(cluster,stage_path)
      return false unless slot_id

      #output table stores stage output
      output_path = [Hive.output_db,stage_path.gridsafe].join(".")
      output_db,output_table = output_table.split(".")

      #get hql
      if params['cmd']
        command = params['cmd']
      else
        #user has passed in a gsheet hql
        gdrive_slot = Gdrive.slot_worker_by_path(stage_path)
        #return blank response if there are no slots available
        return nil unless gdrive_slot
        source_dst = s.source_dsts(gdrive_slot).first
        Gdrive.unslot_worker_by_path(stage_path)
        command = source_dst.read(user)
      end

      #check for select at end
      command_array = command.split(";").map{|cc| cc.strip}.reject{|cc| cc.length==0}
      if command_array.last.downcase.starts_with?("select")
        #nil if no prior commands
        prior_hql = command_array[0..-2].join(";") if command_array.length > 1
        select_hql = command_array.last
        output_table_hql = ["drop table if exists #{output_path}",
                            "create table #{output_path} as #{select_hql};"].join(";")
        full_hql = [prior_hql, output_table_hql].compact.join(";")
        Hive.run(full_hql, cluster, user)
        #make sure node user owns the stage result directory
        output_dir = Hive.table_dir(output_db,output_table,cluster,node_user)
        chown_command = "#{Hadoop.exec_path(cluster)} fs -chown -R #{node_user} #{output_dir}"
        Ssh.run(node,chown_command,node_user)
      else
        out_string = Hive.run(command, cluster, user)
        out_string_filename = "000000_0"
        #create table for result, load result into it
        output_table_hql = ["drop table if exists #{output_path}",
                            "create table #{output_path} (result string)",
                            "load data local inpath '#{out_string_filename}' overwrite into table #{output_path};"].join(";")
        file_hash = {out_string_filename=>out_string}
        Hive.run(output_table_hql, cluster, node_user, file_hash)
      end
      #unslot worker and write result
      Hive.unslot_worker_by_path(path)
      out_url = "hive://#{cluster}/#{output_db}/#{output_table}"
      out_url
    end

    def Hive.gdrive_schema(schema_path,user,gdrive_slot)
      if schema_path.index("/")
        #slashes mean sheets
        out_tsv = Gsheet.find_by_path(schema_path,gdrive_slot).read(user)
      else
        u = User.where(:name=>user).first
        #check sheets in runner
        r = u.runner
        runner_sheet = r.gbook(gdrive_slot).worksheet_by_title(schema_path)
        out_tsv = if runner_sheet
                    runner_sheet.read(user)
                  else
                    #check for gfile. will fail if there isn't one.
                    Gfile.find_by_path(schema_path).read(user)
                  end
        #use Gridfs to cache gdrive results
        file_name = schema_path.split("/").last
        out_url = "gridfs://#{schema_path}/#{file_name}"
        Dataset.write_by_url(out_url,out_tsv,user)
        return Dataset.find_by_url(out_url).read(user)
      end
    end

    def Hive.write_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      user = params['user']
      cluster = params['cluster'] || Hive.clusters.keys.first

      #slot Hive worker if available
      slot_id = Hive.slot_worker_by_cluster_and_path(cluster,stage_path)
      return false unless slot_id

      node = Hadoop.gateway_node(cluster)
      node_user = Ssh.host(node)['user']
      if user and !Ssh.sudoers(node).include?(u.name)
        raise "#{u.name} does not have su permissions for #{node}"
      elsif user.nil? and Ssh.su_all_users(node)
        user = u.name
      end

      #output table stores stage output
      output_path = [Hive.output_db,stage_path.gridsafe].join(".")
      output_db,output_table = output_table.split(".")

      #determine path for target
      target_path = params['target']
      target_db, target_table, target_partitions = target_path.gsub(".","/").split("/").ie{|sp| [sp.first, sp.second, sp[2..-1]]}

      target_table_path = "#{target_db}.#{target_table}"

      #get target stats if any
      target_table_stats = begin
                             Hive.table_stats(target_db,target_table,cluster,node_user)
                           rescue
                             nil
                           end

      gdrive_slot = Gdrive.slot_worker_by_path(stage_path)
      #return blank response if there are no slots available
      return nil unless gdrive_slot
      source_dst = s.source_dsts(gdrive_slot).first
      user_schema = if params['schema']
                      #get the schema
                      Hive.gdrive_schema(params['schema'],
                                         user,
                                         gdrive_slot)
                    else
                      nil
                    end
      Gdrive.unslot_worker_by_path(stage_path)

      #determine source
      if source_dst.handler == 'hive'
        #source table
        source_path = source_dst.path

        #get table stats from source table
        source_table_stats = Hive.table_stats(source_db, source_table, cluster, node_user)

        target_table_hql = if target_partitions.length == 0 and
                             target_table_stats.ie{|tts| tts.nil? || tts['partitions'].nil?}
                             #no partitions in either user params or the target table

                             target_create_hql = "create table if not exists #{target_table_path} (#{field_defs.join(",")})"

                             target_insert_hql = "insert overwrite table #{target_table_path} select * from #{source_table_path};"

                             [target_create_hql,target_insert_hql].join(";")

                           elsif target_partitions.length > 0 and
                             target_table_stats.ie{|tts| tts.nil? || tts['partitions'] == target_partitions}
                             #partitions and no target table or same partitions in both target table and user params

                             target_set_hql = ["set hive.exec.dynamic.partition.mode=nonstrict",
                                               "set hive.exec.max.dynamic.partitions.pernode=1000",
                                               "set hive.exec.dynamic.partition=true",
                                               "set hive.exec.max.created.files = 200000",
                                               "set hive.max.created.files = 200000"].join(";")

                             target_create_hql = "create table if not exists #{target_table_path} (#{field_defs.join(",")}) " +
                                                 "partitioned by (#{partition_defs.join(",")})"

                             target_insert_hql = "insert overwrite table #{target_table_path} " +
                                                 "partition (#{partition_defs.join(",")}) " +
                                                 "select * from #{source_table_path};"

                             [target_set_hql, target_create_hql, target_insert_hql].join(";")

                           else
                             error_msg = "Incompatible partition specs: " +
                                         "target table:#{target_table_stats['partitions'].to_s}, " +
                                         "user_params:#{target_partitions.to_s}"
                             raise error_msg
                           end

      elsif source_dst.handler == 'gridfs'
        #tsv from sheet
        target_string = source_dst.read(user)
        target_string_filename = "000000_0"
        #create table for result, load result into it
        target_table_hql = ["create table #{target_table_path} (#{field_defs.join(",")})",
                            "load data local inpath '#{target_string_filename}' overwrite into table #{target_table_path};"].join(";")
        file_hash = {target_string_filename=>target_string}
        Hive.run(target_table_hql, cluster, node_user, file_hash)
      else
        raise "unsupported handler #{source_dst.handler}"
      end

      output_table_hql = ["drop table if exists #{output_path}",
                            "create table #{output_path} (#{field_defs.join(",")})",
                            "load data local inpath '#{out_string_filename}' overwrite into table #{output_path};"].join(";")

      command = source_dst.read(user)

      header_row,rows = tsv.split("\n").instance_eval{|a| [a.first,a[1..-1]]}
      #no rows, no write
      return true if header_row.to_s.length == 0 or rows.nil? or rows.length==0
      #make sure header row does not have forbidden terms or characters
      #and that tsv is encoded in UTF-8
      sane_header_row, sane_tsv = Hive.sanitize_table(header_row,rows)

      #hash array is easier to work with
      hash_array = sane_tsv.tsv_to_hash_array

      #use tsv to build field definitions
      field_defs = Hive.field_defs(hash_array)

      #convert all values to discovered datatypes
      sane_hash_array = Hive.sanitize_hash_array(hash_array, field_defs)

      table_statement = if partitions.length == 0
                          #if there are no partitions, drop the table
                          #and recreate
                          data_fields = field_defs.map{|fdef| "#{fdef.first} #{fdef.last}"}
                          %{drop table if exists #{db}.#{table};} +
                          %{create table if not exists #{db}.#{table} (#{data_fields.join(",")}) } +
                          %{row format delimited fields terminated by \"\\t\";}
                        else
                          data_fields = []
                          partition_fields = []
                          field_defs.each do |fdef|
                            if partitions.include?(fdef.first)
                              partition_fields << "#{fdef.first} #{fdef.last}"
                            else
                              data_fields << "#{fdef.first} #{fdef.last}"
                            end
                          end
                          %{set hive.exec.dynamic.partition.mode=nonstrict; } +
                          %{set hive.exec.max.dynamic.partitions.pernode=1000; } +
                          %{set hive.exec.dynamic.partition=true; } +
                          %{create table if not exists #{db}.#{table} (#{data_fields.join(",")}) } +
                          %{partitioned by (#{partition_fields.join(",")}) } +
                          %{row format delimited fields terminated by \"\\t\";}
                        end

      #drop, create, load data into temp table
      temp_data_fields = field_defs.map{|fdef| "#{fdef.first} #{fdef.last}"}
      temp_table_name = table.to_md5
      temp_table_statement = %{use #{Hive.temp_table_db};} +
                             %{drop table if exists #{temp_table_name};} +
                             %{create table #{temp_table_name} (#{temp_data_fields.join(",")}) } +
                             %{row format delimited fields terminated by \"\\t\"; } +
                             %{load data inpath 'input' overwrite into table #{temp_table_name}; }

      insert_statement = if partitions.length == 0
                           #determine the number of distinct partition columns
                           #of partition
                         else

                         end

      full_statement = table_statement + add_data_statement
      puts full_statement
      stdout,stderr = Hiver.sh(full_statement,dbuser)
      stdout = nil
      #this raises STDERR text if it is prefaced by a FAILED marker
      if stderr.to_s.downcase.index("failed") or stderr.to_s.downcase.index("killed")
        if stderr.length>1000
          if stderr.downcase.index("failed")
            raise stderr[stderr.downcase.index("failed")-1000..-1]
          elsif stderr.downcase.index("killed")
            raise stderr[stderr.downcase.index("killed")-1000..-1]
          end
        else
          raise stderr
        end
      else
        return true
      end
    end
  end
end
