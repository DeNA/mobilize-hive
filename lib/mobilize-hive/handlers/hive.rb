module Mobilize
  module Hive
    def Hive.config
      Base.config('hive')
    end

    def Hive.exec_path(cluster)
      Hive.clusters[cluster]['exec_path']
    end

    def Hive.clusters
      Hive.config['clusters']
    end

    def Hive.run(command,cluster,user)
      filename = command.to_md5
      file_hash = {filename=>command}
      #silent mode so we don't have logs in stderr
      exec_command = "#{Hive.exec_path(cluster)} -S -f #{filename}"
      gateway_node = Hadoop.gateway_node(cluster)
      Ssh.run(gateway_node,exec_command,user,file_hash)
    end

    def Hive.write_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      target_path = params['target']
      user = params['user']
      source_dst = s.source_dsts.first
      #determine path for target
      cluster, db, table, partitions = Hive.resolve_path(target_path)
      node = Hadoop.gateway_node(cluster)
      if user and !Ssh.sudoers(node).include?(u.name)
        raise "#{u.name} does not have su permissions for #{node}"
      elsif user.nil? and Ssh.su_all_users(node)
        user = u.name
      end
      tsv = source_dst.read(user)
      out_string = Hive.write(cluster, db, table, partitions, in_string, user)
    end

    def Hive.sanitize_table(header_row,rows)
      sane_header_row = header_row.downcase.split("\t").map{|h|
        h == 'date' ? 'cdate' : h}.map{|h|
          h.gsub(/[^a-z0-9_ ]/,"").strip.gsub(" ","_").gsub(/_+/,"_")}.join("\t")
      sane_tsv = ([sane_header_row] + rows).join("\n").force_encoding("UTF-8")
      return [sane_header_row, sane_tsv]
    end

    def Hive.sanitize_hash_array(hash_array,field_defs)
      hash_array.each_with_index do |row,row_i|
        row.each do |fname, fvalue|
          if field_defs[fname] == "string"
            #do nothing
          elsif ["null","\\n","nil",""].include?(fvalue.to_s.downcase.strip)
            #standardize as NULL
            hash_array[row_i][fname] = "NULL"
          elsif field_defs[fname]=="bigint"
            hash_array[row_i][fname] = fvalue.to_i.to_s
          else
            raise "Invalid value row #{(row_i+1).to_s} #{fname}: #{fvalue}"
          end
        end
      end
      return hash_array
    end

    def Hive.field_defs(hash_array)
      #create a hash for fields; keys are names and values are datatypes, nil at first
      field_defs = {}
      hash_array.first.keys.each{|k| field_defs[k] = nil}
      #discover datatypes by going through each column
      hash_array.each do |row|
        row.each do |fname, fvalue|
          if field_defs[fname] == "string"
            #do nothing
          elsif ["null","\\n","nil",""].include?(fvalue.to_s.downcase.strip)
            #unknown, do nothing
          elsif fvalue.is_time?
            field_defs[fname] = "string"
          elsif fvalue.to_s.is_fixnum?
            #do bigint so we don't have to deal w crap
            field_defs[fname] = "bigint"
          elsif ['true','false'].include?(fvalue.to_s.downcase.strip)
            #tinyint(1) (boolean)
            field_defs[fname] = "string"
          else
            field_defs[fname] = "string"
          end
        end
      end
      #assume string for any fields with no value
      field_defs.each{|k,v| field_defs[k] = "string" unless field_defs[k]}
      return field_defs
    end

    def Hive.write(cluster, db, table, partitions, tsv, user)
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



    def Hive.read_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      source_path = params['source']
      user = params['user']
      #check for source in hdfs format
      source_cluster, source_cluster_path = Hive.resolve_path(source_path)
      raise "unable to resolve source path" if source_cluster.nil?

      node = Hadoop.gateway_node(source_cluster)
      if user and !Ssh.sudoers(node).include?(u.name)
        raise "#{u.name} does not have su permissions for #{node}"
      elsif user.nil? and Ssh.su_all_users(node)
        user = u.name
      end

      source_path = "#{source_cluster}#{source_cluster_path}"
      out_string = Hive.read(source_path,user).to_s
      out_url = "hdfs://#{Hadoop.output_cluster}#{Hadoop.output_dir}hdfs/#{stage_path}/out"
      Dataset.write_by_url(out_url,out_string,Gdrive.owner_name)
      out_url
    end

    def Hive.write_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      source_path = params['source']
      target_path = params['target']
      user = params['user']
      #check for source in hdfs format
      source_cluster, source_cluster_path = Hive.resolve_path(source_path)
      if source_cluster.nil?
        #not hdfs
        gdrive_slot = Gdrive.slot_worker_by_path(stage_path)
        #return blank response if there are no slots available
        return nil unless gdrive_slot
        source_dst = s.source_dsts(gdrive_slot).first
        Gdrive.unslot_worker_by_path(stage_path)
      else
        source_path = "#{source_cluster}#{source_cluster_path}"
        source_dst = Dataset.find_or_create_by_handler_and_path("hdfs",source_path)
      end

      #determine cluster for target
      target_cluster, target_cluster_path = Hive.resolve_path(target_path)
      raise "unable to resolve target path" if target_cluster.nil?

      node = Hadoop.gateway_node(target_cluster)
      if user and !Ssh.sudoers(node).include?(u.name)
        raise "#{u.name} does not have su permissions for #{node}"
      elsif user.nil? and Ssh.su_all_users(node)
        user = u.name
      end

      target_path = "#{target_cluster}#{target_cluster_path}"
      in_string = source_dst.read(user)
      out_string = Hive.write(target_path,in_string,user)

      out_url = "hdfs://#{Hadoop.output_cluster}#{Hadoop.output_dir}hdfs/#{stage_path}/out"
      Dataset.write_by_url(out_url,out_string,Gdrive.owner_name)
      out_url
    end

    def Hive.copy_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      source_path = params['source']
      target_path = params['target']
      user = params['user']
      #check for source in hdfs format
      source_cluster, source_cluster_path = Hive.resolve_path(source_path)
      raise "unable to resolve source path" if source_cluster.nil?

      #determine cluster for target
      target_cluster, target_cluster_path = Hive.resolve_path(target_path)
      raise "unable to resolve target path" if target_cluster.nil?

      node = Hadoop.gateway_node(source_cluster)
      if user and !Ssh.sudoers(node).include?(u.name)
        raise "#{u.name} does not have su permissions for #{node}"
      elsif user.nil? and Ssh.su_all_users(node)
        user = u.name
      end

      source_path = "#{source_cluster}#{source_cluster_path}"
      target_path = "#{target_cluster}#{target_cluster_path}"
      out_string = Hive.copy(source_path,target_path,user)

      out_url = "hdfs://#{Hadoop.output_cluster}#{Hadoop.output_dir}hdfs/#{stage_path}/out"
      Dataset.write_by_url(out_url,out_string,Gdrive.owner_name)
      out_url
    end

    def Hive.read_by_dataset_path(dst_path,user)
      Hive.read(dst_path,user)
    end

    def Hive.write_by_dataset_path(dst_path,string,user)
      Hive.write(dst_path,string,user)
    end
  end
end
