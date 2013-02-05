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
      exec_command = "#{Hive.exec_path(cluster)} -S -f #{filename}"
      gateway_node = Hadoop.gateway_node(cluster)
      Ssh.run(gateway_node,exec_command,user,file_hash)
    end

    def Hive.write(path,string,user)
      file_hash = {'file.txt'=>string}
      cluster = Hive.resolve_path(path).first
      Hive.rm(path,user) #remove old one if any
      write_command = "dfs -copyFromLocal file.txt '#{Hive.namenode_path(path)}'"
      Hadoop.run(write_command,cluster,user,file_hash)
      return Hive.namenode_path(path)
    end

    def Hive.copy(source_path,target_path,user)
      Hive.rm(target_path,user) #remove to_path
      source_cluster = Hive.resolve_path(source_path).first
      command = "dfs -cp '#{Hive.namenode_path(source_path)}' '#{Hive.namenode_path(target_path)}'"
      #copy operation implies access to target_url from source_cluster
      Hadoop.run(command,source_cluster,user)
      return Hive.namenode_path(target_path)
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
