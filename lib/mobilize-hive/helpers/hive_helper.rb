module Mobilize
  module Hive
    def self.config
      Base.config('hive')
    end

    def self.exec_path(cluster)
      self.clusters[cluster]['exec_path']
    end

    def self.output_db(cluster)
      self.clusters[cluster]['output_db']
    end

    def self.output_db_user(cluster)
      output_db_node = Hadoop.gateway_node(cluster)
      output_db_user = Ssh.host(output_db_node)['user']
      output_db_user
    end

    def self.clusters
      self.config['clusters']
    end

    def self.slot_ids(cluster)
      (1..self.clusters[cluster]['max_slots']).to_a.map{|s| "#{cluster}_#{s.to_s}"}
    end

    def self.slot_worker_by_cluster_and_path(cluster,path)
      working_slots = Mobilize::Resque.jobs.map{|j| begin j['args'][1]['hive_slot'];rescue;nil;end}.compact.uniq
      self.slot_ids(cluster).each do |slot_id|
        unless working_slots.include?(slot_id)
          Mobilize::Resque.set_worker_args_by_path(path,{'hive_slot'=>slot_id})
          return slot_id
        end
      end
      #return false if none are available
      return false
    end

    def self.unslot_worker_by_path(path)
      begin
        Mobilize::Resque.set_worker_args_by_path(path,{'hive_slot'=>nil})
        return true
      rescue
        return false
      end
    end

    def self.databases(cluster,user_name)
      self.run(cluster,"show databases",user_name)['stdout'].split("\n")
    end

    def self.default_params
      time = Time.now.utc
      {
       '$utc_date'=>time.strftime("%Y-%m-%d"),
       '$utc_time'=>time.strftime("%H:%M"),
      }
    end
  end
end

