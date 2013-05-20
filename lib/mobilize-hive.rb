require "mobilize-hive/version"
require "mobilize-hdfs"

module Mobilize
  module Hive
    def Hive.home_dir
      File.expand_path('..',File.dirname(__FILE__))
    end
  end
end
require "mobilize-hive/handlers/hive"
