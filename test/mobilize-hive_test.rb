require 'test_helper'

describe "Mobilize" do

  def before
    puts 'nothing before'
  end

  # enqueues 4 workers on Resque
  it "runs integration test" do

    puts "restart workers"
    Mobilize::Jobtracker.restart_workers!

    gdrive_slot = Mobilize::Gdrive.owner_email
    puts "create user 'mobilize'"
    user_name = gdrive_slot.split("@").first
    u = Mobilize::User.where(:name=>user_name).first
    r = u.runner

    puts "add test_source data"
    hive_1_in_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1.in",gdrive_slot)
    [hive_1_in_sheet].each {|s| s.delete if s}
    hive_1_in_sheet = Mobilize::Gsheet.find_or_create_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1.in",gdrive_slot)
    hive_1_in_tsv = YAML.load_file("#{Mobilize::Base.root}/test/hive_test_1_in.yml").hash_array_to_tsv
    hive_1_in_sheet.write(hive_1_in_tsv,Mobilize::Gdrive.owner_name)

    hive_1_schema_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1.schema",gdrive_slot)
    [hive_1_schema_sheet].each {|s| s.delete if s}
    hive_1_schema_sheet = Mobilize::Gsheet.find_or_create_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1.schema",gdrive_slot)
    hive_1_schema_tsv = YAML.load_file("#{Mobilize::Base.root}/test/hive_test_1_schema.yml").hash_array_to_tsv
    hive_1_schema_sheet.write(hive_1_schema_tsv,Mobilize::Gdrive.owner_name)

    jobs_sheet = r.gsheet(gdrive_slot)

    test_job_rows = ::YAML.load_file("#{Mobilize::Base.root}/test/hive_job_rows.yml")
    jobs_sheet.add_or_update_rows(test_job_rows)

    hive_1_stage_2_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1_stage_2.out",gdrive_slot)
    [hive_1_stage_2_target_sheet].each{|s| s.delete if s}
    hive_1_stage_3_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1_stage_3.out",gdrive_slot)
    [hive_1_stage_3_target_sheet].each{|s| s.delete if s}
    hive_2_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_2.out",gdrive_slot)
    [hive_2_target_sheet].each{|s| s.delete if s}
    hive_3_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_3.out",gdrive_slot)
    [hive_3_target_sheet].each{|s| s.delete if s}

    puts "job row added, force enqueued requestor, wait 150s"
    r.enqueue!
    sleep 180

    puts "jobtracker posted data to test sheet"
    hive_1_stage_2_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1_stage_2.out",gdrive_slot)
    hive_1_stage_3_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1_stage_3.out",gdrive_slot)
    hive_2_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_2.out",gdrive_slot)
    hive_3_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_3.out",gdrive_slot)

    [hive_1_stage_2_target_sheet,
     hive_1_stage_3_target_sheet,
     hive_2_target_sheet,
     hive_3_target_sheet].each do |s|
      assert s.to_tsv.length == 499
      sleep 1 
    end
  end

end
