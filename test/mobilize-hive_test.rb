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

    hive_1_hql_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1.hql",gdrive_slot)
    [hive_1_hql_sheet].each {|s| s.delete if s}
    hive_1_hql_sheet = Mobilize::Gsheet.find_or_create_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1.hql",gdrive_slot)
    hive_1_hql_tsv = File.open("#{Mobilize::Base.root}/test/hive_test_1.hql").read
    hive_1_hql_sheet.write(hive_1_hql_tsv,Mobilize::Gdrive.owner_name)

    jobs_sheet = r.gsheet(gdrive_slot)

    test_job_rows = ::YAML.load_file("#{Mobilize::Base.root}/test/hive_job_rows.yml")
    test_job_rows.map{|j| r.jobs(j['name'])}.each{|j| j.delete if j}
    jobs_sheet.add_or_update_rows(test_job_rows)

    hive_1_stage_2_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1_stage_2.out",gdrive_slot)
    [hive_1_stage_2_target_sheet].each{|s| s.delete if s}
    hive_1_stage_3_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1_stage_3.out",gdrive_slot)
    [hive_1_stage_3_target_sheet].each{|s| s.delete if s}
    hive_2_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_2.out",gdrive_slot)
    [hive_2_target_sheet].each{|s| s.delete if s}
    hive_3_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_3.out",gdrive_slot)
    [hive_3_target_sheet].each{|s| s.delete if s}

    puts "job row added, force enqueued requestor, wait for stages"
    r.enqueue!
    wait_for_stages(1200)

    puts "jobtracker posted data to test sheet"
    hive_1_stage_2_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1_stage_2.out",gdrive_slot)
    hive_1_stage_3_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_1_stage_3.out",gdrive_slot)
    hive_2_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_2.out",gdrive_slot)
    hive_3_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/hive_test_3.out",gdrive_slot)

    assert hive_1_stage_2_target_sheet.read(u.name).length == 219
    assert hive_1_stage_3_target_sheet.read(u.name).length > 3
    assert hive_2_target_sheet.read(u.name).length == 599
    assert hive_3_target_sheet.read(u.name).length == 347
  end

  def wait_for_stages(time_limit=600,stage_limit=120,wait_length=10)
    time = 0
    time_since_stage = 0
    #check for 10 min
    while time < time_limit and time_since_stage < stage_limit
      sleep wait_length
      job_classes = Mobilize::Resque.jobs.map{|j| j['class']}
      if job_classes.include?("Mobilize::Stage")
        time_since_stage = 0
        puts "saw stage at #{time.to_s} seconds"
      else
        time_since_stage += wait_length
        puts "#{time_since_stage.to_s} seconds since stage seen"
      end
      time += wait_length
      puts "total wait time #{time.to_s} seconds"
    end

    if time >= time_limit
      raise "Timed out before stage completion"
    end
  end



end
