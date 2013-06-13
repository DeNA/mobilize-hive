require 'test_helper'
describe "Mobilize" do
  # enqueues 4 workers on Resque
  it "runs integration test" do

    puts "restart workers"
    Mobilize::Jobtracker.restart_workers!

    u = TestHelper.owner_user
    r = u.runner
    user_name = u.name
    gdrive_slot = u.email

    puts "add test data"
    ["hive1.in","hive4_stage1.in","hive4_stage2.in","hive1.schema","hive1.sql"].each do |fixture_name|
      target_url = "gsheet://#{r.title}/#{fixture_name}"
      TestHelper.write_fixture(fixture_name, target_url, 'replace')
    end

    puts "add/update jobs"
    u.jobs.each{|j| j.stages.each{|s| s.delete}; j.delete}
    jobs_fixture_name = "integration_jobs"
    jobs_target_url = "gsheet://#{r.title}/jobs"
    TestHelper.write_fixture(jobs_fixture_name, jobs_target_url, 'update')

    puts "job rows added, force enqueue runner, wait for stages"
    #wait for stages to complete
    expected_fixture_name = "integration_expected"
    Mobilize::Jobtracker.stop!
    r.enqueue!
    TestHelper.confirm_expected_jobs(expected_fixture_name,3600)

    puts "update job status and activity"
    r.update_gsheet(gdrive_slot)

    puts "check posted data"
    assert TestHelper.check_output("gsheet://#{r.title}/hive1_stage2.out", 'min_length' => 219) == true
    assert TestHelper.check_output("gsheet://#{r.title}/hive1_stage3.out", 'min_length' => 3) == true
    assert TestHelper.check_output("gsheet://#{r.title}/hive2.out", 'min_length' => 599) == true
    assert TestHelper.check_output("gsheet://#{r.title}/hive3.out", 'min_length' => 347) == true
    assert TestHelper.check_output("gsheet://#{r.title}/hive4.out", 'min_length' => 432) == true
  end
end
