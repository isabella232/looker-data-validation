require_relative 'queries'
require_relative 'data_validator'
require 'rspec/retry'
require 'yaml'

conf = YAML.load(File.read("config.yml"))
dashboard_ids = ENV['DASHBOARD_IDS'] ? ENV['DASHBOARD_IDS'].split(',') : conf['DASHBOARD_IDS']

look_ids = ENV['LOOK_IDS'] ? ENV['LOOK_IDS'].split('') : conf['LOOK_IDS']

RSpec.configure do |config|
  # show retry status in spec process
  config.verbose_retry = true
  # show exception that triggers a retry if verbose_retry is set to true
  config.display_try_failure_messages = true
  # seconds to wait between retries
  config.default_sleep_interval = 5
  # default retries if not set in example
  config.default_retry_count = 2
end

RSpec.describe 'Verify Dashboard data' do
  unless dashboard_ids.nil?
    dashboard_ids.each do |d_id|
      context "Dashboard id: #{d_id}" do
        all_query_ids = get_query_ids_from_dashboard(d_id)
        all_query_ids.each do |q_id|
          it "Query id: #{q_id}" do
            run_queries(q_id.to_s, d_id.to_s)
            verify_data(q_id.to_s)
          end
        end
      end
    end
  end
end

RSpec.describe "Verify Look data" do
  unless look_ids.nil?
    look_ids.each do |d_id|
      context "Look id: #{d_id}" do
        query_id = get_query_id_from_look(d_id)
        it "Query id: #{query_id}" do
          run_queries(query_id.to_s)
          verify_data(query_id.to_s)
        end
      end
    end
  end
end
