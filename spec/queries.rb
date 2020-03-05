require 'looker-sdk'
require 'fileutils'
require 'rack'
require 'yaml'

require_relative 'data_validator'
require_relative 'logger'

RESULTS_DIR = "#{Dir.pwd}/results".freeze
conf = YAML.load(File.read("config.yml"))

SDK = LookerSDK::Client.new(
  :client_id => conf['CLIENT_ID'],
  :client_secret => conf['CLIENT_SECRET'],
  :api_endpoint => conf['API_BASEURL'] + ':19999/api/3.1',
  :connection_options => { :ssl => { :verify => true }, :request => { :timeout => 60 * 60, :open_timeout => 300 } },
)

def get_query_ids_from_dashboard(dashboard_id)
  all_query_ids = []

  elems = SDK.dashboard(dashboard_id, :fields => 'dashboard_elements')[:dashboard_elements]
  elems.each do |elem|
    unless elem[:type] == 'text'
      if elem[:query_id].nil?
        query_id = elem[:result_maker][:query_id]
      else
        query_id = elem[:query_id]
      end
      # another check for null values
      unless query_id.nil?
        all_query_ids.push(query_id)
      end
    end
  end
  return all_query_ids
end

def get_query_id_from_look(look_id)
  query_id = SDK.look(look_id, :fields => 'query_id')[:query_id]
  logger.info("Query id for look #{look_id} is: " + query_id.to_s)
  return query_id
end

def get_query_body(query_id)
  old_query_body = SDK.query(query_id).to_attrs

  # remove all null values to avoid server error
  old_query_body.compact!

  # unnecessary fields, need to remove or url gets too long and results in HTTP 501
  query_properties = [:client_id, :expanded_share_url, :url, :can, :filter_config, :vis_config, :slug, :runtime]

  query_properties.each do |prop|
    old_query_body.delete(prop)
  end

  old_query_body = apply_custom_sort(old_query_body)

  if old_query_body[:filters]
    # add filters to the new param 'f', required for running run_url_encoded_query
    old_query_body[:filters].each do |key, value|
      old_query_body["f[#{key}]"] = value
    end
    # finally remove filters
    old_query_body.delete(:filters)
  end

  if old_query_body[:dynamic_fields]
    # need to remove this after all the above checks, else query gets too long and server throws a 500
    # old_query_body.delete(:dynamic_fields)
  end

  query_properties = [:fields, :subtotals, :pivots, :fill_fields]
  query_properties.each do |prop|
    old_query_body[prop] = old_query_body[prop].join(',') if old_query_body[prop]
  end
  return old_query_body
end

def url_encoded_query(query_id, use_calcite)
  old_query_body = get_query_body(query_id)

  logger.info('Explore url is: ' + old_query_body[:share_url])

  begin
    if use_calcite
      logger.info('Running new query using run_url_encoded_query with Calcite enabled *******************')
      logger.debug('Request body is: ' + old_query_body.to_s)
      new_query_data = SDK.run_url_encoded_query(old_query_body.delete(:model), old_query_body.delete(:view), 'csv', query: old_query_body.merge('force_calcite' => true, 'cache' => false))
      File.write("#{RESULTS_DIR}/data/new_query_data_id_#{query_id}.txt", new_query_data)
    else
      logger.info('Running new query using run_url_encoded_query with Calcite disabled *******************')
      logger.debug('Request body is: ' + old_query_body.to_s)
      old_query_data = SDK.run_url_encoded_query(old_query_body.delete(:model), old_query_body.delete(:view), 'csv', query: old_query_body.merge('force_calcite' => false, 'cache' => false))
      File.write("#{RESULTS_DIR}/data/old_query_data_id_#{query_id}.txt", old_query_data)
    end
  rescue StandardError => e
    logger.info("#{e.inspect}")
    # get calcite SQL on error, will be executed unless a server error
    get_calcite_sql_query(query_id) unless use_calcite
    raise 'Error getting data for query id: ' + query_id + ' ' + "#{e.inspect}"
  end
end

def get_calcite_sql_query(query_id)
  query_body = get_query_body(query_id)
  begin
    logger.info('Getting sql')
    new_query_sql = SDK.run_url_encoded_query(query_body.delete(:model), query_body.delete(:view), 'sql', query: query_body.merge('force_calcite' => true))
    File.write("#{RESULTS_DIR}/sql/calcite_sql_id_#{query_id}.txt", new_query_sql)
  rescue StandardError => e
    logger.info("#{e.inspect}")
    raise 'Error getting SQL for query id: ' + query_id + ' ' + "#{e.inspect}"
  end
end

def has_table_calc?(query_body, field)
  # remove asc or desc
  field.slice! /\s..../
  dynamic_fields = query_body[:dynamic_fields].to_s

  if dynamic_fields.include? "table_calculation\":\"#{field}\""
    logger.info('Sort has table calculation, applying additional magic')
    true
  end
end

def apply_custom_sort(old_query_body)
  sort_value = ''
  value = ''

  if old_query_body[:sorts]
    sort_value = old_query_body[:sorts][0].to_s
    value = sort_value.dup

    if sort_value.include? 'asc'
      old_query_body[:fields].each do |key|
        unless old_query_body[:sorts].include?(key) || old_query_body[:sorts].include?(key + ' asc')
          old_query_body[:sorts].push(key + ' asc')
        end
      end
    elsif sort_value.include? 'desc'
      old_query_body[:fields].each do |key|
        unless old_query_body[:sorts].include?(key) || old_query_body[:sorts].include?(key + ' desc')
          old_query_body[:sorts].push(key + ' desc')
        end
      end
    else
      # sort without asc/desc
      old_query_body[:fields].each do |key|
        unless old_query_body[:sorts].include?(key)
          old_query_body[:sorts].push(key)
        end
      end
    end
  end

  # create sort field if it doesn't exist and all dimensions to it
  unless old_query_body[:sorts]
    old_query_body[:sorts] = []
    old_query_body[:fields].each do |key|
      old_query_body[:sorts].push(key)
    end
    sort_value = old_query_body[:sorts][0].to_s
    value = sort_value.dup
  end

  # push the first field to the end if used for table calculation
  if has_table_calc?(old_query_body, value) && sort_value
    sort_value = old_query_body[:sorts].delete(sort_value)
    old_query_body[:sorts].push(sort_value)
  end

  old_query_body[:sorts] = old_query_body[:sorts].join(',')
  return old_query_body
end

def run_queries(query_id, dashboard_id)
  logger.info("-------------- Starting test for Dashboard: #{dashboard_id} Query id: #{query_id} --------------")
  puts "-------------- Starting test for Dashboard: #{dashboard_id} Query id: #{query_id} --------------"
  url_encoded_query(query_id, false)
  url_encoded_query(query_id, true)
end
