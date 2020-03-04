require_relative 'queries.rb'
require 'digest'

def numeric?(value)
  true if Float(value) rescue false
end

def getSha1Digest(file_location)
  sha1 = Digest::SHA1.file file_location
  return sha1
end

def verify_data(query_id)
  old_data_file = File.open("#{RESULTS_DIR}/data/old_query_data_id_#{query_id}.txt")
  new_data_file = File.open("#{RESULTS_DIR}/data/new_query_data_id_#{query_id}.txt")

  old_data_lines = old_data_file.readlines
  new_data_lines = new_data_file.readlines

  # temporary check, will be removed later since queries can return empty results
  if File.size?(old_data_file).nil? || File.size?(new_data_file).nil?
    raise 'One of the files are empty'
  end

  # compare files using SHA1, else read both to find difference
  unless getSha1Digest(old_data_file) == getSha1Digest(new_data_file)
    logger.info('SHA1 not matching, reading file')
    line_num = 0
    until line_num == old_data_lines.length
      line_old_value = old_data_lines[line_num].split(',')
      line_new_value = new_data_lines[line_num].split(',')

      line_old_value.zip(line_new_value).each do |v1, v2|
        begin
          if numeric?(v1) && numeric?(v2)
            # check numbers are within the tolerance level
            unless within_tolerance?(v1, v2)
              raise "Following numbers not matching and are above the tolerance rate on line number:#{line_num + 1} \n" \
                      + v1 + "\n with: \n" + v2
            end
          else
            unless v1 == v2
              raise "Following values not matching on line number:#{line_num + 1} \n" + v1 \
                      + "\n with: \n" + v2
            end
          end
        rescue => exception
          # get calcite SQL on error
          get_calcite_sql_query(query_id)
          logger.debug(exception)
          raise exception
        end
      end
      line_num += 1
    end
  end
end

def within_tolerance?(value1, value2)
  # should likely not be changed
  tolerance_rate_num = 10
  tolerance_rate_percentage = (ENV['TOLERANCE_RATE_PERCENTAGE'] || 10).to_i

  calculated_error_num = value1.to_f - value2.to_f
  calculated_error_percentage = (calculated_error_num.abs / (value1.to_f + value2.to_f)) * 100

  # check if calculated difference is <= 10, this is intended for smaller errors
  # or for larger errors a fixed tolerance range doesn't work, thus we use percentage
  if calculated_error_num.abs <= tolerance_rate_num || calculated_error_percentage.abs <= tolerance_rate_percentage
    return true
  end
end
