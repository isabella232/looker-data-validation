require 'logger'

def logger
  logger = Logger.new("#{RESULTS_DIR}/run_log.log")

  logger.level = ENV['LOG_LEVEL'] || Logger::DEBUG
  logger.formatter = proc do |severity, datetime, progname, msg|
    "#{severity}: #{msg}\n"
  end
  logger
end
