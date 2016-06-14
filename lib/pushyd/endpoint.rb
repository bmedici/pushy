require 'bunny'
require "securerandom"

module PushyDaemon
  class EndpointConnexionContext    < StandardError; end
  class EndpointConnectionError     < StandardError; end
  class EndpointSubscribeContext    < StandardError; end
  class EndpointSubscribeError      < StandardError; end

  class Endpoint

    def initialize
      # Prepare logger
      init_logger Conf[:logs]

      # Done
      info "endpoint initialized"
      # loop do
      #   info "info"
      #   info ["info1", "info2", "info3"]
      #   error "error"
      #   debug "debug"
      #   sleep 1
      # end
    end

  protected

    def init_logger logconf
      # Check structure conformity or set it to an empty hash
      logconf = {} unless logconf.is_a? Hash

      # Extract context
      logconf ||= {}
      logfile   = logconf[:file]
      loglevel  = logconf[:level]
      me        = self.class.name

      # Prepare logger (may be NIL > won't output anything)
      @logger = Logger.new(logfile, LOG_ROTATION)
      @logger.formatter = Formatter

      # Set progname
      @logger.progname = sprintf(LOG_FORMAT_PROGNAME, Process.pid, me.split('::').last)

      # Set expected level
      @logger.level = case loglevel
      when "debug"
        Logger::DEBUG
      when "info"
        Logger::INFO
      when "warn"
        Logger::WARN
      else
        Logger::INFO
      end

      # Announce on STDOUT we're now logging to file
      if logfile
        puts "#{self.class} logging loglevel [#{loglevel} > #{@logger.level}] to [#{logfile}]"
      else
        puts "#{self.class} logging disabled"
      end
    end

    def info message, lines = []
      @logger.info message
      debug_lines lines
    end

    def error messages
      @logger.error messages
    end

    def debug messages
      @logger.debug messages
    end

    def log_message msg_way, msg_exchange, msg_key, msg_body = [], msg_attrs = {}
      # Message header
      @logger.info sprintf("%3s %-15s %s", msg_way, msg_exchange, msg_key)

      # Body lines
      if msg_body.is_a?(Enumerable) && !msg_body.empty?
        body_json = JSON.pretty_generate(msg_body)
        debug_lines body_json.lines
      end

      # Attributes lines
      log_lines msg_attrs
    end

    # Start connexion to RabbitMQ
    def connect_channel busconf
      fail PushyDaemon::EndpointConnexionContext, "invalid bus host/port" unless (busconf.is_a? Hash) &&
        busconf[:host] && busconf[:port]

      info "connecting to #{busconf[:host]} port #{busconf[:port]}"
      conn = Bunny.new host: busconf[:host].to_s,
        port: busconf[:port].to_i,
        user: busconf[:user].to_s,
        pass: busconf[:pass].to_s,
        heartbeat: :server,
        logger: @logger
      conn.start

      # Create channel
      channel = conn.create_channel

    rescue Bunny::TCPConnectionFailedForAllHosts, Bunny::AuthenticationFailureError, AMQ::Protocol::EmptyResponseError  => e
      fail PushyDaemon::EndpointConnectionError, "error connecting (#{e.class})"
    rescue StandardError => e
      fail PushyDaemon::EndpointConnectionError, "unknow (#{e.inspect})"
    else
      return channel
    end

    # Declare or return the exchange for this topic
    def channel_exchange topic
      @exchanges ||= {}
      @exchanges[topic] ||= @channel.topic(topic, durable: true, persistent: true)
    end

    # Subscribe to interesting topic/routes and bind a listenner
    def channel_subscribe rule
      # Check information
      rule_name = rule[:name].to_s
      rule_topic = rule[:topic].to_s
      rule_routes = rule[:routes].to_s.split(' ')
      rule_queue = "#{Conf.name}-#{rule[:name]}"
      fail PushyDaemon::EndpointSubscribeContext, "rule [#{rule_name}] lacking topic" unless rule_topic
      fail PushyDaemon::EndpointSubscribeContext, "rule [#{rule_name}] lacking routes" if rule_routes.empty?

      # Create queue for this rule (remove it beforehand)
      #conn.create_channel.queue_delete(rule_queue_name)
      queue = @channel.queue(rule_queue, auto_delete: false, durable: true)

      # Bind each route from this topic-exchange
      topic_exchange = channel_exchange(rule_topic)
      rule_routes.each do |route|
        # Bind exchange to queue
        queue.bind topic_exchange, routing_key: route
        info "subscribe: bind [#{rule_topic}/#{route}] \t> #{rule_queue}"

        # Add row to config table
        # ["rule", "topic", "route", "relay", "queue", "description"]
        @table.add_row [rule_name, rule_topic, route, rule[:relay].to_s, rule_queue, rule[:title].to_s ]
      end

      # Subscribe to our new queue
      queue.subscribe(block: false, manual_ack: PROXY_USE_ACK, message_max: PROXY_MESSAGE_MAX) do |delivery_info, metadata, payload|

        # Handle the message
        handle_message rule, delivery_info, metadata, payload

      end

    rescue Bunny::PreconditionFailed => e
      fail PushyDaemon::EndpointSubscribeError, "PreconditionFailed: [#{rule_topic}] code(#{e.channel_close.reply_code}) message(#{e.channel_close.reply_text})"

    rescue StandardError => e
      fail PushyDaemon::EndpointSubscribeError, "unhandled (#{e.inspect})"

    end

    def handle_message rule, delivery_info, metadata, payload
    end

  private

    def debug_lines lines
      if lines.is_a? Array
        @logger.debug lines.map{ |line| sprintf(LOG_FORMAT_ARRAY, line) }
      elsif lines.is_a? Hash
        @logger.debug lines.map{ |key, value| sprintf(LOG_FORMAT_HASH, key, value) }
      end
    end

  end
end
