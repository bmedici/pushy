module PushyDaemon
  # class ShouterResponseError       < StandardError; end
  # class ShouterChannelClosed       < StandardError; end
  # class ShouterPreconditionFailed  < StandardError; end
  # class ShouterInterrupted         < StandardError; end
  class ConsumerError              < StandardError; end
  class ConsumerRuleMissing        < StandardError; end
  class ConsumerSubscribeError     < StandardError; end

  class Consumer < BmcDaemonLib::MqConsumer
    #include ::NewRelic::Agent::Instrumentation::ControllerInstrumentation
    include Shared::HmacSignature

    def initialize(channel, rule_name, rule)
      # Init MqConsumer
      log_pipe :consumer
      super

      # Init
      @queue = nil
      @rule = rule
      @rule_name = rule_name
    end

  protected

    def log_context
      {
        rule: @rule_name
      }
    end

    # Handle the reception of a message on a queue
    def handle_message context, metadata, delivery_info, message = {}
      # Prepare data
      headers = metadata.headers || {}

      # Relay data if needed
      handle_relay context, message, headers

      # Handle errors and acknowledgments
      # log_debug "handle_message : channel[#{@channel.inspect}]"
      rescue Exception => e
        log_error "handle_message: EXCEPTION: #{e.message}, #{e.inspect}", e.backtrace
        channel_ackit(message[:tag], false)
      rescue StandardError => e
        log_error "handle_message: unknown: #{e.message}, #{e.inspect}", e.backtrace
        channel_ackit(message[:tag], false)
      else
        channel_ackit(message[:tag], true)
    end

  private

    def handle_relay context, message, headers
      # Check we have a valid @rule
      raise ConsumerRuleMissing unless @rule.is_a? Hash

      # Check if we need to relay anything
      unless @rule[:relay]
        # log_debug "handle_relay: no [relay] URL"
        return
      end

      # Prepare stuff
      relay_auth  = @rule[:auth].to_s
      relay_url   = URI(@rule[:relay]).to_s
      request_id  = identifier(6)
      request_prefix = "handle_relay [#{request_id}] "

      # Build payload
      request_infos = {
        topic:    message[:topic],
        route:    message[:rkey],
        sent_at:  headers['sent_at'],
        sent_by:  headers['sent_by'],
        context:  context,
        data:     message[:data],
        }
      request_body = JSON.pretty_generate(request_infos)

      # Build request headers
      headers = {
        content_type: :json,
        accept: :json,
        user_agent: BmcDaemonLib::Conf.generate_user_agent,
        }

      # Set custom headers if provided
      @rule[:headers].each do |header_name, header_value|
        log_debug "set header [#{header_name}] to [#{header_value}]"
        headers[header_name] = header_value.to_s
      end if @rule[:headers].is_a? Enumerable

      # Compute: payload MD5, HMAC signature
      headers_md5 headers, request_body
      headers_sign headers, @rule[:sign]

      # Build final request
      request = RestClient::Request.new url: relay_url,
        method: :post,
        payload: request_body,
        headers: headers

      # Execute request
      log_message MSG_RLAY, request_id, relay_url, request_infos, request.processed_headers
      response = request.execute

      # Handle exceptions
      rescue RestClient::ExceptionWithResponse, URI::InvalidURIError, RestClient::InternalServerError => e
        log_error "#{request_prefix} rest-client: #{e.message}"
      rescue ApiAuth::ApiAuthError, ApiAuth::UnknownHTTPRequest => e
        log_error "#{request_prefix} api-auth: #{e.message}"
      rescue Errno::ECONNREFUSED => e
        log_error "#{request_prefix} connection refused: #{e.message}"
      rescue StandardError => e
        log_error "#{request_prefix} unknown: #{e.message}, #{e.inspect}", e.backtrace
      else
        log_info "#{request_prefix} received [#{response.body}]"
    end

    def channel_ackit tag, success=true
      # log_debug "channel_ackit[#{channel}.#{tag}] #{@channel.inspect}"
    #   if success
    #     log_debug "channel_ackit[#{@channel.id}.#{tag}]: ACK"
    #     @channel.ack(tag)
    #   else
    #     log_debug "channel_ackit[#{@channel.id}.#{tag}]: NACK"
    #     @channel.nack(tag)
    #   end

    # rescue Bunny::ChannelAlreadyClosed => ex
    #   error "channel_ackit[#{@channel.id}.#{tag}]: exception: ChannelAlreadyClosed"

    # rescue StandardError => ex
    #   log_debug "channel_ackit[#{@channel.id}.#{tag}]: exception: #{ex.inspect}"
    #   # fail PushyDaemon::ConsumerSubscribeError, "unhandled (#{e.inspect})"

    # else
    #   log_debug "channel_ackit[#{@channel.id}.#{tag}]: done"
    end

    # NewRelic instrumentation
    #add_transaction_tracer :receive,        category: :task
    #add_transaction_tracer :propagate,      category: :task
  end
end
