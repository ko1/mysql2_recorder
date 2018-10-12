require 'yaml'
require 'mysql2'
require 'digest/md5'

module DetArray
  def sample(*)
    srand(0)
    super
  end

  def shuffle(*)
    srand(0)
    super
  end
end

class Array
  prepend DetArray
end

module Mysql2
  class QueryCache
    class Rack
      def initialize app
        p enabled: self.class if $VERBOSE
        @app = app
      end

      def call env
        name = File.join('rack', env["PATH_INFO"])
        query = env['QUERY_STRING']
        name +=  ':' + Digest::MD5.hexdigest(query) unless query.empty?

        QueryCache.use name, query do
          srand(0)
          @app.call(env)
        end
      end
    end

    CURRENT_QC = Struct.new(:current).new(nil)

    QC_CACHE = {}

    def debug_msg
      if $VERBOSE
        STDERR.puts "Mysql2::QueryCache: #{yield}"
      end
    end

    def self.prepare name, query
      if qc = QC_CACHE[name]
        qc
      else
        qc = QueryCache.new(name, query)
      end
    end

    def self.use name, query
      if CURRENT_QC.current
        raise "Mysql2::QueryCache already used by #{$qc}"
      else
        qc = prepare(name, query)
        CURRENT_QC.current = qc
        qc.debug_msg{ "use #{qc}" }
        yield
      end
    ensure
      qc = CURRENT_QC.current
      CURRENT_QC.current = nil
      qc.finish!
      QC_CACHE[name] = qc
    end

    def self.current
      CURRENT_QC.current
    end

    def self.recording?
      CURRENT_QC.recording
    end

    attr_reader :recording

    MATCH_OPT_KEYS = [:as, :symbolize_keys, :encoding, :database]

    def initialize name, query
      @name = name
      @storage_name = "qc_records/#{name}.yaml"

      debug_msg{ "initialize: #{self}" }

      if File.exist? @storage_name
        @data = YAML.load_file(@storage_name)
        @recording = false
      else
        @recording = true
        @data = {query: query}
      end

    end

    def to_s
      "<Mysql2::QueryCache name:#{@name}#{@recording ? ' (recording)' : ''}>"
    end

    def sql_key sql
      sql.gsub(/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+/){|e| e.gsub(/\d/, 'X')}. # remove time info
          gsub(/\/\*.+\*\//, '') # remove comment
    end

    def record sql, opt, result
      opt = opt.slice(*MATCH_OPT_KEYS)
      sql_key = sql_key(sql)
      val = {sql: sql, opt: opt, result: result}

      @data[sql_key] = val
      # pp @data

      debug_msg{ "record for #{sql.inspect}" }
    end

    def replay sql, opt, client
      debug_msg{ "replay for #{sql.inspect}" }
      opt = opt.slice(*MATCH_OPT_KEYS)
      sql_key = sql_key(sql)

      if data = @data[sql_key]
        raise "unmatch opt: expected: #{data[:opt]}, actual: #{opt}" if data[:opt] != opt
        cached_result = data[:result]
      else
        # not found
        if false # TODO?: insert mode
          raise "Unmatch: #{@name} @position:#{@position} for @data.size:#{@data.size}, sql:#{sql}"
        else
          debug_msg{ "not found. record new!" }
          @recording = true
          cached_result = client.query_and_record(sql, opt)
        end
      end

      cached_result
    end

    def finish!
      # save to storage
      if @recording
        unless File.exist?(dirname = File.dirname(@storage_name))
          require 'fileutils'
          FileUtils.mkdir_p(dirname)
        end
        File.open(@storage_name, 'w'){|f|
          YAML.dump @data, f
        }

        @recording = false
      end

      # reset
      @position = 1
    end
  end

  class FakeResult
    attr_reader :fields, :to_a

    def initialize result
      @fields = result.fields
      @to_a = result.to_a
    end

    def method_missing *args
      pp caller: caller, args: args, to_a: @to_a
      raise NoMethodError, args.inspect
    end
  end

  module QueryCacheClient
    # original
    if defined? ::Mysql2::Util::TIMEOUT_ERROR_CLASS
      TimeoutErrorClass = ::Mysql2::Util::TIMEOUT_ERROR_CLASS
    else # < 0.5
      TimeoutErrorClass = ::Mysql2::Util::TimeoutError
    end
 
    def query2(sql, opt)
      Thread.handle_interrupt(TimeoutErrorClass => :never) do
        _query(sql, opt)
      end
    end

    def query_and_record sql, opt
      result = query2(sql, opt)
      result = FakeResult.new(result) if result
      QueryCache.current.record sql, opt, result
      result
    end

    # cahce
    def query(sql, options = {})
      opt = @query_options.merge(options)

      if /^select/i =~ sql && QueryCache.current
        if QueryCache.current.recording
          query_and_record(sql, opt)
        else
          QueryCache.current.replay(sql, opt, self)
        end
      else
        if qc = QueryCache.current
          qc.debug_msg{ "other than select. sql:#{sql}" }
        end

        query2(sql, opt)
      end
    end
  end

  class Client
    prepend QueryCacheClient
  end
end
