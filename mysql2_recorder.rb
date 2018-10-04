
require 'yaml'

module Mysql2
  class QueryCache
    class Rack
      def initialize app
        p enabled: self.class
        @app = app
      end

      def call env
        QueryCache.use 'rack/' +env["REQUEST_PATH"] do
          @app.call(env)
        end
      end
    end

    CURRENT_QC = Struct.new(:current).new(nil)

    def self.use name
      if CURRENT_QC.current
        raise "Mysql2::QueryCache already used by #{$qc}"
      end
      CURRENT_QC.current = QueryCache.new(name)
      yield
    ensure
      CURRENT_QC.current.save!
      CURRENT_QC.current = nil
    end

    def self.current
      CURRENT_QC.current
    end

    def self.recording?
      CURRENT_QC.recording
    end

    attr_reader :recording
    def initialize name
      @name = name
      @storage_name = "qc_records/#{name}.yaml"
      if File.exist? @storage_name
        @recording = false
        @data = YAML.load_file(@storage_name)
        @position = 0
      else
        @recording = true
        @data = []
      end
      @match_opt_keys = [:as, :symbolize_keys, :encoding, :database]
    end

    def record sql, opt, result
      opt = opt.slice(*@match_opt_keys)
      @data << {sql: sql, opt: opt, result: result}
    end

    def replay sql, opt
      @position += 1
      opt = opt.slice(*@match_opt_keys)
      cache_sql, cache_opt, cache_result = *@data[@position - 1].values

      if cache_sql != sql || cache_opt != opt
        pp cache: [cache_sql, cache_opt], given: [sql, opt]
        raise "Unmatch"
      end

      cache_result
    end

    def save!
      if @recording
        unless File.exist?(dirname = File.dirname(@storage_name))
          require 'fileutils'
          FileUtils.mkdir_p(dirname)
        end
        File.open(@storage_name, 'w'){|f|
          YAML.dump @data, f
        }
      end
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
    def query2(sql, opt)
      Thread.handle_interrupt(::Mysql2::Util::TIMEOUT_ERROR_CLASS => :never) do
        _query(sql, opt)
      end
    end

    # cahce
    def query(sql, options = {})
      opt = @query_options.merge(options)

      if /^select/i =~ sql && QueryCache.current
        if QueryCache.current.recording
          result = query2(sql, opt)
          result = FakeResult.new(result) if result
          QueryCache.current.record sql, opt, result
          result
        else
          result = QueryCache.current.replay(sql, opt)
        end
      else
        query2(sql, opt)
      end
    end
  end

  class Client
    prepend QueryCacheClient

    def self.use_cache
      QueryCacheClient::QUERY_CACHE.in_use = true
    end
  end
end
