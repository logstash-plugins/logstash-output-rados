# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "stud/temporary"
require "stud/task"
require "socket" # for Socket.gethostname
require "thread"
require "tmpdir"
require "fileutils"


# INFORMATION:
#
# This plugin sends logstash events to Ceph.
# To use it you need to have a properly configured librados and a valid ceph cluster.
# Make sure you have permissions to write files on Ceph.  Also be sure to run logstash as super user to establish a connection.
#
#
# This plugin outputs temporary files to "/opt/logstash/rados_temp/". If you want, you can change the path at the start of register method.
# These files have a special name, for example:
#
# ls.rados.ip-10-228-27-95.2013-04-18T10.00.tag_hello.part0.txt
#
# ls.rados : indicate logstash plugin rados
#
# "ip-10-228-27-95" : indicates the ip of your machine.
# "2013-04-18T10.00" : represents the time whenever you specify time_file.
# "tag_hello" : this indicates the event's tag.
# "part0" : this means if you indicate size_file then it will generate more parts if you file.size > size_file.
#           When a file is full it will be pushed to the pool and will be deleted from the temporary directory.
#           If a file is empty is not pushed, it is not deleted.
#
# This plugin have a system to restore the previous temporary files if something crash.
#
##[Note] :
#
## If you specify size_file and time_file then it will create file for each tag (if specified), when time_file or
## their size > size_file, it will be triggered then they will be pushed on Rados pool and will delete from local disk.
## If you don't specify size_file, but time_file then it will create only one file for each tag (if specified).
## When time_file it will be triggered then the files will be pushed on Rados and delete from local disk.
#
## If you don't specify time_file, but size_file  then it will create files for each tag (if specified),
## that will be triggered when their size > size_file, then they will be pushed on Rados pool and will delete from local disk.
#
## If you don't specific size_file and time_file you have a curios mode. It will create only one file for each tag (if specified).
## Then the file will be rest on temporary directory and don't will be pushed on pool until we will restart logstash.
#
#
# #### Usage:
# This is an example of logstash config:
# [source,ruby]
# output {
#    rados{
#      mypool => "mypool"             (required)
#      size_file => 2048                        (optional)
#      time_file => 5                           (optional)
#    }
#
class LogStash::Outputs::Rados < LogStash::Outputs::Base

  TEMPFILE_EXTENSION = "txt"
  RADOS_INVALID_CHARACTERS = /[\^`><]/


  config_name "rados"
  default :codec, 'line'

  # Rados pool
  config :pool, :validate => :string, :default => 'logstash'

  # Set the size of file in bytes, this means that files on pool when have dimension > file_size, they are stored in two or more file.
  # If you have tags then it will generate a specific size file for every tags
  ##NOTE: define size of file is the better thing, because generate a local temporary file on disk and then put it in pool.
  config :size_file, :validate => :number, :default => 0

  # Set the time, in minutes, to close the current sub_time_section of pool.
  # If you define file_size you have a number of files in consideration of the section and the current tag.
  # 0 stay all time on listerner, beware if you specific 0 and size_file 0, because you will not put the file on pool,
  # for now the only thing this plugin can do is to put the file when logstash restart.
  config :time_file, :validate => :number, :default => 0

  # Set the directory where logstash will store the tmp files before sending it to Rados
  # default to the current OS temporary directory in linux /tmp/logstash
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")

  # Specify a prefix to the uploaded filename, this can simulate directories on rados
  config :prefix, :validate => :string, :default => ''

  # Specify how many workers to use to upload the files to Rados
  config :upload_workers_count, :validate => :number, :default => 1

  # Define tags to be appended to the file on the Rados pool.
  #
  # Example:
  # tags => ["elasticsearch", "logstash", "kibana"]
  #
  # Will generate this file:
  # "ls.rados.logstash.local.2015-01-01T00.00.tag_elasticsearch.logstash.kibana.part0.txt"
  #
  config :tags, :validate => :array, :default => []


  config :use_ssl, :validate => :boolean, :default => true

  # Exposed attributes for testing purpose.
  attr_accessor :tempfile
  attr_reader :page_counter
  # Exposed attributes for testing purpose.
  attr_reader :cluster


  public
  def write_on_pool(file)
    rados_pool = @cluster.pool(@pool)
    rados_pool.open
    remote_filename = "#{@prefix}#{File.basename(file)}"

    @logger.debug("RADOS: ready to write file in pool", :remote_filename => remote_filename, :pool => @pool)

    File.open(file, 'r') do |fileIO|
      begin
        # prepare for write the file
        object = rados_pool.rados_object(remote_filename)
        object.write(0, fileIO.read)
      rescue SystemCallError => error
        @logger.error("RADOS: CEPH error", :error => error)
        raise LogStash::Error, "CEPH Configuration Error, #{error}"
      ensure
        rados_pool.close
      end
    end

    @logger.debug("RADOS: has written remote file in pool", :remote_filename => remote_filename, :pool  => @pool)
  end

  # This method is used for create new empty temporary files for use. Flag is needed for indicate new subsection time_file.
  public
  def create_temporary_file
    filename = File.join(@temporary_directory, get_temporary_filename(@page_counter))

    @logger.debug("RADOS: Creating a new temporary file", :filename => filename)

    @file_rotation_lock.synchronize do
      unless @tempfile.nil?
        @tempfile.close
      end

      @tempfile = File.open(filename, "a")
    end
  end

  public
  def register
    require "ceph-ruby"
    # required if using ruby version < 2.0
    # http://ruby.awsblog.com/post/Tx16QY1CI5GVBFT/Threading-with-the-AWS-SDK-for-Ruby
    workers_not_supported

    @cluster = CephRuby::Cluster.new
    @upload_queue = Queue.new
    @file_rotation_lock = Mutex.new

    if @prefix && @prefix =~ RADOS_INVALID_CHARACTERS
      @logger.error("RADOS: prefix contains invalid characters", :prefix => @prefix, :contains => RADOS_INVALID_CHARACTERS)
      raise LogStash::ConfigurationError, "RADOS: prefix contains invalid characters"
    end

    if !Dir.exist?(@temporary_directory)
      FileUtils.mkdir_p(@temporary_directory)
    end
    restore_from_crashes if @restore == true
    reset_page_counter
    create_temporary_file
    configure_periodic_rotation if time_file != 0
    configure_upload_workers

    @codec.on_event do |event, encoded_event|
      handle_event(encoded_event)
    end
  end

  public
  def restore_from_crashes
    @logger.debug("RADOS: is attempting to verify previous crashes...")

    Dir[File.join(@temporary_directory, "*.#{TEMPFILE_EXTENSION}")].each do |file|
      name_file = File.basename(file)
      @logger.warn("RADOS: have found temporary file the upload process crashed, uploading file to Rados.", :filename => name_file)
      move_file_to_pool_async(file)
    end
  end

  public
  def move_file_to_pool(file)
    if !File.zero?(file)
      write_on_pool(file)
      @logger.debug("RADOS: file was put on the upload thread", :filename => File.basename(file), :pool => @pool)
    end

    begin
      File.delete(file)
    rescue Errno::ENOENT
      # Something else deleted the file, logging but not raising the issue
      @logger.warn("RADOS: Cannot delete the temporary file since it doesn't exist on disk", :filename => File.basename(file))
    rescue Errno::EACCES
      @logger.error("RADOS: Logstash doesnt have the permission to delete the file in the temporary directory.", :filename => File.basename(file), :temporary_directory => @temporary_directory)
    end
  end

  public
  def periodic_interval
    @time_file * 60
  end

  public
  def get_temporary_filename(page_counter = 0)
    current_time = Time.now
    filename = "ls.rados.#{Socket.gethostname}.#{current_time.strftime("%Y-%m-%dT%H.%M")}"

    if @tags.size > 0
      return "#{filename}.tag_#{@tags.join('.')}.part#{page_counter}.#{TEMPFILE_EXTENSION}"
    else
      return "#{filename}.part#{page_counter}.#{TEMPFILE_EXTENSION}"
    end
  end

  public
  def receive(event)

    @codec.encode(event)
  end

  public
  def rotate_events_log?
    @file_rotation_lock.synchronize do
      @tempfile.size > @size_file
    end
  end

  public
  def write_events_to_multiple_files?
    @size_file > 0
  end

  public
  def write_to_tempfile(event)
    begin
      @logger.debug("RADOS: put event into tempfile ", :tempfile => File.basename(@tempfile))

      @file_rotation_lock.synchronize do
        @tempfile.syswrite(event)
      end
    rescue Errno::ENOSPC
      @logger.error("RADOS: No space left in temporary directory", :temporary_directory => @temporary_directory)
      close
    end
  end

  public  # Specify how many workers to use to upload the files to Rados
  config :upload_workers_count, :validate => :number, :default => 1
  def close
    shutdown_upload_workers
    @periodic_rotation_thread.stop! if @periodic_rotation_thread

    @file_rotation_lock.synchronize do
      @tempfile.close unless @tempfile.nil? && @tempfile.closed?
    end
    @cluster.shutdown
  end

  private
  def shutdown_upload_workers
    @logger.debug("RADOS: Gracefully shutdown the upload workers")
    @upload_queue << LogStash::ShutdownEvent
  end

  private
  def handle_event(encoded_event)
    if write_events_to_multiple_files?
      if rotate_events_log?
        @logger.debug("RADOS: tempfile is too large, let's upload it and create new file", :tempfile => File.basename(@tempfile))

        move_file_to_pool_async(@tempfile.path)
        next_page
        create_temporary_file
      else
        @logger.debug("RADOS: tempfile file size report.", :tempfile_size => @tempfile.size, :size_file => @size_file)
      end
    end

    write_to_tempfile(encoded_event)
  end

  private
  def configure_periodic_rotation
    @periodic_rotation_thread = Stud::Task.new do
      LogStash::Util::set_thread_name("<RADOS periodic uploader")

      Stud.interval(periodic_interval, :sleep_then_run => true) do
        @logger.debug("RADOS: time_file triggered, uploading the file", :filename => @tempfile.path)

        move_file_to_pool_async(@tempfile.path)
        next_page
        create_temporary_file
      end
    end
  end

  private
  def configure_upload_workers
    @logger.debug("RADOS: Configure upload workers")

    @upload_workers = @upload_workers_count.times.map do |worker_id|
      Stud::Task.new do
        LogStash::Util::set_thread_name("<RADOS upload worker #{worker_id}")

        while true do
          @logger.debug("RADOS: upload worker is waiting for a new file to upload.", :worker_id => worker_id)

          upload_worker
        end
      end
    end
  end

  private
  def upload_worker
    file = @upload_queue.deq

    case file
      when LogStash::ShutdownEvent
        @logger.debug("RADOS: upload worker is shutting down gracefuly")
        @upload_queue.enq(LogStash::ShutdownEvent)
      else
        @logger.debug("RADOS: upload worker is uploading a new file", :filename => File.basename(file))
        move_file_to_pool(file)
    end
  end

  private
  def next_page
    @page_counter += 1
  end

  private
  def reset_page_counter
    @page_counter = 0
  end

  private
  def delete_on_pool(filename)
    rados_pool = @cluster.pool(@pool)
    rados_pool.open
    remote_filename = "#{@prefix}#{File.basename(filename)}"

    @logger.debug("RADOS: delete file from pool", :remote_filename => remote_filename, :pool => @pool)

    begin
        object = rados_pool.rados_object(remote_filename)
        object.destroy
    rescue SystemCallError => error
        @logger.error("RADOS: CEPH error", :error => error)
        raise LogStash::Error, "CEPH Configuration Error, #{error}"
    ensure
        rados_pool.close
    end
  end

  private
  def move_file_to_pool_async(file)
    @logger.debug("RADOS: Sending the file to the upload queue.", :filename => File.basename(file))
    @upload_queue.enq(file)
  end
end
