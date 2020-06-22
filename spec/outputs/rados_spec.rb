# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/rados"
require "logstash/codecs/line"
require "logstash/pipeline"
require "fileutils"
require_relative "../supports/helpers"

describe LogStash::Outputs::Rados do
  before do
    # We stub all the calls from Rad, for more information see:
    # http://ruby.awsblog.com/post/Tx2SU6TYJWQQLC3/Stubbing-AWS-Responses
    Thread.abort_on_exception = true
  end

  let(:minimal_settings)  {  {"pool" => ENV['RADOS_LOGSTASH_TEST_POOL'] }  }

  describe "#register" do
    it "should create the tmp directory if it doesn't exist" do
      temporary_directory = Stud::Temporary.pathname("temporary_directory")

      config = {
        "pool" => "logstash",
        "size_file" => 10,
        "temporary_directory" => temporary_directory
      }

      rados = LogStash::Outputs::Rados.new(config)
      rados.register

      expect(Dir.exist?(temporary_directory)).to eq(true)
      rados.close
      FileUtils.rm_r(temporary_directory)
    end

    it "should raise a ConfigurationError if the prefix contains one or more '\^`><' characters" do
      config = {
        "prefix" => "`no\><^"
      }

      rados = LogStash::Outputs::Rados.new(config)

      expect {
        rados.register
      }.to raise_error(LogStash::ConfigurationError)
    end
  end

  describe "#generate_temporary_filename" do
    before do
      allow(Socket).to receive(:gethostname) { "logstash.local" }
    end

    it "should add tags to the filename if present" do
      config = minimal_settings.merge({ "tags" => ["elasticsearch", "logstash", "kibana"], "temporary_directory" => "/tmp/logstash"})
      rados = LogStash::Outputs::Rados.new(config)
      expect(rados.get_temporary_filename).to match(/^ls\.rados\.logstash\.local\.\d{4}-\d{2}\-\d{2}T\d{2}\.\d{2}\.tag_#{config["tags"].join("\.")}\.part0\.txt\Z/)
    end

    it "should not add the tags to the filename" do
      config = minimal_settings.merge({ "tags" => [], "temporary_directory" => "/tmp/logstash" })
      rados = LogStash::Outputs::Rados.new(config)
      expect(rados.get_temporary_filename(3)).to match(/^ls\.rados\.logstash\.local\.\d{4}-\d{2}\-\d{2}T\d{2}\.\d{2}\.part3\.txt\Z/)
    end

    it "normalized the temp directory to include the trailing slash if missing" do
      rados = LogStash::Outputs::Rados.new(minimal_settings.merge({ "temporary_directory" => "/tmp/logstash" }))
      expect(rados.get_temporary_filename).to match(/^ls\.rados\.logstash\.local\.\d{4}-\d{2}\-\d{2}T\d{2}\.\d{2}\.part0\.txt\Z/)
    end
  end


  describe "#write_events_to_multiple_files?" do
    it 'returns true if the size_file is != 0 ' do
      rados = LogStash::Outputs::Rados.new(minimal_settings.merge({ "size_file" => 200 }))
      expect(rados.write_events_to_multiple_files?).to eq(true)
    end

    it 'returns false if size_file is zero or not set' do
      rados = LogStash::Outputs::Rados.new(minimal_settings)
      expect(rados.write_events_to_multiple_files?).to eq(false)
    end
  end

  describe "#write_to_tempfile" do
    it "should append the event to a file" do
      Stud::Temporary.file("logstash", "a+") do |tmp|
        rados = LogStash::Outputs::Rados.new(minimal_settings)
        rados.register
        rados.tempfile = tmp
        rados.write_to_tempfile("test-write")
        tmp.rewind
        expect(tmp.read).to eq("test-write")
      end
    end
  end

  describe "#rotate_events_log" do

    context "having a single worker" do
      let(:rados) { LogStash::Outputs::Rados.new(minimal_settings.merge({ "size_file" => 1024 })) }

      before(:each) do
        rados.register
      end

      it "returns true if the tempfile is over the file_size limit" do
        Stud::Temporary.file do |tmp|
          allow(tmp).to receive(:size) { 2024001 }

          rados.tempfile = tmp
          expect(rados.rotate_events_log?).to be(true)
        end
      end

      it "returns false if the tempfile is under the file_size limit" do
        Stud::Temporary.file do |tmp|
          allow(tmp).to receive(:size) { 100 }

          rados.tempfile = tmp
          expect(rados.rotate_events_log?).to eq(false)
        end
      end
    end

    context "having periodic rotations" do
      let(:rados)  { LogStash::Outputs::Rados.new(minimal_settings.merge({ "size_file" => 1024, "time_file" => 6e-10 })) }
      let(:tmp) { Tempfile.new('rados_rotation_temp_file') }

      before(:each) do
        rados.tempfile = tmp
        rados.register
      end

      after(:each) do
        rados.close
        tmp.close 
        tmp.unlink
      end

      it "raises no error when periodic rotation happen" do
        1000.times do
          expect { rados.rotate_events_log? }.not_to raise_error
        end
      end
    end
  end

  describe "#move_file_to_pool" do
    subject { LogStash::Outputs::Rados.new(minimal_settings) }

    it "should always delete the source file" do
      tmp = Stud::Temporary.file

      allow(File).to receive(:zero?).and_return(true)
      expect(File).to receive(:delete).with(tmp)

      subject.move_file_to_pool(tmp)
    end

    it 'should not upload the file if the size of the file is zero' do
      temp_file = Stud::Temporary.file
      allow(temp_file).to receive(:zero?).and_return(true)

      expect(subject).not_to receive(:write_on_pool)
      subject.move_file_to_pool(temp_file)
    end

    it "should upload the file if the size > 0" do
      tmp = Stud::Temporary.file

      allow(File).to receive(:zero?).and_return(false)
      expect(subject).to receive(:write_on_pool)

      subject.move_file_to_pool(tmp)
    end
  end

  describe "#restore_from_crashes" do
    it "read the temp directory and upload the matching file to rados" do
      subject = LogStash::Outputs::Rados.new(minimal_settings.merge({ "temporary_directory" => "/tmp/logstash/" }))

      expect(Dir).to receive(:[]).with("/tmp/logstash/*.txt").and_return(["/tmp/logstash/01.txt"])
      expect(subject).to receive(:move_file_to_pool_async).with("/tmp/logstash/01.txt")


      subject.restore_from_crashes
    end
  end

  describe "#receive" do
    it "should send the event through the codecs" do
      data = {"foo" => "bar", "baz" => {"bah" => ["a","b","c"]}, "@timestamp" => "2014-05-30T02:52:17.929Z"}
      event = LogStash::Event.new(data)

      expect_any_instance_of(LogStash::Codecs::Line).to receive(:encode).with(event)

      subject = LogStash::Outputs::Rados.new(minimal_settings)
      subject.register

      subject.receive(event)
    end
  end

  describe "when rotating the temporary file" do
    before { allow(File).to receive(:delete) }

    it "doesn't skip events if using the size_file option" do
      Stud::Temporary.directory do |temporary_directory|
        size_file = rand(200..20000)
        event_count = rand(300..15000)

        config = %Q[
        input {
          generator {
            count => #{event_count}
          }
        }
        output {
          rados {
            size_file => #{size_file}
            codec => line
            temporary_directory => '#{temporary_directory}'
            pool =>  '#{ENV['RADOS_LOGSTASH_TEST_POOL']}'
          }
        }
        ]

        pipeline = LogStash::Pipeline.new(config)

        pipeline_thread = Thread.new { pipeline.run }
        sleep 0.1 while !pipeline.ready?
        pipeline_thread.join

        events_written_count = events_in_files(Dir[File.join(temporary_directory, 'ls.*.txt')])
        expect(events_written_count).to eq(event_count)
      end
    end

    it "doesn't skip events if using the time_file option", :tag => :slow do
      Stud::Temporary.directory do |temporary_directory|
        time_file = rand(1..2)
        number_of_rotation = rand(2..5)

        config = {
          "time_file" => time_file,
          "codec" => "line",
          "temporary_directory" => temporary_directory,
          "pool" => "#{ENV['RADOS_LOGSTASH_TEST_POOL']}"
        }

        rados = LogStash::Outputs::Rados.new(minimal_settings.merge(config))
        # Make the test run in seconds intead of minutes..
        expect(rados).to receive(:periodic_interval).and_return(time_file)
        rados.register

        # Force to have a few files rotation
        stop_time = Time.now + (number_of_rotation * time_file)
        event_count = 0

        event = LogStash::Event.new("message" => "Hello World")

        until Time.now > stop_time do
          rados.receive(event)
          event_count += 1
        end
        rados.close

        generated_files = Dir[File.join(temporary_directory, 'ls.*.txt')]

        events_written_count = events_in_files(generated_files)

        # Skew times can affect the number of rotation..
        expect(generated_files.count).to be_within(number_of_rotation).of(number_of_rotation + 1)
        expect(events_written_count).to eq(event_count)
      end
    end
  end
end
