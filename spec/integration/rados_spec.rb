require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/rados"
require 'socket'
require "fileutils"
require "stud/temporary"
require_relative "../supports/helpers"

describe LogStash::Outputs::Rados, :integration => true, :rados => true do
  before do
    Thread.abort_on_exception = true
  end

  let!(:minimal_settings)  {  { "pool" => ENV['RADOS_LOGSTASH_TEST_POOL'],
                                "temporary_directory" => Stud::Temporary.pathname('temporary_directory') }}

  let!(:rados_object) do
      radosoutput = LogStash::Outputs::Rados.new(minimal_settings)
      radosoutput.register
      radosoutput.cluster
  end

  describe "#register" do
    it "write a file on the pool to check permissions" do
      rados = LogStash::Outputs::Rados.new(minimal_settings)
      expect(rados.register).not_to raise_error
    end
  end

  describe "#write_on_pool" do
    after(:each) do
      File.unlink(fake_data.path)
    end

    let!(:fake_data) { Stud::Temporary.file }

    it "should prefix the file on the pool if a prefix is specified" do
      prefix = "my-prefix"

      config = minimal_settings.merge({
        "prefix" => prefix,
      })

      rados = LogStash::Outputs::Rados.new(config)
      rados.register
      rados.write_on_pool(fake_data)

      expect(key_exists_on_pool?("#{prefix}#{File.basename(fake_data.path)}")).to eq(true)
    end

    it 'should use the same local filename if no prefix is specified' do
      rados = LogStash::Outputs::Rados.new(minimal_settings)
      rados.register
      rados.write_on_pool(fake_data)

      expect(key_exists_on_pool?(File.basename(fake_data.path))).to eq(true)
    end
  end

  describe "#move_file_to_pool" do
    let!(:rados) { LogStash::Outputs::Rados.new(minimal_settings) }

    before do
      rados.register
    end

    it "should upload the file if the size > 0" do
      tmp = Stud::Temporary.file
      allow(File).to receive(:zero?).and_return(false)
      rados.move_file_to_pool(tmp)
      expect(key_exists_on_pool?(File.basename(tmp.path))).to eq(true)
    end
  end

  describe "#restore_from_crashes" do
    it "read the temp directory and upload the matching file to rados" do
      Stud::Temporary.pathname do |temp_path|
        tempfile = File.open(File.join(temp_path, 'A'), 'w+') { |f| f.write('test')}

        rados = LogStash::Outputs::Rados.new(minimal_settings.merge({ "temporary_directory" => temp_path }))
        rados.restore_from_crashes

        expect(File.exist?(tempfile.path)).to eq(false)
        expect(key_exists_on_pool?(File.basename(tempfile.path))).to eq(true)
      end
    end
  end
end
