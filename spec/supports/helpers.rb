def key_exists_on_pool?(key)
   pool = rados_object.pool(minimal_settings["pool"])
   pool.open
   obj = pool.rados_object(key)
   obj.exists?
end

def events_in_files(files)
  files.collect { |file| File.foreach(file).count }.inject(&:+)
end

