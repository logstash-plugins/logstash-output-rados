[Missing the other part of the readme]

## Running the tests

```
bundle install
bundle rspec
```

If you want to run the integration test you have to have access to a ceph cluster, and a real bucket

```
RADOS_LOGSTASH_TEST_POOL=mytest bundle exec rspec spec/integration/rados_spec.rb --tag integration
RADOS_LOGSTASH_TEST_POOL=mytest bundle exec rspec spec/outputs/rados_spec.rb

```
