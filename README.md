# impala-ruby

![build](https://travis-ci.org/colinmarc/impala-ruby.svg?branch=master) ![rubygems](https://badge.fury.io/rb/impala.svg)](http://rubygems.org/gems/impala)

This is a ruby client for [Cloudera Impala][1]. You use it like this:

```ruby
require 'impala'

Impala.connect('host', 21000) do |conn|
  conn.query('SELECT zip, income FROM zipcode_incomes LIMIT 5')
end
# => [{:zip=>'02446', :income=>89597}, ...]
```

You can also use cursors to avoid loading the entire result set into memory:

```ruby
conn = Impala.connect('host', 21000)
cursor = conn.execute('SELECT zip, income FROM zipcode_incomes ORDER BY income DESC')

one_row = cursor.fetch_row
cursor.each do |row|
  # etc
end

conn.close
```

[1]: https://ccp.cloudera.com/display/IMPALA10BETADOC/Introducing+Cloudera+Impala
