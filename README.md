# impala-ruby

[![build](https://travis-ci.org/colinmarc/impala-ruby.svg?branch=master)](https://travis-ci.org/colinmarc/impala-ruby) &nbsp; [![rubygems](https://badge.fury.io/rb/impala.svg)](http://rubygems.org/gems/impala)

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

To connect to a kerberos-enabled Impala (assuming you ran kinit):
```
conn = Impala.connect('host', 21000,
               { :transport => :sasl,
                 :sasl_params => {
                   :mechanism => 'GSSAPI',
                   :remote_host => 'host',
                   :remote_principal => 'impala/host@REALM'}})
```

[1]: https://ccp.cloudera.com/display/IMPALA10BETADOC/Introducing+Cloudera+Impala
