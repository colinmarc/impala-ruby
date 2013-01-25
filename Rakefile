require "bundler/gem_tasks"

task :default => [:test]

task :test do
  ret = true
  Dir["test/**/*.rb"].each do |f|
    ret = ret && ruby(f, '')
  end
  exit(ret)
end

THRIFT_FILES = FileList['./thrift/*.thrift']
GENNED_FILES = FileList['./lib/impala/protocol/*']

task :gen do
  THRIFT_FILES.each do |f|
    sh "thrift -out lib/impala/protocol --gen rb #{f}"
  end
  sh "eden rewrite lib/impala/protocol/*.rb"
end

task :clean do
  GENNED_FILES.each { |f| rm f }
end
