require "fileutils"
require 'optparse'

image_name = "search"

def exec_or_die( cmd, current_dir = nil )
	orgDir = Dir.pwd
	Dir.chdir(current_dir) if current_dir != nil
	
	res = `#{cmd}`
	
	Dir.chdir(orgDir)

	if $?.exitstatus != 0
		print(res)
		exit(false)
	end
end

options = {}
OptionParser.new do |opts|
	opts.banner = "Usage: docker-build.rb [options]"
	opts.on("-t", "--dockertag TAG", "Docker tag to use when pusing the image. Defaults to latest.") do |dt|
		options[:dockertag] = dt.gsub("origin/","")
	end
	opts.on("-p", "--push", "Push the image to Dockerhub") do |p|
		options[:push] = p
	end
	opts.on("-h", "--help", "Prints this help") do
		puts opts
		exit
	end
end.parse!

puts("Building binary")
exec_or_die("docker run --rm -e GOPATH=/usr/src/search -v #{Dir.pwd}:/usr/src/search -w /usr/src/search golang:1.8 go install github.com/IMQS/search/cmd")
puts("Building image")
exec_or_die("docker build -t #{image_name}:#{options[:dockertag]} .")
puts("Taging image")
exec_or_die("docker tag #{image_name}:#{options[:dockertag]} imqs/#{image_name}:#{options[:dockertag]}")
if options[:push]
	puts("Pusing image")
	exec_or_die("docker push imqs/#{image_name}:#{options[:dockertag]}")
end