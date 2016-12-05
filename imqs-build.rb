require "fileutils"

out_dir = "../out"

oldGoPath = ENV["GOPATH"]
ENV["GOPATH"] = Dir.pwd

at_exit {
	ENV["GOPATH"] = oldGoPath
}

def exec_or_die(cmd, current_dir = nil)
	orgDir = Dir.pwd
	Dir.chdir(current_dir) if current_dir != nil
	
	res = `#{cmd}`
	
	Dir.chdir(orgDir)

	if $?.exitstatus != 0
		print(res)
		exit(false)
	end
end

case ARGV[0]
	when "prepare"	then
		exec_or_die("go install github.com/IMQS/search/cmd")
	when "copy_out" then
		FileUtils.cp("bin/cmd.exe", out_dir + '/bin/imqssearch.exe')
	when "test_unit" then
		exec_or_die("go test github.com/IMQS/search/search -db_postgres -cpu 2")
		exec_or_die("go test github.com/IMQS/search/search -db_postgres -cpu 2 -race")
	when "test_integration" then
end

