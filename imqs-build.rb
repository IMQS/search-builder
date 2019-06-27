require "fileutils"

out_dir = "../out"

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
		exec_or_die("go build")
	when "copy_out" then
		FileUtils.cp("search.exe", out_dir + '/bin/imqssearch.exe')
	when "test_unit" then
		exec_or_die("go test github.com/IMQS/search/server -db_postgres -cpu 2")
		exec_or_die("go test github.com/IMQS/search/server -db_postgres -cpu 2 -race")
	when "test_integration" then
end
