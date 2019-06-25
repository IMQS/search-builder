package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/IMQS/cli"
	"github.com/IMQS/gowinsvc/service"
	"github.com/IMQS/search/server"
	_ "github.com/lib/pq"
)

func main() {
	app := cli.App{}
	app.Description = "imqs-search -c=configfile [options] command"
	app.DefaultExec = exec
	app.AddCommand("run", "Run the search service")
	app.AddCommand("find", "Find something from the command line", "...term")
	app.AddCommand("rebuild", "Rebuild indexes\nIf no tables are specified, then perform an auto rebuild, which rebuilds "+
		"all tables that are detected to have modifications. This is used in combination with the DisableAutoIndexRebuild "+
		"config option.", "...table")
	app.AddCommand("vacuum", "Vacuum the search index")
	app.AddValueOption("c", "configfile", "Configuration file if not using the configuration service")
	os.Exit(app.Run())
}

func exec(cmdName string, args []string, options cli.OptionSet) int {
	configFile := options["c"]

	engine := server.Engine{}
	engine.ConfigFile = configFile

	err := engine.LoadConfigFromFile()
	if err != nil {
		fmt.Printf("Error loading search config: %v\n", err)
		return 1
	}

	err = engine.Initialize(false)
	if err != nil {
		if engine.ErrorLog != nil {
			engine.ErrorLog.Error(err.Error())
		}
		fmt.Printf("Error initializing search engine: %v\n", err)
		return 1
	}

	//search.BenchmarkReads(engine.IndexDB)
	//return 1
	run := func() {
		go engine.StartStateWatcher()
		config := engine.GetConfig()
		if !config.DisableAutoIndexRebuild {
			go engine.StartAutoRebuilder()
		}
		err = engine.RunHttp()
		if err != nil {
			engine.ErrorLog.Errorf("Error running HTTP server: %v\n", err)
		}
	}

	start := time.Now()

	switch cmdName {
	case "run":
		if !service.RunAsService(run) {
			run()
		}
	case "find":
		var res *server.FindResult
		query := &server.Query{
			Query: strings.Join(args, " "),
		}
		config := engine.GetConfig()
		res, err = engine.Find(query, config)
		if err == nil {
			fmt.Printf("%-20v %8v %5v\n", "Table", "Row", "Rank")
			for _, r := range res.Rows {
				fmt.Printf("%-20v %8v %5v\n", r.Table, r.Row, r.Rank)
			}
		}
	case "rebuild":
		if len(args) == 0 {
			fmt.Printf("Performing auto rebuild. See log for details.\n")
			err = engine.AutoRebuild()
		} else {
			tables := []server.TableFullName{}
			for _, a := range args {
				tables = append(tables, server.TableFullName(a))
			}
			err = engine.BuildIndex(tables, false)
		}
	case "vacuum":
		err = engine.Vacuum()
	default:
		fmt.Printf("Unknown command %v\n", cmdName)
		return 1
	}

	if err == nil {
		fmt.Printf("Finished in %.3v seconds\n", time.Now().Sub(start).Seconds())
		return 0
	} else {
		fmt.Printf("Error: %v\n", err)
		return 1
	}
}
