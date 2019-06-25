package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/IMQS/gzipresponse"
	"github.com/julienschmidt/httprouter"
)

const defaultHttpPort = "2007"

type jsonResultRoot struct {
	NumValidItems int
	StaleIndex    bool
	Items         []*jsonResult
	Stats         jsonResultStats
}

// GenericTable represents a full table structure used to save config to the database
type GenericTable struct {
	Name          string      // Represents the table's internal name that we expect to change when a generic package is reimported or deleted
	LongLivedName string      // Represents the table's friendly name that we do not expect to change
	Table         ConfigTable // Contains a list of indexed fields
}

type jsonResultStats struct {
	TimeQueryParse float64
	TimeDBQuery    float64
	TimeDBFetch    float64
	TimeTotal      float64
	TimeAuxFetch   float64
	TimeKeywords   float64
	TimeRelWalk    float64
	TimeSort       float64
}

type jsonRelatedRow struct {
	TableID int
	RowKey  int64
}

type jsonResult struct {
	Database       string
	Table          string
	TableID        int
	HumanIDs       []string // Friendly names for the DB columns. The `FriendlyName` field in `Fields` in the config file makes up this value
	RowKey         string   // Unique id (primary key value) associated with this result
	RowKeyField    string   // Column name of the unique id field (primary key) associated with this result
	Rank           float64  // Level of relevance to search request
	Values         map[string]string
	RelatedRecords []jsonRelatedRow
}

type jsonPingResult struct {
	Timestamp int64
}

func (e *Engine) RunHttp() error {
	config := e.GetConfig()
	port := defaultHttpPort
	if config.HTTP.Port != "" {
		port = config.HTTP.Port
	}
	addr := fmt.Sprintf("%v:%v", config.HTTP.Bind, port)

	makeRoute := func(f func(*Engine, http.ResponseWriter, *http.Request, httprouter.Params)) httprouter.Handle {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			f(e, w, r, ps)
		}
	}

	router := httprouter.New()
	router.GET("/find/:query", makeRoute(httpFind))
	router.PUT("/config/:database", makeRoute(httpConfigSet))
	router.PUT("/rename_table/:database/:external/:internal", makeRoute(httpConfigRenameTable))
	router.GET("/config", makeRoute(httpConfigGet))
	router.GET("/ping", makeRoute(httpPing))
	router.POST("/delete_table/:database/:table", makeRoute(httpConfigDeleteTable))

	e.ErrorLog.Infof("Search is listening on %v", addr)

	err := http.ListenAndServe(addr, router)
	e.ErrorLog.Infof("ListenAndServe: %v", err)
	return err
}

func httpSendError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintf(w, "%v", err)
}

func httpFind(e *Engine, w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	maxResults, _ := strconv.ParseInt(r.URL.Query().Get("maxResults"), 10, 64)
	query := &Query{
		SendRelatedRecords: r.URL.Query().Get("sendRelatedRecords") == "1",
		MaxResultNum:       int(maxResults),
		Query:              ps.ByName("query"),
	}
	config := e.GetConfig()
	results, err := e.Find(query, config)
	if err != nil {
		e.ErrorLog.Warnf(`Query failed: %v. Query = "%v"`, err, query)
		httpSendError(w, err)
	} else {
		e.AccessLog.Infof("Find(%v): %v results in %.2v ms", query, len(results.Rows), results.TimeTotal.Seconds()*1000.0)
		res := jsonResultRoot{
			NumValidItems: results.NumValidRows,
			StaleIndex:    results.StaleIndex,
		}
		res.Stats = jsonResultStats{
			TimeQueryParse: results.TimeQueryParse.Seconds(),
			TimeDBQuery:    results.TimeDBQuery.Seconds(),
			TimeDBFetch:    results.TimeDBFetch.Seconds(),
			TimeTotal:      results.TimeTotal.Seconds(),
			TimeAuxFetch:   results.TimeAuxFetch.Seconds(),
			TimeKeywords:   results.TimeKeywords.Seconds(),
			TimeRelWalk:    results.TimeRelWalk.Seconds(),
			TimeSort:       results.TimeSort.Seconds(),
		}
		for _, r := range results.Rows {
			table := config.tableConfigFromName(r.Table)
			jr := &jsonResult{
				Database:    r.Table.DBOnly(),
				Table:       r.Table.TableOnly(),
				TableID:     int(r.SrcTab),
				HumanIDs:    config.getTableHumanIDsFromName(r.Table),
				RowKey:      strconv.FormatInt(r.Row, 10),
				RowKeyField: table.IndexField,
				Rank:        float64(r.Rank),
				Values:      r.Values,
			}
			for _, rel := range r.RelatedRecords {
				jr.RelatedRecords = append(jr.RelatedRecords, jsonRelatedRow{
					TableID: int(rel.SrcTab),
					RowKey:  rel.Row,
				})
			}
			res.Items = append(res.Items, jr)
		}
		raw, _ := json.Marshal(res)
		w.Header().Set("Content-Type", "application/json")
		gzipresponse.Write(w, r, raw)
	}
}

func httpConfigGet(e *Engine, w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	cfg := e.GetConfig()
	cfg.httpAPIConfigLock.RLock()
	defer cfg.httpAPIConfigLock.RUnlock()
	eTag := r.Header.Get("If-None-Match")
	if eTag == cfg.httpAPIConfigHash {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "max-age=0, no-cache")
	w.Header().Set("ETag", cfg.httpAPIConfigHash)
	gzipresponse.Write(w, r, []byte(cfg.httpAPIConfig))
}

func httpPing(e *Engine, w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "max-age=0, no-cache")
	res := jsonPingResult{
		Timestamp: time.Now().Unix(),
	}
	response, _ := json.Marshal(&res)
	w.Write(response)
}

func httpConfigSet(e *Engine, w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dbname := ps.ByName("database")

	tables := make([]GenericTable, 0)
	if err := json.NewDecoder(r.Body).Decode(&tables); err != nil {
		e.ErrorLog.Errorf(`Error unmarshaling config: %v.`, err)
		httpSendError(w, err)
	}
	if err := e.updateConfig(dbname, tables); err != nil {
		e.ErrorLog.Errorf(`Unable to update config with error: %v`, err)
		httpSendError(w, err)
	}

	if err := e.refreshJSONConfig(); err != nil {
		e.ErrorLog.Errorf(`Failed to update front-end config after update %v`, err)
		httpSendError(w, err)
	}

	http.Error(w, "", http.StatusOK)
}

func httpConfigDeleteTable(e *Engine, w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dbName := ps.ByName("database")
	table := ps.ByName("table")

	if err := e.deleteConfigTable(dbName, table); err != nil {
		e.ErrorLog.Errorf(`Failed to delete search config for table %v: %v.`, table, err)
		httpSendError(w, err)
	}

	if err := e.refreshJSONConfig(); err != nil {
		e.ErrorLog.Errorf(`Failed to update front-end config after delete, table %v: %v.`, table, err)
		httpSendError(w, err)
	}
	http.Error(w, "", http.StatusOK)
}

func httpConfigRenameTable(e *Engine, w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dbName := ps.ByName("database")
	tableName := ps.ByName("internal")

	longLivedName, err := url.QueryUnescape(ps.ByName("external"))
	if err != nil {
		e.ErrorLog.Errorf(`Unable to get table external name from request: %v.`, err)
		httpSendError(w, err)
	}

	if err := e.updateConfigTable(dbName, tableName, longLivedName); err != nil {
		e.ErrorLog.Errorf(`Failed to update config after table rename, table %v: %v.`, longLivedName, err)
		httpSendError(w, err)
	}

	if err = e.refreshJSONConfig(); err != nil {
		e.ErrorLog.Errorf(`Failed to update front-end config after table rename table: %v, %v.`, longLivedName, err)
		httpSendError(w, err)
	}
	http.Error(w, "", http.StatusOK)
}
