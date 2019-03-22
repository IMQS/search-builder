package search

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
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
	Row     int64
}

type jsonResult struct {
	Database       string
	Table          string
	TableID        int
	ItemType       string
	HumanIDs       []string
	Row            string
	Rank           float64
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
	router.GET("/config", makeRoute(httpConfigGet))
	router.GET("/ping", makeRoute(httpPing))

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
			jr := &jsonResult{
				Database: r.Table.DBOnly(),
				Table:    r.Table.TableOnly(),
				TableID:  int(r.SrcTab),
				ItemType: config.tableConfigFromName(r.Table).FriendlyName,
				HumanIDs: config.getTableHumanIDsFromName(r.Table),
				Row:      strconv.FormatInt(r.Row, 10),
				Rank:     float64(r.Rank),
				Values:   r.Values,
			}
			for _, rel := range r.RelatedRecords {
				jr.RelatedRecords = append(jr.RelatedRecords, jsonRelatedRow{
					TableID: int(rel.SrcTab),
					Row:     rel.Row,
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
	if err := e.updateConfig(dbname, r.Body); err != nil {
		e.ErrorLog.Errorf(`Unable to update config with error: %v.`, err)
		httpSendError(w, err)
	}

	if err := e.ReloadMergedConfig(); err != nil {
		e.ErrorLog.Errorf(`Error updating search config in ReloadMergedConfig: %v.`, err)
		httpSendError(w, err)
	}

	eConfig := e.GetConfig()
	if err := eConfig.postJSONLoad(); err != nil {
		e.ErrorLog.Errorf(`Error updating search config in postJSONLoad: %v.`, err)
		httpSendError(w, err)
	}

	// Set index out of date to true after config update.
	atomic.StoreUint32(&e.isIndexOutOfDate_Atomic, 1)

	// Update front-end config with the new and updated engine config
	eConfig.updateHttpApiConfig(e)
	http.Error(w, "", http.StatusOK)
}
