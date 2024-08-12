package handlers

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"quackpipe/controller/root"
	"quackpipe/model"
	"quackpipe/service/db"
	"quackpipe/utils"
	"regexp"
	"strings"
	"time"
)

//go:embed play.html
var staticPlay string

type Handler struct {
	FlagInformation *model.CommandLineFlags
}

func (u *Handler) Handlers(w http.ResponseWriter, r *http.Request) {
	var bodyBytes []byte
	var query string
	var err error
	defaultFormat := *u.FlagInformation.Format
	defaultParams := *u.FlagInformation.Params
	defaultPath := *u.FlagInformation.DBPath
	// handle query parameter
	if r.URL.Query().Get("query") != "" {
		query = r.URL.Query().Get("query")
	} else if r.Body != nil {
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("Body reading error: %v", err)
			return
		}
		defer r.Body.Close()
		query = string(bodyBytes)
	}

	switch r.Header.Get("Accept") {
	case "application/json":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	case "application/xml":
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	case "text/css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	default:
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	}
	// format handling
	if r.URL.Query().Get("default_format") != "" {
		defaultFormat = r.URL.Query().Get("default_format")
	}
	// param handling
	if r.URL.Query().Get("default_params") != "" {
		defaultParams = r.URL.Query().Get("default_params")
	}

	// extract FORMAT from query and override the current `default_format`
	cleanQuery, format := utils.ExtractAndRemoveFormat(query)
	if len(format) > 0 {
		query = cleanQuery
		defaultFormat = format
	}
	if len(query) == 0 {
		_, _ = w.Write([]byte(staticPlay))

	} else {
		result, err := root.QueryOperation(u.FlagInformation, query, r, defaultPath, defaultFormat, defaultParams)
		if err != nil {
			_, _ = w.Write([]byte(err.Error()))
		} else {
			_, _ = w.Write([]byte(result))
		}
	}

}

func (u *Handler) InsertHandler(w http.ResponseWriter, r *http.Request) {
	// Read and URL decode the query parameter
	defaultFormat := *u.FlagInformation.Format
	defaultParams := *u.FlagInformation.Params
	defaultPath := *u.FlagInformation.DBPath
	rawQuery := r.URL.Query().Get("query")
	query, err := url.QueryUnescape(rawQuery)
	if err != nil {
		http.Error(w, "Invalid URL encoding in query parameter", http.StatusBadRequest)
		return
	}

	if query == "" {
		http.Error(w, "Query cannot be empty", http.StatusBadRequest)
		return
	}

	// Ensure query starts with "INSERT INTO"
	if !strings.HasPrefix(query, "INSERT INTO") {
		http.Error(w, "Invalid query format", http.StatusBadRequest)
		return
	}

	// Ensure the format is JSONEachRow
	if !strings.Contains(strings.ToUpper(query), "FORMAT JSONEACHROW") {
		http.Error(w, "Format must be JSONEachRow", http.StatusBadRequest)
		return
	}

	// Read and parse the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	switch r.Header.Get("Accept") {
	case "application/json":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	case "application/xml":
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	case "text/css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	default:
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	}

	// Format handling
	if r.URL.Query().Get("default_format") != "" {
		defaultFormat = r.URL.Query().Get("default_format")
	}
	// Param handling
	if r.URL.Query().Get("default_params") != "" {
		defaultParams = r.URL.Query().Get("default_params")
	}

	// Extract FORMAT from query and override the current `default_format`
	cleanQuery, format := ExtractAndRemoveFormat(query)
	if len(format) > 0 {
		query = cleanQuery
		defaultFormat = format
	}
	fmt.Println(defaultFormat)
	// Make sure to use a file within the specified directory
	dbFile := filepath.Join(defaultPath, "duckdb.db")
	_, _, err = db.Quack(*u.FlagInformation, query, body, false, defaultParams, dbFile)
	//_, _, err = Quack(*u.FlagInformation, query, body, false, defaultParams, dbFile)
	//if err != nil {
	//	http.Error(w, fmt.Sprintf("Failed to insert into DuckDB: %v", err), http.StatusInternalServerError)
	//	return
	//}

	// Respond with success
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Data inserted successfully"))
}

func Quack(appFlags model.CommandLineFlags, query string, jsonData []byte, stdin bool, params string, dbPath string) (*sql.Rows, time.Duration, error) {
	fmt.Println("Quack Function Trigger.....")

	var err error
	alias := *appFlags.Alias
	motherduck, md := os.LookupEnv("motherduck_token")

	// Construct the full DuckDB connection string
	if len(params) > 0 {
		params = dbPath + "?" + params
	} else {
		params = dbPath
	}

	fmt.Println("params", params)
	db, err := sql.Open("duckdb", params)
	if err != nil {
		return nil, 0, err
	}
	defer db.Close()

	if !stdin {
		check(db.ExecContext(context.Background(), "LOAD httpfs; LOAD json; LOAD parquet;"))
		check(db.ExecContext(context.Background(), "SET autoinstall_known_extensions=1;"))
		check(db.ExecContext(context.Background(), "SET autoload_known_extensions=1;"))
	}

	if alias {
		check(db.ExecContext(context.Background(), "LOAD chsql;"))
	}

	if (md) && (motherduck != "") {
		check(db.ExecContext(context.Background(), "LOAD motherduck; ATTACH 'md:';"))
	}

	// Base64 encode the JSON data
	base64Json := base64.StdEncoding.EncodeToString(jsonData)
	fmt.Println("Query Information", query)
	// Modify the query to use the JSON data directly
	jsonQuery := fmt.Sprintf("SELECT * FROM read_json_auto(base64_decode('%s'))", base64Json)

	// Add the JSON query to the main query
	query = query + " " + jsonQuery

	fmt.Println("final Query", query)
	startTime := time.Now()
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return nil, 0, err
	}
	elapsedTime := time.Since(startTime)
	return rows, elapsedTime, nil
}

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}

// ExtractAndRemoveFormat extracts the FORMAT clause from the query and returns the query without the FORMAT clause
func ExtractAndRemoveFormat(input string) (string, string) {
	re := regexp.MustCompile(`(?i)\bFORMAT\s+(\w+)\b`)
	match := re.FindStringSubmatch(input)
	if len(match) != 2 {
		return input, ""
	}
	format := match[1]
	return re.ReplaceAllString(input, ""), format
}

func (u *Handler) CHHandlers(w http.ResponseWriter, r *http.Request) {

	defaultFormat := *u.FlagInformation.Format
	defaultParams := *u.FlagInformation.Params
	defaultPath := *u.FlagInformation.DBPath
	rawQuery := r.URL.Query().Get("query")
	if rawQuery == "" {
		http.Error(w, "Format must be JSON", http.StatusBadRequest)
		return
	}

	query, err := url.QueryUnescape(rawQuery)
	if err != nil {
		http.Error(w, "Invalid URL encoding in query parameter", http.StatusBadRequest)
		return
	}
	// Ensure query starts with "INSERT INTO"
	if !strings.HasPrefix(query, "INSERT INTO") {
		http.Error(w, "Invalid query format", http.StatusBadRequest)
		return
	}
	fmt.Println("query information", query)
	// Ensure the format is JSONEachRow
	if !strings.Contains(query, "JSONEachRow") {
		http.Error(w, "Format must be JSONEachRow", http.StatusBadRequest)
		return
	}
	// Read and parse the request body
	var data interface{}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	err = json.Unmarshal(body, &data)
	if err != nil {
		http.Error(w, "Failed to parse JSON", http.StatusBadRequest)
		return
	}

	filename, err := utils.FileSaveLocal(body)
	if err != nil {
		http.Error(w, "Failed to save data into file", http.StatusBadRequest)
		return
	}
	switch r.Header.Get("Accept") {
	case "application/json":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	case "application/xml":
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	case "text/css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	default:
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	}
	//// format handling
	if r.URL.Query().Get("default_format") != "" {
		defaultFormat = r.URL.Query().Get("default_format")
	}
	// param handling
	if r.URL.Query().Get("default_params") != "" {
		defaultParams = r.URL.Query().Get("default_params")
	}

	// extract FORMAT from query and override the current `default_format`
	cleanQuery, format := utils.ExtractAndRemoveFormat(query)
	if len(format) > 0 {
		query = cleanQuery
		defaultFormat = format
	}
	fmt.Println("Clean Query", cleanQuery)
	err = root.InsertIntoDuckDB(u.FlagInformation, query, filename, r, defaultPath, defaultFormat, defaultParams)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer os.Remove(filename)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Data inserted successfully"))
}
