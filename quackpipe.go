package main

import (
    "context"
    "crypto/sha256"
    "database/sql"
    "encoding/base64"
    "encoding/csv"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "net/http"
    "strings"
    "sync"
    "time"
	
    _ "github.com/mattn/go-duckdb"
)

type Session struct {
    DB *sql.DB
}

var sessionCache sync.Map

func main() {
    http.HandleFunc("/query", basicAuth(queryHandler))

    // Existing endpoints for backwards compatibility
    http.HandleFunc("/start_session", startSessionHandler)
    http.HandleFunc("/close_session", closeSessionHandler)

    fmt.Println("Starting server on :8080")
    http.ListenAndServe(":8080", nil)
}

func basicAuth(next http.HandlerFunc) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        auth := r.Header.Get("Authorization")
        if auth == "" {
            http.Error(w, "authorization required", http.StatusUnauthorized)
            return
        }

        parts := strings.SplitN(auth, " ", 2)
        if len(parts) != 2 || parts[0] != "Basic" {
            http.Error(w, "invalid authorization header", http.StatusUnauthorized)
            return
        }

        payload, _ := base64.StdEncoding.DecodeString(parts[1])
        authStr := string(payload)
        sessionID := hashCredentials(authStr)

        session, err := getSession(sessionID)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }

        r.Header.Set("X-Session-ID", sessionID)
        r = r.WithContext(context.WithValue(r.Context(), "session", session))

        next.ServeHTTP(w, r)
    }
}

func hashCredentials(auth string) string {
    hash := sha256.Sum256([]byte(auth))
    return hex.EncodeToString(hash[:])
}

func getSession(sessionID string) (*Session, error) {
    if session, ok := sessionCache.Load(sessionID); ok {
        return session.(*Session), nil
    }

    db, err := sql.Open("duckdb", "")
    if err != nil {
        return nil, err
    }

    session := &Session{DB: db}
    sessionCache.Store(sessionID, session)

    return session, nil
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
    sessionID := r.Header.Get("X-Session-ID")
    query := r.URL.Query().Get("query")
    format := r.URL.Query().Get("format")

    if format == "" {
        format = "json"
    }

    session := r.Context().Value("session").(*Session)
    result, err := quackWithDB(session.DB, query, format)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.Write([]byte(result))
}

func quackWithDB(db *sql.DB, query string, format string) (string, error) {
    startTime := time.Now()
    rows, err := db.Query(query)
    if err != nil {
        return "", err
    }
    elapsedTime := time.Since(startTime)

    switch format {
    case "json":
        return rowsToJSON(rows, elapsedTime)
    case "csv":
        return rowsToCSV(rows, elapsedTime)
    case "tsv":
        return rowsToTSV(rows, elapsedTime)
    default:
        return "", fmt.Errorf("unsupported format: %s", format)
    }
}

func rowsToJSON(rows *sql.Rows, elapsedTime time.Duration) (string, error) {
    columns, err := rows.Columns()
    if err != nil {
        return "", err
    }

    count := len(columns)
    tableData := make([]map[string]interface{}, 0)
    values := make([]interface{}, count)
    valuePtrs := make([]interface{}, count)

    for rows.Next() {
        for i := range columns {
            valuePtrs[i] = &values[i]
        }
        rows.Scan(valuePtrs...)

        entry := make(map[string]interface{})
        for i, col := range columns {
            var v interface{}
            val := values[i]
            b, ok := val.([]byte)
            if ok {
                v = string(b)
            } else {
                v = val
            }
            entry[col] = v
        }

        tableData = append(tableData, entry)
    }

    result := map[string]interface{}{
        "data":         tableData,
        "elapsed_time": elapsedTime.String(),
    }

    jsonData, err := json.Marshal(result)
    if err != nil {
        return "", err
    }

    return string(jsonData), nil
}

func rowsToCSV(rows *sql.Rows, elapsedTime time.Duration) (string, error) {
    var sb strings.Builder
    writer := csv.NewWriter(&sb)

    columns, err := rows.Columns()
    if err != nil {
        return "", err
    }

    writer.Write(columns)

    count := len(columns)
    values := make([]interface{}, count)
    valuePtrs := make([]interface{}, count)

    for rows.Next() {
        for i := range columns {
            valuePtrs[i] = &values[i]
        }
        rows.Scan(valuePtrs...)

        row := make([]string, count)
        for i, col := range columns {
            var v interface{}
            val := values[i]
            b, ok := val.([]byte)
            if ok {
                v = string(b)
            } else {
                v = val
            }
            row[i] = fmt.Sprintf("%v", v)
        }

        writer.Write(row)
    }

    writer.Flush()
    if err := writer.Error(); err != nil {
        return "", err
    }

    sb.WriteString(fmt.Sprintf("\nElapsed Time: %s", elapsedTime))
    return sb.String(), nil
}

func rowsToTSV(rows *sql.Rows, elapsedTime time.Duration) (string, error) {
    var sb strings.Builder
    writer := csv.NewWriter(&sb)
    writer.Comma = '\t'

    columns, err := rows.Columns()
    if err != nil {
        return "", err
    }

    writer.Write(columns)

    count := len(columns)
    values := make([]interface{}, count)
    valuePtrs := make([]interface{}, count)

    for rows.Next() {
        for i := range columns {
            valuePtrs[i] = &values[i]
        }
        rows.Scan(valuePtrs...)

        row := make([]string, count)
        for i, col := range columns {
            var v interface{}
            val := values[i]
            b, ok := val.([]byte)
            if ok {
                v = string(b)
            } else {
                v = val
            }
            row[i] = fmt.Sprintf("%v", v)
        }

        writer.Write(row)
    }

    writer.Flush()
    if err := writer.Error(); err != nil {
        return "", err
    }

    sb.WriteString(fmt.Sprintf("\nElapsed Time: %s", elapsedTime))
    return sb.String(), nil
}

// Existing handlers for backwards compatibility
type SessionManager struct {
    sessions map[string]*Session
    mu       sync.Mutex
}

var manager = &SessionManager{sessions: make(map[string]*Session)}

func startSessionHandler(w http.ResponseWriter, r *http.Request) {
    params := r.URL.Query().Get("params")
    session, err := manager.NewSession(params)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    json.NewEncoder(w).Encode(map[string]string{"session_id": session.ID})
}

func closeSessionHandler(w http.ResponseWriter, r *http.Request) {
    sessionID := r.URL.Query().Get("session_id")

    err := manager.CloseSession(sessionID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }

    w.WriteHeader(http.StatusOK)
}

func (sm *SessionManager) NewSession(params string) (*Session, error) {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    db, err := sql.Open("duckdb", params)
    if err != nil {
        return nil, err
    }

    session := &Session{
        ID: generateSessionID(),
        DB: db,
    }
    sm.sessions[session.ID] = session
    return session, nil
}

func (sm *SessionManager) CloseSession(id string) error {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    session, exists := sm.sessions[id]
    if !exists {
        return fmt.Errorf("session not found")
    }

    err := session.DB.Close()
    if err != nil {
        return err
    }

    delete(sm.sessions, id)
    return nil
}

func generateSessionID() string {
    return fmt.Sprintf("%d", time.Now().UnixNano())
}

