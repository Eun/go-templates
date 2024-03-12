package main

import (
	"database/sql"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	goose "github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var embedMigrations embed.FS

func main() {
	var db *sql.DB
	db, err := sql.Open("mysql", os.Getenv("DATABASE_DSN"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	goose.SetBaseFS(embedMigrations)

	if err := goose.SetDialect("mysql"); err != nil {
		panic(err)
	}

	if err := goose.Up(db, "migrations"); err != nil {
		panic(err)
	}

	http.HandleFunc("/list", listHandler(db))
	http.HandleFunc("/insert", insertHandler(db))
	http.HandleFunc("/clear", clearHandler(db))

	if err := http.ListenAndServe("127.0.0.1:8000", nil); err != nil {
		panic(err)
	}
}

func listHandler(db *sql.DB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// the list handler utilizes the system versioned tables feature of MariaDB to
		// create responses immutable to changes in the database.
		// the handler uses two types of tokens:
		// - next page token: used to get the next page of results
		// - sync token: used to get all changes since a certain point in time
		// The handler will return a next page token if there are more results to fetch
		// and a sync token if there are no more results to fetch.
		//
		// The flow of the handler is as follows:
		// 1. A client makes a request to the list endpoint and specifies no next page token or sync token.
		// 2. The handler will fetch the first page using the current time as reference.
		// 3. If the result set is not empty and the result set is equal to the max results, the handler will return a next page token.
		//        No sync token will be included.
		//
		//        The next page token contains:
		//        - the current timestamp
		//            the current timestamp is needed so the next request can use the same time as reference to fetch the next set of items
		//        - the "updated after" timestamp
		//            this is used to build a WHERE clause to only select rows that have been updated after this timestamp
		//            this is only needed when the sync token was used - read below
		//        - the offset of the next page
		//            this is used to skip the first n rows
		//        - the "valid until" timestamp
		//            Due to storage limitations, the history of the database will be cleared after a certain period of time (here 24hours)
		//            and it cannot provide data older than that period
		// 4. If the result set is empty or the result set is less than the max results, the handler will return a sync token.
		//       No next page token will be included.
		//
		//       The sync token contains:
		//       - the current timestamp
		//       - the "valid until" timestamp
		//
		//       The current timestamp will be used (in the next request) as reference to fetch the next set of items.
		//       Also the "updated after" timestamp will be set to this timestamp, this has the effect that only items that have been updated
		//       after this timestamp will be returned.
		//
		//  Notice that this implementation is not optimal, as the client can modify next page and sync tokens.
		//  There are various ways to mitigate this, for example by encrypting the tokens, using HMAC or storing the tokens
		//  in a cache or database. The implementation here is just a simple example.
		//  Choose the best approach for your use case.

		tableInPointOfTime := time.Now().UTC()
		var onlyNewerThan time.Time
		var offset int

		var nextPageToken *NextPageToken
		var syncToken *SyncToken

		if v := r.URL.Query().Get("sync_token"); v != "" {
			syncToken = &SyncToken{}
			if err := syncToken.UnmarshalJSON([]byte(v)); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "%+v", err)
				return
			}
			if syncToken.ValidUntil.Before(time.Now()) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "sync token expired")
				return
			}
			onlyNewerThan = syncToken.Timestamp
			offset = 0
			syncToken = nil
			nextPageToken = nil
		} else if v := r.URL.Query().Get("next_page_token"); v != "" {
			nextPageToken = &NextPageToken{}
			if err := nextPageToken.UnmarshalJSON([]byte(v)); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "%+v", err)
				return
			}
			if nextPageToken.ValidUntil.Before(time.Now()) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "next page token expired")
				return
			}
			tableInPointOfTime = nextPageToken.Timestamp
			onlyNewerThan = nextPageToken.UpdatedAfter
			offset = nextPageToken.Offset
			syncToken = nil
			nextPageToken = nil
		}

		maxResults := 10
		if v := r.URL.Query().Get("max_results"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				maxResults = n
			}
		}

		var rows *sql.Rows
		var err error
		if onlyNewerThan.IsZero() {
			rows, err = db.Query(
				"SELECT * FROM users FOR SYSTEM_TIME AS OF TIMESTAMP ? ORDER BY name LIMIT ? OFFSET ?",
				tableInPointOfTime.Format(time.DateTime),
				maxResults,
				offset,
			)
		} else {
			rows, err = db.Query(
				"SELECT * FROM users FOR SYSTEM_TIME AS OF TIMESTAMP ? WHERE updated_at > ? ORDER BY name LIMIT ? OFFSET ?",
				tableInPointOfTime.Format(time.DateTime),
				onlyNewerThan,
				maxResults,
				offset,
			)
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "%+v", err)
			return
		}
		defer rows.Close()

		users := []User{}
		for rows.Next() {
			var user User
			if err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.UpdatedAt, &user.CreatedAt); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "%+v", err)
				return
			}
			users = append(users, user)
		}
		if err = rows.Err(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "%+v", err)
			return
		}

		if len(users) == 0 || len(users) < maxResults {
			syncToken = &SyncToken{Timestamp: tableInPointOfTime, ValidUntil: time.Now().AddDate(0, 0, 1)}
		} else {
			nextPageToken = &NextPageToken{
				Timestamp:    tableInPointOfTime,
				UpdatedAfter: onlyNewerThan,
				Offset:       offset + len(users),
				ValidUntil:   time.Now().AddDate(0, 0, 1),
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(ListResponse{
			Users:         users,
			NextPageToken: nextPageToken,
			SyncToken:     syncToken,
		}); err != nil {
			log.Printf("error encoding response: %v", err)
		}
	}
}

func insertHandler(db *sql.DB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var user User
		if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "%+v", err)
			return
		}
		row := db.QueryRow(
			"INSERT INTO users (name, email) VALUES (?, ?) RETURNING *",
			user.Name,
			user.Email,
		)
		if err := row.Scan(&user.ID, &user.Name, &user.Email, &user.UpdatedAt, &user.CreatedAt); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "%+v", err)
			return
		}
		if err := row.Err(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "%+v", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(user); err != nil {
			log.Printf("error encoding response: %v", err)
		}
	}
}

func clearHandler(db *sql.DB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		res, err := db.Exec("DELETE FROM users")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "%+v", err)
			return
		}
		affectedRows, err := res.RowsAffected()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "%+v", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{"affected_rows": affectedRows}); err != nil {
			log.Printf("error encoding response: %v", err)
		}
	}
}

type User struct {
	ID        string `db:"id" json:"id"`
	Name      string `db:"name" json:"name"`
	Email     string `db:"email" json:"email"`
	CreatedAt string `db:"created_at" json:"created_at"`
	UpdatedAt string `db:"updated_at" json:"updated_at"`
}

type ListResponse struct {
	Users         []User         `json:"users"`
	NextPageToken *NextPageToken `json:"next_page_token"`
	SyncToken     *SyncToken     `json:"sync_token"`
}

type NextPageToken struct {
	Timestamp    time.Time
	UpdatedAfter time.Time
	Offset       int
	ValidUntil   time.Time
}

func (t *NextPageToken) MarshalJSON() ([]byte, error) {
	if t == nil {
		return []byte(`""`), nil
	}

	m := map[string]any{
		"t": t.Timestamp.Unix(),
		"o": t.Offset,
		"v": t.ValidUntil.Unix(),
	}

	if !t.UpdatedAfter.IsZero() {
		m["u"] = t.UpdatedAfter.Unix()
	}

	src, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	size := base64.StdEncoding.EncodedLen(len(src)) + 2
	buf := make([]byte, size)
	buf[0] = '"'
	buf[size-1] = '"'
	base64.StdEncoding.Encode(buf[1:], src)
	return buf, nil
}

func (t *NextPageToken) UnmarshalJSON(data []byte) error {
	buf := make([]byte, base64.StdEncoding.DecodedLen(len(data)))
	n, err := base64.StdEncoding.Decode(buf, data)
	if err != nil {
		return err
	}

	var v struct {
		Timestamp    int64  `json:"t"`
		Offset       int    `json:"o"`
		UpdatedAfter *int64 `json:"u"`
		ValidUntil   int64  `json:"v"`
	}
	if err := json.Unmarshal(buf[:n], &v); err != nil {
		return err
	}
	t.Timestamp = time.Unix(v.Timestamp, 0)
	t.Offset = v.Offset
	if v.UpdatedAfter != nil {
		t.UpdatedAfter = time.Unix(*v.UpdatedAfter, 0)
	}
	t.ValidUntil = time.Unix(v.ValidUntil, 0)
	return nil
}

type SyncToken struct {
	Timestamp  time.Time
	ValidUntil time.Time
}

func (t *SyncToken) MarshalJSON() ([]byte, error) {
	if t == nil {
		return []byte(`""`), nil
	}
	src, err := json.Marshal(struct {
		Timestamp  int64 `json:"t"`
		ValidUntil int64 `json:"v"`
	}{
		Timestamp:  t.Timestamp.Unix(),
		ValidUntil: t.ValidUntil.Unix(),
	})
	if err != nil {
		return nil, err
	}

	size := base64.StdEncoding.EncodedLen(len(src)) + 2
	buf := make([]byte, size)
	buf[0] = '"'
	buf[size-1] = '"'
	base64.StdEncoding.Encode(buf[1:], src)
	return buf, nil
}

func (t *SyncToken) UnmarshalJSON(data []byte) error {
	buf := make([]byte, base64.StdEncoding.DecodedLen(len(data)))
	n, err := base64.StdEncoding.Decode(buf, data)
	if err != nil {
		return err
	}

	var v struct {
		Timestamp  int64 `json:"t"`
		ValidUntil int64 `json:"v"`
	}
	if err := json.Unmarshal(buf[:n], &v); err != nil {
		return err
	}
	t.Timestamp = time.Unix(v.Timestamp, 0)
	t.ValidUntil = time.Unix(v.ValidUntil, 0)
	return nil
}
