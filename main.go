package main

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/sevlyar/go-daemon"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	dbMap  = make(map[string]*sql.DB)
	dbLock = new(sync.Mutex)
)

const (
	CONN_ERRNO = 28000
)

func getDB(connStr string) (*sql.DB, error) {
	dbLock.Lock()
	defer dbLock.Unlock()

	if db, ok := dbMap[connStr]; ok {
		return db, nil
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	dbMap[connStr] = db
	return db, nil
}

func checkError(err error) (uint16, string) {
	if err == nil {
		return 0, ""
	}

	return 500, err.Error()
}

func handleHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=x-user-defined")

	user := req.PostFormValue("login")
	pass := req.PostFormValue("password")
	host := req.PostFormValue("host")
	port := req.PostFormValue("port")
	addr := host + ":" + port
	dbname := req.PostFormValue("db")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, dbname)

	db, err := getDB(connStr)
	var errno uint16
	var errmsg string
	if err != nil {
		if errno, errmsg = checkError(err); errno > 0 {
			EchoHeader(uint16(CONN_ERRNO), GetBlock(errmsg), w)
			return
		}
	}

	EchoHeader(0, nil, w)
	actn := req.PostFormValue("actn")
	if actn == "C" {
		EchoConnInfo(addr, db, w)
		return
	}

	if actn == "Q" {

		querys := getQuerys(req)
		for i, q := range querys {
			if q == "" {
				continue
			}

			var rows *sql.Rows
			var result sql.Result
			var err error
			var isExec bool = false
			qStr := strings.TrimSpace(strings.ToLower(q))
			fmt.Println(q)
			if strings.HasPrefix(qStr, "update") || strings.HasPrefix(qStr, "delete") {
				result, err = db.Exec(q)
				isExec = true
			} else {
				rows, err = db.Query(q)
			}

			numcols := 0
			numrows := 0
			affectrows := int64(0)
			insertid := int64(0)
			var colnames []string
			var cols []*sql.ColumnType
			resultSet := make([][]interface{}, 0)
			if errno, errmsg = checkError(err); errno <= 0 {
				if isExec {
					affectrows, _ = result.RowsAffected()
					insertid, _ = result.LastInsertId()

				} else {
					colnames, _ = rows.Columns()
					cols, _ = rows.ColumnTypes()
					numcols = len(colnames)
					numrows = 0
					data := make([]interface{}, numcols)
					for j := range data {
						data[j] = new(interface{})
					}
					for rows.Next() {
						resultRow := make([]interface{}, numcols)
						rows.Scan(data...)
						for k, _ := range colnames {
							val := data[k].(*interface{})
							resultRow[k] = *val
						}
						resultSet = append(resultSet, resultRow)
						numrows++
					}
				}

				EchoResultSetHeader(errno, int(affectrows), int(insertid), numcols, numrows, w)
				if errno > 0 {
					w.Write(GetBlock(errmsg))
				} else {
					if numcols > 0 {
						EchoFieldsHeader(colnames, cols, numcols, w)
						EchoData(resultSet, w, cols)
					} else {
						w.Write(GetBlock(""))
					}
				}

			} else {
				EchoResultSetHeader(errno, 0, 0, 0, 0, w)
				w.Write(GetBlock(errmsg))
			}

			if i < len(querys)-1 {
				w.Write([]byte("\x01"))
			} else {
				w.Write([]byte("\x00"))
			}
		}
	}
}

func getQuerys(req *http.Request) []string {
	var querys []string
	for key, values := range req.PostForm {
		if key != "q[]" {
			continue
		}
		for _, q := range values {
			if enc := req.PostFormValue("encodeBase64"); enc == "1" {
				c := 4 - (len(q) % 4)
				if c > 0 && c < 4 {
					q += strings.Repeat("=", c)
				}

				data, err := base64.StdEncoding.DecodeString(q)
				if err != nil {
					continue
				}
				q = string(data)
			}
			querys = append(querys, q)
		}
	}
	return querys
}

func EchoHeader(errno uint16, msg []byte, w http.ResponseWriter) {
	w.Write(GetLongBinary(1111))
	w.Write(GetShortBinary(201))
	w.Write(GetLongBinary(int(errno)))
	w.Write(GetDummy(6))

	if msg != nil {
		w.Write(msg)
	}
}

func GetLongBinary(num int) []byte {
	buf := new(bytes.Buffer)
	byteOrder := binary.BigEndian
	binary.Write(buf, byteOrder, uint32(num))
	return buf.Bytes()
}

func GetShortBinary(num int) []byte {
	buf := new(bytes.Buffer)
	byteOrder := binary.BigEndian
	binary.Write(buf, byteOrder, uint16(num))
	return buf.Bytes()
}

func GetDummy(count int) []byte {
	var b []byte
	for i := 0; i < count; i++ {
		b = append(b, 0)
	}
	return b
}

func GetBlock(val string) []byte {
	buf := new(bytes.Buffer)
	l := len(val)
	if l < 254 {
		binary.Write(buf, binary.BigEndian, uint8(l))
		buf.WriteString(val)
	} else {
		buf.Write([]byte("\xFE"))
		buf.Write(GetLongBinary(l))
		buf.WriteString(val)
	}
	return buf.Bytes()
}

func EchoConnInfo(addr string, db *sql.DB, w http.ResponseWriter) {
	rows, err := db.Query("SELECT current_setting('server_version_num')")
	if err != nil {
		log.Println(err.Error())
		return
	}

	var serverVer = "0"
	var proto = "3"
	for rows.Next() {
		err = rows.Scan(&serverVer)
		if err != nil {
			log.Println(err.Error())
			return
		}
	}

	w.Write(GetBlock(addr))
	w.Write(GetBlock(proto))
	w.Write(GetBlock(serverVer))
}

func EchoResultSetHeader(errno uint16, affectrows int, insertid int, numfields int, numrows int, w http.ResponseWriter) {
	w.Write(GetLongBinary(int(errno)))
	w.Write(GetLongBinary(affectrows))
	w.Write(GetLongBinary(insertid))
	w.Write(GetLongBinary(numfields))
	w.Write(GetLongBinary(numrows))
	w.Write(GetDummy(12))
}

func EchoFieldsHeader(colNames []string, cols []*sql.ColumnType, numcols int, w http.ResponseWriter) {

	for i := 0; i < numcols; i++ {
		colType := strings.ToLower(cols[i].DatabaseTypeName())
		len, _ := cols[i].Length()
		var typ int
		switch colType {
		case "bool", "boolean":
			typ = 16
			len = 1
		case "bytea":
			typ = 25
			//typ = 17
		case "bit":
			typ = 1560
		case "varbit":
			typ = 1562
		case "char":
			typ = 18
		case "name":
			typ = 19
		case "int2vector":
			typ = 22
		case "oidvector":
			typ = 30
		case "int8":
			typ = 20
		case "tid":
			typ = 27
		case "int2":
			typ = 21
		case "int4":
			typ = 23
		case "oid":
			typ = 26
			len = 4
		case "xid":
			typ = 28
		case "cid":
			typ = 29
		case "text":
			typ = 25
		case "money":
			typ = 790
		case "numeric":
			typ = 1700
		case "point":
			typ = 600
		case "lseg":
			typ = 601
		case "path":
			typ = 602
		case "box":
			typ = 603
		case "polygon":
			typ = 604
		case "line":
			typ = 628
		case "circle":
			typ = 718
		case "float4":
			typ = 700
		case "float8":
			typ = 701
		case "abstime":
			typ = 702
		case "tinterval":
			typ = 704
		case "timestamp":
			typ = 1114
		case "timestamptz":
			typ = 1184
		case "interval":
			typ = 1186
		case "timetz":
			typ = 1266
		case "unknown":
			typ = 705
		case "macaddr":
			typ = 829
		case "inet":
			typ = 869
		case "cidr":
			typ = 650
		case "bpchar":
			typ = 1042
		case "varchar":
			typ = 1043
		case "date":
			typ = 1082
		case "time":
			typ = 1083
		case "regproc":
			typ = 24
		case "refcursor":
			typ = 1790
		case "regprocedure":
			typ = 2202
		case "regoper":
			typ = 2203
		case "regoperator":
			typ = 2204
		case "regclass":
			typ = 2205
		case "regtype":
			typ = 2206
		default:
			typ = 0
		}

		w.Write(GetBlock(colNames[i]))
		w.Write(GetBlock(""))
		w.Write(GetLongBinary(typ))
		w.Write(GetLongBinary(0))
		w.Write(GetLongBinary(int(len)))
	}
}

func EchoData(resultset [][]interface{}, w http.ResponseWriter, cols []*sql.ColumnType) {
	for _, row := range resultset {
		buf := new(bytes.Buffer)
		for k, v := range row {
			if v == nil {
				buf.Write([]byte("\xFF"))
			} else {
				var val string
				switch cols[k].DatabaseTypeName() {
				case "OID":
					val = string(v.([]uint8))
				case "BOOL", "BOOLEAN":
					if v.(bool) {
						val = "t"
					} else {
						val = "f"
					}
				case "INT2", "INT4", "INT8", "BIGINT", "INTEGER":
					val = fmt.Sprintf("%d", v)
				case "NUMERIC":
					val = string(v.([]uint8))
				case "FLOAT4", "FLOAT8":
					val = fmt.Sprintf("%f", v)
				case "BYTEA":
					val = string(v.([]uint8))
				case "TIMESTAMP":
					val = fmt.Sprint(v.(time.Time).Format("2006-01-02 15:04:05"))
				default:
					val = fmt.Sprintf("%s", v)
					escapedVal := strings.ReplaceAll(val, "\\", "\\\\")
					escapedVal = strings.ReplaceAll(escapedVal, "\n", "\n")
					escapedVal = strings.ReplaceAll(escapedVal, "\r", "\r")
					escapedVal = strings.ReplaceAll(escapedVal, "'", "''")
					val = escapedVal
				}
				buf.Write(GetBlock(val))
			}
		}
		w.Write(buf.Bytes())
	}
}

func main() {
	if runtime.GOOS != "windows" {
		cntxt := &daemon.Context{
			PidFileName: "pid",
			PidFilePerm: 0644,
			//LogFileName: "go-pgsql-tunnel.log",
			//LogFilePerm: 0640,
			WorkDir: "./",
			Umask:   027,
			Args:    os.Args,
		}

		d, err := cntxt.Reborn()
		if err != nil {
			log.Fatal("Unable to run: ", err.Error())
		}
		if d != nil {
			return
		}
		defer cntxt.Release()

		log.Println("go-pgsql-tunnel daemon started")
	}

	var port = flag.Int("p", 8080, "set app port with -p=xxx or -p xxx.")
	if !flag.Parsed() {
		flag.Parse()
	}
	defer func() {
		dbLock.Lock()
		defer dbLock.Unlock()

		for _, db := range dbMap {
			db.Close()
		}
	}()
	portStr := strconv.Itoa(*port)
	mux := http.NewServeMux()
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleHTTP(w, r)
	}))

	err := http.ListenAndServe(":"+portStr, mux)
	if err != nil {
		log.Println("ListenAndServe: ", err.Error())
	}
}
