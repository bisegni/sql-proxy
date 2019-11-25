/*
* Radon
*
* Copyright 2018 The Radon Authors.
* Code is licensed under the GPLv3.
*
*/

package proxy

import (
	"fmt"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleShowDatabases used to handle the 'SHOW DATABASES' command.
func (p *Proxy) handleShowDatabases(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	qr, err := p.ExecuteSingle(query)
	if err != nil {
		return nil, err
	}

//  privilegePlug := spanner.plugins.PlugPrivilege()
//  isSuper := privilegePlug.IsSuperPriv(session.User())
//  if isSuper {
// 	 return qr, nil
//  } else {
// 	 isSet := privilegePlug.CheckUserPrivilegeIsSet(session.User())
// 	 if isSet {
// 		 return qr, nil
// 	 } else {
// 		 newqr := &sqltypes.Result{}
// 		 for _, row := range qr.Rows {
// 			 db := string(row[0].Raw())
// 			 if isExist := privilegePlug.CheckDBinUserPrivilege(session.User(), db); isExist {
// 				 newqr.RowsAffected++
// 				 newqr.Rows = append(newqr.Rows, row)
// 			 }
// 		 }

// 		 newqr.Fields = []*querypb.Field{
// 			 {Name: "Database", Type: querypb.Type_VARCHAR},
// 		 }
// 		 return newqr, nil
// 	 }
//  }
return qr, err
}

// handleShowEngines used to handle the 'SHOW ENGINES' command.
func (p *Proxy) handleShowEngines(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return p.ExecuteSingle(query)
}

// handleShowCreateDatabase used to handle the 'SHOW CREATE DATABASE' command.
func (p *Proxy) handleShowCreateDatabase(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return p.ExecuteSingle(query)
}

// handleShowTableStatus used to handle the 'SHOW TABLE STATUS' command.
// | Name          | Engine | Version | Row_format  | Rows    | Avg_row_length | Data_length | Max_data_length     | Index_length | Data_free            | Auto_increment | Create_time         | Update_time         | Check_time | Collation       | Checksum | Create_options | Comment |
// +---------------+--------+---------+-------------+---------+----------------+-------------+---------------------+--------------+----------------------+----------------+---------------------+---------------------+------------+-----------------+----------+----------------+---------+
// | block_0000    | TokuDB |      10 | tokudb_zstd |    6134 |           1395 |     8556930 | 9223372036854775807 |       509122 | 18446744073704837574 |           NULL | 2019-04-24 17:36:10 | 2019-05-04 12:47:45 | NULL       | utf8_general_ci |     NULL |                |
func (p *Proxy) handleShowTableStatus(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	ast := node.(*sqlparser.Show)
//  router := p.router

	database := session.Schema()
	if !ast.Database.IsEmpty() {
		database = ast.Database.Name.String()
	}

	if database == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
	}
//  // Check the database ACL.
//  if err := router.DatabaseACL(database); err != nil {
// 	 return nil, err
//  }

	rewritten := fmt.Sprintf("SHOW TABLE STATUS from %s", database)
	qr, err := p.ExecuteScatter(rewritten)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// handleShowTables used to handle the 'SHOW TABLES' command.
func (p *Proxy) handleShowTables(session *driver.Session, query string, node *sqlparser.Show) (*sqltypes.Result, error) {
//  router := spanner.router
	ast := node

	database := session.Schema()
	if !ast.Database.IsEmpty() {
		database = ast.Database.Name.String()
	}
	if database == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
	}
	// Check the database ACL.
//  if err := router.DatabaseACL(database); err != nil {
// 	 return nil, err
//  }

	// For validating the query works, we send it to the backend and check the error.
	rewritten := fmt.Sprintf("SHOW TABLES FROM %s", database)
	_, err := p.ExecuteScatter(rewritten)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (p *Proxy) handleShowCreateTable(session *driver.Session, query string, node *sqlparser.Show) (*sqltypes.Result, error) {
//  router := spanner.router
	ast := node

	// table := ast.Table.Name.String()
	database := session.Schema()
	if !ast.Table.Qualifier.IsEmpty() {
		database = ast.Table.Qualifier.String()
	}
	if database == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
	}
	// Check the database ACL.
	// if err := router.DatabaseACL(database); err != nil {
	// 	return nil, err
	// }

	var qr *sqltypes.Result
	var err error

	// tableConfig, err := router.TableConfig(database, table)
	// if err != nil {
	// 	return nil, err
	// }
	// tableType := tableConfig.ShardType

	// shardKey, err := router.ShardKey(database, table)
	// if err != nil {
	// 	return nil, err
	// }
	return qr, err
}

// handleShowColumns used to handle the 'SHOW COLUMNS' command.
func (p *Proxy) handleShowColumns(session *driver.Session, query string, node *sqlparser.Show) (*sqltypes.Result, error) {
	// router := spanner.router
	ast := node

	// table := ast.Table.Name.String()
	database := session.Schema()
	if !ast.Table.Qualifier.IsEmpty() {
		database = ast.Table.Qualifier.String()
	}
	if database == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
	}
	/*
	// Check the database ACL.
	if err := router.DatabaseACL(database); err != nil {
		return nil, err
	}

	// Get one table from the router.
	parts, err := router.Lookup(database, table, nil, nil)
	if err != nil {
		return nil, err
	}
	partTable := parts[0].Table
	backend := parts[0].Backend
	rewritten := fmt.Sprintf("SHOW COLUMNS FROM %s.%s", database, partTable)
	qr, err := spanner.ExecuteOnThisBackend(backend, rewritten)
	if err != nil {
		return nil, err
	}*/
	return nil, nil
}

// handleShowProcesslist used to handle the query "SHOW PROCESSLIST".
func (p *Proxy) handleShowProcesslist(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	// sessions := spanner.sessions
	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "Id", Type: querypb.Type_INT64},
		{Name: "User", Type: querypb.Type_VARCHAR},
		{Name: "Host", Type: querypb.Type_VARCHAR},
		{Name: "db", Type: querypb.Type_VARCHAR},
		{Name: "Command", Type: querypb.Type_VARCHAR},
		{Name: "Time", Type: querypb.Type_INT32},
		{Name: "State", Type: querypb.Type_VARCHAR},
		{Name: "Info", Type: querypb.Type_VARCHAR},
		{Name: "Rows_sent", Type: querypb.Type_INT64},
		{Name: "Rows_examined", Type: querypb.Type_INT64},
	}

	// var sessionInfos []SessionInfo
	// privilegePlug := spanner.plugins.PlugPrivilege()
	// if privilegePlug.IsSuperPriv(session.User()) {
	// 	sessionInfos = sessions.Snapshot()
	// } else {
	// 	sessionInfos = sessions.SnapshotUser(session.User())
	// }

	// for _, info := range sessionInfos {
	// 	row := []sqltypes.Value{
	// 		sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", info.ID))),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.User)),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.Host)),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.DB)),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.Command)),
	// 		sqltypes.MakeTrusted(querypb.Type_INT32, []byte(fmt.Sprintf("%v", info.Time))),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.State)),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.Info)),
	// 		sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", 0))),
	// 		sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", 0))),
	// 	}
	// 	qr.Rows = append(qr.Rows, row)
	// }
	return qr, nil
}

// handleShowStatus used to handle the query "SHOW STATUS".
func (p *Proxy) handleShowStatus(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	// var varname string
	// log := p.log
	// scatter := spanner.scatter

	// privilegePlug := spanner.plugins.PlugPrivilege()
	// if !privilegePlug.IsSuperPriv(session.User()) {
	// 	return nil, sqldb.NewSQLErrorf(sqldb.ER_SPECIFIC_ACCESS_DENIED_ERROR, "Access denied; lacking super privilege for the operation")
	// }

	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "Variable_name", Type: querypb.Type_VARCHAR},
		{Name: "Value", Type: querypb.Type_VARCHAR},
	}

	// // 1. radon_rate row.
	// varname = "radon_rate"
	// rate := scatter.QueryRates()
	// qr.Rows = append(qr.Rows, []sqltypes.Value{
	// 	sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(varname)),
	// 	sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(rate.String())),
	// })

	// // 2. radon_config row.
	// var confJSON []byte
	// varname = "radon_config"
	// type confShow struct {
	// 	MaxConnections int      `json:"max-connections"`
	// 	MaxResultSize  int      `json:"max-result-size"`
	// 	MaxJoinRows    int      `json:"max-join-rows"`
	// 	DDLTimeout     int      `json:"ddl-timeout"`
	// 	QueryTimeout   int      `json:"query-timeout"`
	// 	TwopcEnable    bool     `json:"twopc-enable"`
	// 	AllowIP        []string `json:"allow-ip"`
	// 	AuditMode      string   `json:"audit-log-mode"`
	// 	ReadOnly       bool     `json:"readonly"`
	// 	Throttle       int      `json:"throttle"`
	// }
	// conf := confShow{
	// 	MaxConnections: spanner.conf.Proxy.MaxConnections,
	// 	MaxResultSize:  spanner.conf.Proxy.MaxResultSize,
	// 	MaxJoinRows:    spanner.conf.Proxy.MaxJoinRows,
	// 	DDLTimeout:     spanner.conf.Proxy.DDLTimeout,
	// 	QueryTimeout:   spanner.conf.Proxy.QueryTimeout,
	// 	TwopcEnable:    spanner.conf.Proxy.TwopcEnable,
	// 	AllowIP:        spanner.conf.Proxy.IPS,
	// 	AuditMode:      spanner.conf.Audit.Mode,
	// 	ReadOnly:       spanner.readonly.Get(),
	// 	Throttle:       spanner.throttle.Limits(),
	// }
	// if b, err := json.Marshal(conf); err != nil {
	// 	confJSON = []byte(err.Error())
	// } else {
	// 	confJSON = b
	// }
	// qr.Rows = append(qr.Rows, []sqltypes.Value{
	// 	sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(varname)),
	// 	sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(confJSON)),
	// })

	// // 3. radon_counter row.
	// varname = "radon_transaction"
	// txnCounter := scatter.TxnCounters()
	// qr.Rows = append(qr.Rows, []sqltypes.Value{
	// 	sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(varname)),
	// 	sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(txnCounter.String())),
	// })

	// // 4. radon_backend_pool row.
	// var poolJSON []byte
	// varname = "radon_backendpool"
	// type poolShow struct {
	// 	Pools []string
	// }
	// be := poolShow{}
	// poolz := scatter.PoolClone()
	// for _, v := range poolz {
	// 	be.Pools = append(be.Pools, v.JSON())
	// }

	// sort.Strings(be.Pools)
	// if b, err := json.MarshalIndent(be, "", "\t\t\t"); err != nil {
	// 	poolJSON = []byte(err.Error())
	// } else {
	// 	poolJSON = b
	// }
	// qr.Rows = append(qr.Rows, []sqltypes.Value{
	// 	sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(varname)),
	// 	sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(poolJSON)),
	// })

	// // 5. backends row.
	// var backendsJSON []byte
	// varname = "radon_backend"
	// type backendShow struct {
	// 	Backends []string
	// }
	// bs := backendShow{}

	// backShowFunc := func(backend string, qr *sqltypes.Result) {
	// 	tables := "0"
	// 	datasize := "0MB"

	// 	if len(qr.Rows) > 0 {
	// 		tables = string(qr.Rows[0][0].Raw())
	// 		if string(qr.Rows[0][1].Raw()) != "" {
	// 			datasize = string(qr.Rows[0][1].Raw()) + "MB"
	// 		}
	// 	}
	// 	buff := bytes.NewBuffer(make([]byte, 0, 256))
	// 	fmt.Fprintf(buff, `{"name": "%s","tables": "%s", "datasize":"%s"}`, backend, tables, datasize)
	// 	bs.Backends = append(bs.Backends, buff.String())
	// }

	// sql := "select count(0), round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  from information_schema.TABLES  where table_schema not in ('sys', 'information_schema', 'mysql', 'performance_schema')"
	// backends := spanner.scatter.AllBackends()
	// for _, backend := range backends {
	// 	qr, err := spanner.ExecuteOnThisBackend(backend, sql)
	// 	if err != nil {
	// 		log.Error("proxy.show.execute.on.this.backend[%x].error:%+v", backend, err)
	// 	} else {
	// 		backShowFunc(backend, qr)
	// 	}
	// }

	// sort.Strings(bs.Backends)
	// if b, err := json.MarshalIndent(bs, "", "\t\t\t"); err != nil {
	// 	backendsJSON = []byte(err.Error())
	// } else {
	// 	backendsJSON = b
	// }
	// qr.Rows = append(qr.Rows, []sqltypes.Value{
	// 	sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(varname)),
	// 	sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(backendsJSON)),
	// })
	return qr, nil
}

// handleShowQueryz used to handle the query "SHOW QUERYZ".
func (p *Proxy) handleShowQueryz(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	// privilegePlug := spanner.plugins.PlugPrivilege()
	// if !privilegePlug.IsSuperPriv(session.User()) {
	// 	return nil, sqldb.NewSQLErrorf(sqldb.ER_SPECIFIC_ACCESS_DENIED_ERROR, "Access denied; lacking super privilege for the operation")
	// }

	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "ConnID", Type: querypb.Type_INT64},
		{Name: "Host", Type: querypb.Type_VARCHAR},
		{Name: "Start", Type: querypb.Type_VARCHAR},
		{Name: "Duration", Type: querypb.Type_INT32},
		{Name: "Query", Type: querypb.Type_VARCHAR},
	}
	// rows := spanner.scatter.Queryz().GetQueryzRows()
	// for _, row := range rows {
	// 	row := []sqltypes.Value{
	// 		sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", uint64(row.ConnID)))),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.Address)),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.Start.Format("20060102150405.000"))),
	// 		sqltypes.MakeTrusted(querypb.Type_INT32, []byte(fmt.Sprintf("%v", row.Duration))),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.Query)),
	// 	}
	// 	qr.Rows = append(qr.Rows, row)
	// }
	return qr, nil
}

// handleShowTxnz used to handle the query "SHOW TXNZ".
func (p *Proxy) handleShowTxnz(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return nil, sqldb.NewSQLErrorf(sqldb.ER_SPECIFIC_ACCESS_DENIED_ERROR, "Access denied; lacking super privilege for the operation")
	// privilegePlug := spanner.plugins.PlugPrivilege()
	// if !privilegePlug.IsSuperPriv(session.User()) {
	// 	return nil, sqldb.NewSQLErrorf(sqldb.ER_SPECIFIC_ACCESS_DENIED_ERROR, "Access denied; lacking super privilege for the operation")
	// }

	// qr := &sqltypes.Result{}
	// qr.Fields = []*querypb.Field{
	// 	{Name: "TxnID", Type: querypb.Type_INT64},
	// 	{Name: "Start", Type: querypb.Type_VARCHAR},
	// 	{Name: "Duration", Type: querypb.Type_INT32},
	// 	{Name: "XaState", Type: querypb.Type_VARCHAR},
	// 	{Name: "TxnState", Type: querypb.Type_VARCHAR},
	// }

	// rows := spanner.scatter.Txnz().GetTxnzRows()
	// for _, row := range rows {
	// 	row := []sqltypes.Value{
	// 		sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", uint64(row.TxnID)))),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.Start.Format("20060102150405.000"))),
	// 		sqltypes.MakeTrusted(querypb.Type_INT32, []byte(fmt.Sprintf("%v", row.Duration))),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.XaState)),
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.State)),
	// 	}
	// 	qr.Rows = append(qr.Rows, row)
	// }
	// return qr, nil
}

func (p *Proxy) handleShowVersions(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "Versions", Type: querypb.Type_VARCHAR},
	}

	// build := build.GetInfo()
	row := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(fmt.Sprintf("radon:%+v", "1.0.0"))),
	}
	qr.Rows = append(qr.Rows, row)
	return qr, nil
}

func (p *Proxy) handleJDBCShows(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return p.ExecuteSingle(query)
}