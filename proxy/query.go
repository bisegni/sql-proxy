/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"strings"
	"time"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func truncateQuery(query string, max int) string {
	if max == 0 || len(query) <= max {
		return query
	}
	return query[:max] + " [TRUNCATED]"
}

func returnQuery(qr *sqltypes.Result, callback func(qr *sqltypes.Result) error, err error) error {
	if err != nil {
		return err
	}
	callback(qr)
	return nil
}

// ComQuery impl.
// Supports statements are:
// 1. DDL
// 2. DML
// 3. USE DB: MySQL client use 'database' won't pass here, FIXME.
func (p *Proxy) ComQuery(session *driver.Session,
	query string,
	bindVariables map[string]*querypb.BindVariable,
	callback func(qr *sqltypes.Result) error) error {
	var qr *sqltypes.Result
	log := p.log
	// throttle := p.throttle
	// diskChecker := p.diskChecker
	timeStart := time.Now()
	slowQueryTime := time.Duration( /*p.conf.Proxy.LongQueryTime*/ 10) * time.Second

	// Throttle.
	// throttle.Acquire()
	// defer throttle.Release()

	// Disk usage check.
	if true {
		return sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "%s", "no space left on device")
	}

	// Support for JDBC/Others driver.
	// if package.isConnectorFilter(query) {
	// 	qr, err := p.handleJDBCShows(session, query, nil)
	// 	if err == nil {
	// 		qr.Warnings = 1
	// 	}
	// 	return returnQuery(qr, callback, err)
	// }

	// Trim space and ';'.
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";")

	node, err := sqlparser.Parse(query)
	if err != nil {
		log.Error("query[%v].parser.error: %v", query, err)
		return sqldb.NewSQLError(sqldb.ER_SYNTAX_ERROR, err.Error())
	}

	// Bind variables.
	if bindVariables != nil {
		parsedQuery := sqlparser.NewParsedQuery(node)
		query, err = parsedQuery.GenerateQuery(bindVariables, nil)
		if err != nil {
			log.Error("query[%v].parsed.GenerateQuery.error: %v, bind:%+v", query, err, bindVariables)
			return sqldb.NewSQLError(sqldb.ER_SYNTAX_ERROR, err.Error())
		}

		// This sucks.
		node, err = sqlparser.Parse(query)
		if err != nil {
			log.Error("query[%v].parser.error: %v", query, err)
			return sqldb.NewSQLError(sqldb.ER_SYNTAX_ERROR, err.Error())
		}
	}
	log.Debug("query:%v", query)

	// Readonly check.
	// if p.ReadOnly() {
	// 	// DML Write denied.
	// 	if p.IsDMLWrite(node) {
	// 		return sqldb.NewSQLError(sqldb.ER_OPTION_PREVENTS_STATEMENT, "--read-only")
	// 	}
	// 	// DDL denied.
	// 	if p.IsDDL(node) {
	// 		return sqldb.NewSQLError(sqldb.ER_OPTION_PREVENTS_STATEMENT, "--read-only")
	// 	}
	// }

	defer func() {
		queryStat(log, node, timeStart, slowQueryTime, err)
	}()
	switch node := node.(type) {
	case *sqlparser.Use:
		if qr, err = p.handleUseDB(session, query, node); err != nil {
			log.Error("proxy.usedb[%s].from.session[%v].error:%+v", query, session.ID(), err)
		}
		// spanner.auditLog(session, R, xbase.USEDB, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.DDL:
		if qr, err = p.handleDDL(session, query, node); err != nil {
			log.Error("proxy.DDL[%s].from.session[%v].error:%+v", query, session.ID(), err)
		}
		// spanner.auditLog(session, W, xbase.DDL, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Show:
		show := node
		switch show.Type {
		case sqlparser.ShowDatabasesStr:
			if qr, err = p.handleShowDatabases(session, query, node); err != nil {
				log.Error("proxy.show.databases[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowStatusStr:
			if qr, err = p.handleShowStatus(session, query, node); err != nil {
				log.Error("proxy.show.status[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowVersionsStr:
			if qr, err = p.handleShowVersions(session, query, node); err != nil {
				log.Error("proxy.show.verions[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowEnginesStr:
			if qr, err = p.handleShowEngines(session, query, node); err != nil {
				log.Error("proxy.show.engines[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowTablesStr, sqlparser.ShowFullTablesStr:
			// Support for SHOW FULL TBALES which can be parsed used by Navicat
			// TODO: need to support: SHOW [FULL] TABLES [FROM db_name] [like_or_where]
			if qr, err = p.handleShowTables(session, query, node); err != nil {
				log.Error("proxy.show.tables[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowCreateTableStr:
			if qr, err = p.handleShowCreateTable(session, query, node); err != nil {
				log.Error("proxy.show.create.table[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowColumnsStr:
			if qr, err = p.handleShowColumns(session, query, node); err != nil {
				log.Error("proxy.show.colomns[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowProcesslistStr:
			if qr, err = p.handleShowProcesslist(session, query, node); err != nil {
				log.Error("proxy.show.processlist[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowQueryzStr:
			if qr, err = p.handleShowQueryz(session, query, node); err != nil {
				log.Error("proxy.show.queryz[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowTxnzStr:
			if qr, err = p.handleShowTxnz(session, query, node); err != nil {
				log.Error("proxy.show.txnz[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowCreateDatabaseStr:
			// Support for myloader.
			if qr, err = p.handleShowCreateDatabase(session, query, node); err != nil {
				log.Error("proxy.show.create.database[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowTableStatusStr:
			// Support for Navicat.
			if qr, err = p.handleShowTableStatus(session, query, node); err != nil {
				log.Error("proxy.show.table.status[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowWarningsStr, sqlparser.ShowVariablesStr:
			// Support for JDBC.
			if qr, err = p.handleJDBCShows(session, query, node); err != nil {
				log.Error("proxy.JDBC.shows[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		default:
			log.Error("proxy.show.unsupported[%s].from.session[%v]", query, session.ID())
			err = sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "unsupported.query:%v", query)
		}
		// spanner.auditLog(session, R, xbase.SHOW, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Insert:
		if qr, err = p.handleInsert(session, query, node); err != nil {
			log.Error("proxy.insert[%s].from.session[%v].error:%+v", truncateQuery(query, 256), session.ID(), err)
		}
		switch node.Action {
		case sqlparser.InsertStr:
			// spanner.auditLog(session, W, xbase.INSERT, query, qr)
		case sqlparser.ReplaceStr:
			// spanner.auditLog(session, W, xbase.REPLACE, query, qr)
		}
		return returnQuery(qr, callback, err)
	case *sqlparser.Delete:
		if qr, err = p.handleDelete(session, query, node); err != nil {
			log.Error("proxy.delete[%s].from.session[%v].error:%+v", query, session.ID(), err)
		}
		// spanner.auditLog(session, W, xbase.DELETE, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Update:
		if qr, err = p.handleUpdate(session, query, node); err != nil {
			log.Error("proxy.update[%s].from.session[%v].error:%+v", truncateQuery(query, 256), session.ID(), err)
		}
		// spanner.auditLog(session, W, xbase.UPDATE, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Select:
		// txSession := p.sessions.getTxnSession(session)
		switch node.From[0].(type) {
		case *sqlparser.AliasedTableExpr:
			aliasTableExpr := node.From[0].(*sqlparser.AliasedTableExpr)
			tb, ok := aliasTableExpr.Expr.(sqlparser.TableName)
			if !ok {
				// Subquery.
				if qr, err = p.handleSelect(session, query, node); err != nil {
					log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
				}
			} else {
				if tb.Name.String() == "dual" {
					// Select 1.
					if qr, err = p.ExecuteSingle(query); err != nil {
						log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
					}
					// } else if spanner.router.IsSystemDB(tb.Qualifier.String()) {
					// 	// System database select.
					// 	if qr, err = spanner.handleSelectSystem(session, query, node); err != nil {
					// 		log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
					// 	}
					// } else {
					// 	// Normal select.
					// 	if qr, err = spanner.handleSelect(session, query, node); err != nil {
					// 		log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
					// 	}
					// }
				}
				// spanner.auditLog(session, R, xbase.SELECT, query, qr)
				return returnQuery(qr, callback, err)
			}
		default: // ParenTableExpr, JoinTableExpr
			if qr, err = p.handleSelect(session, query, node); err != nil {
				log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
			// spanner.auditLog(session, R, xbase.SELECT, query, qr)
			return returnQuery(qr, callback, err)
		}
	case *sqlparser.Union:
		if qr, err = p.handleSelect(session, query, node); err != nil {
			log.Error("proxy.union[%s].from.session[%v].error:%+v", query, session.ID(), err)
		}
		// p.auditLog(session, W, xbase.UPDATE, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Kill:
		if qr, err = p.handleKill(session, query, node); err != nil {
			log.Error("proxy.kill[%s].from.session[%v].error:%+v", query, session.ID(), err)
		}
		// p.auditLog(session, R, xbase.KILL, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Explain:
		if qr, err = p.handleExplain(session, query, node); err != nil {
			log.Error("proxy.explain[%s].from.session[%v].error:%+v", query, session.ID(), err)
		}
		// p.auditLog(session, R, xbase.EXPLAIN, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Transaction:
		// Support for myloader.
		// Support Multiple-statement Transaction
		// if qr, err = p.handleMultiStmtTxn(session, query, node); err != nil {
		// 	log.Error("proxy.transaction[%s].from.session[%v].error:%+v", query, session.ID(), err)
		// }
		// p.auditLog(session, R, xbase.TRANSACTION, query, qr)
		// return returnQuery(qr, callback, err)
		err = sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "unsupported.query:%v", query)
		return err
	case *sqlparser.Set:
		log.Warning("proxy.query.set.query:%s", query)
		if qr, err = p.handleSet(session, query, node); err != nil {
			log.Error("proxy.set[%s].from.session[%v].error:%+v", query, session.ID(), err)
		}
		// p.auditLog(session, R, xbase.SET, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Checksum:
		// log.Warning("proxy.query.checksum.query:%s", query)
		// if qr, err = p.handleChecksumTable(session, query, node); err != nil {
		// 	log.Error("proxy.checksum[%s].from.session[%v].error:%+v", query, session.ID(), err)
		// }
		// // p.auditLog(session, R, xbase.CHECKSUM, query, qr)
		// return returnQuery(qr, callback, err)
		err = sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "unsupported.query:%v", query)
		return err
	default:
		log.Error("proxy.unsupported[%s].from.session[%v]", query, session.ID())
		// p.auditLog(session, R, xbase.UNSUPPORT, query, qr)
		err = sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "unsupported.query:%v", query)
		return err
	}
	return err
}

// IsDML returns the DML query or not.
func (p *Proxy) IsDML(node sqlparser.Statement) bool {
	switch node.(type) {
	case *sqlparser.Select, *sqlparser.Union, *sqlparser.Insert, *sqlparser.Delete, *sqlparser.Update:
		return true
	}
	return false
}

// IsDMLWrite returns the DML write or not.
func (p *Proxy) IsDMLWrite(node sqlparser.Statement) bool {
	switch node.(type) {
	case *sqlparser.Insert, *sqlparser.Delete, *sqlparser.Update:
		return true
	}
	return false
}

// IsDDL returns the DDL query or not.
func (p *Proxy) IsDDL(node sqlparser.Statement) bool {
	switch node.(type) {
	case *sqlparser.DDL:
		return true
	}
	return false
}

func queryStat(log *xlog.Log, node sqlparser.Statement, timeStart time.Time, slowQueryTime time.Duration, err error) {
	var command string
	switch node.(type) {
	case *sqlparser.Use:
		command = "Use"
	case *sqlparser.DDL:
		command = "DDL"
	case *sqlparser.Show:
		command = "Show"
	case *sqlparser.Insert:
		command = "Insert"
	case *sqlparser.Delete:
		command = "Delete"
	case *sqlparser.Update:
		command = "Update"
	case *sqlparser.Select:
		command = "Select"
	case *sqlparser.Union:
		command = "Union"
	case *sqlparser.Kill:
		command = "Kill"
	case *sqlparser.Explain:
		command = "Explain"
	case *sqlparser.Transaction:
		command = "Transaction"
	case *sqlparser.Set:
		command = "Set"
	default:
		command = "Unsupport"
	}
	queryTime := time.Since(timeStart)
	if err != nil {
		if queryTime > slowQueryTime {
			log.Info("SlowQueryTotalCounterInc %s", command)
			// monitor.SlowQueryTotalCounterInc(command, "Error")
		}
		log.Info("QueryTotalCounterInc %s", command)
		// monitor.QueryTotalCounterInc(command, "Error")
	} else {
		if queryTime > slowQueryTime {
			log.Info("SlowQueryTotalCounterInc %s", command)
			// monitor.SlowQueryTotalCounterInc(command, "OK")
		}
		log.Info("QueryTotalCounterInc %s", command)
		// monitor.QueryTotalCounterInc(command, "OK")
	}
}
