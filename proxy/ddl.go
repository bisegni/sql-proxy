/*
* Radon
*
* Copyright 2018 The Radon Authors.
* Code is licensed under the GPLv3.
*
 */

package proxy

import (
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleDDL used to handle the DDL command.
// Here we need to deal with database.table grammar.
// Supports:
// 1. CREATE/DROP DATABASE
// 2. CREATE/DROP TABLE ... PARTITION BY HASH(shardkey)
// 3. CREATE/DROP INDEX ON TABLE(columns...)
// 4. ALTER TABLE .. ENGINE=xx
// 5. ALTER TABLE .. ADD COLUMN (column definition)
// 6. ALTER TABLE .. MODIFY COLUMN column definition
// 7. ALTER TABLE .. DROP COLUMN column
func (p *Proxy) handleDDL(session *driver.Session, query string, node *sqlparser.DDL) (*sqltypes.Result, error) {
	log := p.log
	var qr *sqltypes.Result
	// route := spanner.router
	// scatter := spanner.scatter

	ddl := node
	database := session.Schema()
	// Database operation.
	if !ddl.Database.IsEmpty() {
		database = ddl.Database.String()
	}

	// Table operation.
	// when Drop Table, maybe multiple tables, can't use the ddl.Table.Qualifier.
	if ddl.Action != sqlparser.DropTableStr && !ddl.Table.Qualifier.IsEmpty() {
		database = ddl.Table.Qualifier.String()
	}

	// var databases []string
	// if ddl.Action == sqlparser.DropTableStr {
	// 	for _, tableIdent := range ddl.Tables {
	// 		if !tableIdent.Qualifier.IsEmpty() {
	// 			databases = append(databases, ddl.Table.Qualifier.String())
	// 		}
	// 	}
	// }
	// databases = append(databases, database)

	// for _, db := range databases {
	// 	// Check the database ACL.
	// 	if err := route.DatabaseACL(db); err != nil {
	// 		return nil, err
	// 	}
	// 	// Check the database privilege.
	// 	privilegePlug := spanner.plugins.PlugPrivilege()
	// 	if err := privilegePlug.Check(db, session.User(), node); err != nil {
	// 		return nil, err
	// 	}
	// }

	switch ddl.Action {
	case sqlparser.CreateDBStr:
		// if node.IfNotExists && checkDatabaseExists(database, route) {
		// 	return &sqltypes.Result{}, nil
		// }
		// if err := route.CreateDatabase(database); err != nil {
		// 	return nil, err
		// }
		// return p.ExecuteScatter(query)
		break
	case sqlparser.DropDBStr:
		// if node.IfExists && !checkDatabaseExists(database, route) {
		// 	return &sqltypes.Result{}, nil
		// }
		// // Execute the ddl.
		// qr, err := p.ExecuteScatter(query)
		// if err != nil {
		// 	return nil, err
		// }
		// // Drop database from router.
		// if err := route.DropDatabase(database); err != nil {
		// 	return nil, err
		// }
		return qr, nil
	case sqlparser.CreateTableStr:
		var err error
		// table := ddl.Table.Name.String()
		// backends := scatter.Backends()
		// shardKey := ddl.PartitionName
		// tableType := router.TableTypeUnknown

		// if !checkDatabaseExists(database, route) {
		// 	return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
		// }

		// // Check table exists.
		// if node.IfNotExists && checkTableExists(database, table, route) {
		// 	return &sqltypes.Result{}, nil
		// }

		// // Check engine.
		// checkEngine(ddl)

		// switch ddl.TableSpec.Options.Type {
		// case sqlparser.PartitionTableHash, sqlparser.NormalTableType:
		// 	if shardKey, err = tryGetShardKey(ddl); err != nil {
		// 		return nil, err
		// 	}
		// 	tableType = router.TableTypePartitionHash
		// case sqlparser.PartitionTableList:
		// 	if shardKey, err = tryGetShardKey(ddl); err != nil {
		// 		return nil, err
		// 	}
		// 	tableType = router.TableTypePartitionList
		// case sqlparser.GlobalTableType:
		// 	tableType = router.TableTypeGlobal
		// case sqlparser.SingleTableType:
		// 	tableType = router.TableTypeSingle
		// }

		// autoinc, err := autoincrement.GetAutoIncrement(node)
		// if err != nil {
		// 	return nil, err
		// }
		// extra := &router.Extra{
		// 	AutoIncrement: autoinc,
		// }

		// switch tableType {
		// case router.TableTypeSingle:
		// 	if ddl.BackendName != "" {
		// 		// TODO(andy): distributed by a list of backends
		// 		if isExist := scatter.CheckBackend(ddl.BackendName); !isExist {
		// 			log.Error("spanner.ddl.execute[%v].backend.doesn't.exist", query)
		// 			return nil, fmt.Errorf("create table distributed by backend '%s' doesn't exist", ddl.BackendName)
		// 		}

		// 		assignedBackends := []string{ddl.BackendName}
		// 		if err := route.CreateTable(database, table, shardKey, tableType, assignedBackends, extra); err != nil {
		// 			return nil, err
		// 		}
		// 	} else {
		// 		if err := route.CreateTable(database, table, shardKey, tableType, backends, extra); err != nil {
		// 			return nil, err
		// 		}
		// 	}
		// case router.TableTypePartitionList:
		// 	if err := route.CreateListTable(database, table, shardKey, tableType, ddl.PartitionOptions, extra); err != nil {
		// 		return nil, err
		// 	}

		// default:
		// 	if err := route.CreateTable(database, table, shardKey, tableType, backends, extra); err != nil {
		// 		return nil, err
		// 	}
		// }

		r, err := p.ExecuteDDL(session, database, sqlparser.String(ddl), node)
		// if err != nil {
		// 	// Try to drop table.
		// 	route.DropTable(database, table)
		// 	return nil, err
		// }
		return r, err
	case sqlparser.DropTableStr:
		r := &sqltypes.Result{}
		// tables := ddl.Tables
		// for _, tableIdent := range tables {
		// 	node.Table = tableIdent
		// 	table := tableIdent.Name.String()
		// 	db := database
		// 	// The query need differentiate, ddl_plan will define database.
		// 	var query string
		// 	if tableIdent.Qualifier.IsEmpty() {
		// 		query = fmt.Sprintf("drop table %s", table)
		// 	} else {
		// 		//If the tableIdent with Qualifier, us it as db, or else use the default.
		// 		db = tableIdent.Qualifier.String()
		// 		query = fmt.Sprintf("drop table %s.%s", db, table)
		// 	}

		// 	// Check the database and table is exists.
		// 	if !checkDatabaseExists(db, route) {
		// 		return nil, sqldb.NewSQLError(sqldb.ER_BAD_DB_ERROR, db)
		// 	}

		// 	// Check table exists.
		// 	if node.IfExists && !checkTableExists(db, table, route) {
		// 		return &sqltypes.Result{}, nil
		// 	}

		// 	// Execute.
		// 	r, err := spanner.ExecuteDDL(session, db, query, node)
		// 	if err != nil {
		// 		log.Error("spanner.ddl.execute[%v].error[%+v]", query, err)
		// 	}
		// 	if err := route.DropTable(db, table); err != nil {
		// 		log.Error("spanner.ddl.router.drop.table[%s].error[%+v]", table, err)
		// 	}

		// 	if err != nil {
		// 		return r, err
		// 	}
		// }
		return r, nil
	case sqlparser.CreateIndexStr, sqlparser.DropIndexStr,
		sqlparser.AlterEngineStr, sqlparser.AlterCharsetStr,
		sqlparser.AlterAddColumnStr, sqlparser.AlterDropColumnStr, sqlparser.AlterModifyColumnStr,
		sqlparser.TruncateTableStr:

		// // Check the database and table is exists.
		// if !checkDatabaseExists(database, route) {
		// 	return nil, sqldb.NewSQLError(sqldb.ER_BAD_DB_ERROR, database)
		// }

		// table := ddl.Table.Name.String()
		// if !checkTableExists(database, table, route) {
		// 	return nil, sqldb.NewSQLError(sqldb.ER_NO_SUCH_TABLE, table)
		// }
		// Execute.
		r, err := p.ExecuteDDL(session, database, query, node)
		if err != nil {
			log.Error("spanner.ddl[%v].error[%+v]", query, err)
		}
		return r, err
	case sqlparser.RenameStr:
		// TODO: support a list of TableName.
		// TODO: support databases are not equal.
		r := &sqltypes.Result{}
		// fromTable := ddl.Table.Name.String()
		// toTable := ddl.NewName.Name.String()

		// Check the database, fromTable is exists, toTable is not exists.
		// if !checkDatabaseExists(database, route) {
		// 	return nil, sqldb.NewSQLError(sqldb.ER_BAD_DB_ERROR, database)
		// }

		// if !checkTableExists(database, fromTable, route) {
		// 	return nil, sqldb.NewSQLError(sqldb.ER_NO_SUCH_TABLE, fromTable)
		// }

		// if checkTableExists(database, toTable, route) {
		// 	return nil, sqldb.NewSQLError(sqldb.ER_TABLE_EXISTS_ERROR, toTable)
		// }

		// Execute.
		r, err := p.ExecuteDDL(session, database, query, node)
		if err != nil {
			log.Error("spanner.ddl.execute[%v].error[%+v]", query, err)
			return r, err
		}

		// err = route.RenameTable(database, fromTable, toTable)
		// if err != nil {
		// 	log.Error("spanner.ddl.router.rename.fromtable[%s].totable[%s].error[%+v]", fromTable, toTable, err)
		// 	return r, err
		// }
		return r, nil
	default:
		log.Error("spanner.unsupported[%s].from.session[%v]", query, session.ID())
		return nil, sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "unsupported.query:%v", query)
	}

	return qr, nil
}
