package proxy

import (
	// "github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// ExecuteMultiStmtsInTxn used to execute multiple statements in the transaction.
func (p *Proxy) ExecuteMultiStmtsInTxn(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := p.log
	log.Info("proxy.ExecuteMultiStmtsInTxn")
	var qr *sqltypes.Result
	//  router := spanner.router
	//  sessions := spanner.sessions
	//  txSession := sessions.getTxnSession(session)

	//  sessions.MultiStmtTxnBinding(session, nil, node, query)

	//  plans, err := optimizer.NewSimpleOptimizer(log, database, query, node, router).BuildPlanTree()
	//  if err != nil {
	// 	 return nil, err
	//  }
	//  executors := executor.NewTree(log, plans, txSession.transaction)
	//  qr, err := executors.Execute()
	//  if err != nil {
	// 	 // need the user to rollback
	// 	 return nil, err
	//  }

	//  sessions.MultiStmtTxnUnBinding(session, false)
	return qr, nil
}

// ExecuteSingleStmtTxnTwoPC used to execute single statement transaction with 2pc commit.
func (p *Proxy) ExecuteSingleStmtTxnTwoPC(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	// log := spanner.log
	// conf := spanner.conf
	// router := spanner.router
	// scatter := spanner.scatter
	// sessions := spanner.sessions

	// // transaction.
	// txn, err := scatter.CreateTransaction()
	// if err != nil {
	// 	log.Error("spanner.txn.create.error:[%v]", err)
	// 	return nil, err
	// }
	// defer txn.Finish()

	// // txn limits.
	// txn.SetTimeout(conf.Proxy.QueryTimeout)
	// txn.SetMaxResult(conf.Proxy.MaxResultSize)
	// txn.SetMaxJoinRows(conf.Proxy.MaxJoinRows)

	// // binding.
	// sessions.TxnBinding(session, txn, node, query)
	// defer sessions.TxnUnBinding(session)

	// // Transaction begin.
	// if err := txn.Begin(); err != nil {
	// 	log.Error("spanner.execute.2pc.txn.begin.error:[%v]", err)
	// 	return nil, err
	// }

	// // Transaction execute.
	// plans, err := optimizer.NewSimpleOptimizer(log, database, query, node, router).BuildPlanTree()
	// if err != nil {
	// 	return nil, err
	// }

	// executors := executor.NewTree(log, plans, txn)
	// qr, err := executors.Execute()
	// if err != nil {
	// 	if x := txn.Rollback(); x != nil {
	// 		log.Error("spanner.execute.2pc.error.to.rollback.still.error:[%v]", x)
	// 	}
	// 	return nil, err
	// }

	// // Transaction commit.
	// if err := txn.Commit(); err != nil {
	// 	log.Error("spanner.execute.2pc.txn.commit.error:[%v]", err)
	// 	return nil, err
	// }
	return qr, nil
}

// ExecuteNormal used to execute non-2pc querys to shards with QueryTimeout limits.
func (p *Proxy) ExecuteNormal(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	timeout := 10000 //spanner.conf.Proxy.QueryTimeout
	return p.executeWithTimeout(session, database, query, node, timeout)
}

// ExecuteDDL used to execute ddl querys to the shards with DDLTimeout limits, used for create/drop index long time operation.
func (p *Proxy) ExecuteDDL(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	p.log.Info("spanner.execute.ddl.query:%s", query)
	timeout := 10000 //spanner.conf.Proxy.DDLTimeout

	// txSession := spanner.sessions.getTxnSession(session)
	// if spanner.isTwoPC() && txSession.transaction != nil {
	// 	return nil, errors.Errorf("in.multiStmtTrans.unsupported.DDL:%v.", query)
	// }

	return p.executeWithTimeout(session, database, query, node, timeout)
}

// ExecuteNormal used to execute non-2pc querys to shards with timeout limits.
// timeout:
//    0x01. if timeout <= 0, no limits.
//    0x02. if timeout > 0, the query will be interrupted if the timeout(in millisecond) is exceeded.
func (p *Proxy) executeWithTimeout(session *driver.Session, database string, query string, node sqlparser.Statement, timeout int) (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	log := p.log
	log.Info("proxy.executeWithTimeout")
	// conf := spanner.conf
	// router := spanner.router
	// scatter := spanner.scatter
	// sessions := spanner.sessions

	// transaction.
	// txn, err := scatter.CreateTransaction()
	// if err != nil {
	// 	log.Error("spanner.txn.create.error:[%v]", err)
	// 	return nil, err
	// }
	// defer txn.Finish()

	// // txn limits.
	// txn.SetTimeout(timeout)
	// txn.SetMaxResult(conf.Proxy.MaxResultSize)
	// txn.SetMaxJoinRows(conf.Proxy.MaxJoinRows)

	// // binding.
	// sessions.TxnBinding(session, txn, node, query)
	// defer sessions.TxnUnBinding(session)

	// plans, err := optimizer.NewSimpleOptimizer(log, database, query, node, router).BuildPlanTree()
	// if err != nil {
	// 	return nil, err
	// }
	// executors := executor.NewTree(log, plans, txn)
	// qr, err := executors.Execute()
	// if err != nil {
	// 	return nil, err
	// }
	return qr, nil
}

// ExecuteStreamFetch used to execute a stream fetch query.
func (spanner *Proxy) ExecuteStreamFetch(session *driver.Session, database string, query string, node sqlparser.Statement, callback func(qr *sqltypes.Result) error) error {
	// log := spanner.log
	// router := spanner.router
	// scatter := spanner.scatter
	// sessions := spanner.sessions

	// // transaction.
	// txn, err := scatter.CreateTransaction()
	// if err != nil {
	// 	log.Error("spanner.txn.create.error:[%v]", err)
	// 	return err
	// }
	// defer txn.Finish()

	// // binding.
	// sessions.TxnBinding(session, txn, node, query)
	// defer sessions.TxnUnBinding(session)

	// selectNode, ok := node.(*sqlparser.Select)
	// if !ok {
	// 	return errors.New("ExecuteStreamFetch.only.support.select")
	// }

	// plan := planner.NewSelectPlan(log, database, query, selectNode, router)
	// if err := plan.Build(); err != nil {
	// 	return err
	// }
	// m, ok := plan.Root.(*builder.MergeNode)
	// if !ok {
	// 	return errors.New("ExecuteStreamFetch.unsupport.cross-shard.join")
	// }
	// reqCtx := xcontext.NewRequestContext()
	// reqCtx.Mode = m.ReqMode
	// reqCtx.Querys = m.GetQuery()
	// reqCtx.RawQuery = plan.RawQuery
	// streamBufferSize := spanner.conf.Proxy.StreamBufferSize
	// return txn.ExecuteStreamFetch(reqCtx, callback, streamBufferSize)
	return sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "unsupported.query:%v", query)
}

// ExecuteDML used to execute some DML querys to shards.
func (p *Proxy) ExecuteDML(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	// privilegePlug := spanner.plugins.PlugPrivilege()
	// if err := privilegePlug.Check(session.Schema(), session.User(), node); err != nil {
	// 	return nil, err
	// }

	// if spanner.isTwoPC() {
	// 	txSession := spanner.sessions.getTxnSession(session)
	// 	if spanner.IsDML(node) {
	// 		if txSession.transaction == nil {
	// 			return spanner.ExecuteSingleStmtTxnTwoPC(session, database, query, node)
	// 		} else {
	// 			return spanner.ExecuteMultiStmtsInTxn(session, database, query, node)
	// 		}
	// 	}
	// 	return spanner.ExecuteNormal(session, database, query, node)
	// }
	return p.ExecuteNormal(session, database, query, node)
}

// ExecuteSingle used to execute query on one shard without planner.
// The query must contain the database, such as db.table.
func (p *Proxy) ExecuteSingle(query string) (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	log := p.log
	log.Info("proxy.ExecuteSingle")
	// scatter := spanner.scatter
	// txn, err := scatter.CreateTransaction()
	// if err != nil {
	// 	log.Error("spanner.execute.single.txn.create.error:[%v]", err)
	// 	return nil, err
	// }
	// defer txn.Finish()
	// return txn.ExecuteSingle(query)
	return qr, nil
}

// ExecuteScatter used to execute query on all shards without planner.
func (p *Proxy) ExecuteScatter(query string) (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	log := p.log
	log.Info("proxy.ExecuteScatter")
	// scatter := spanner.scatter
	// txn, err := scatter.CreateTransaction()
	// if err != nil {
	// 	log.Error("spanner.execute.scatter.txn.create.error:[%v]", err)
	// 	return nil, err
	// }
	// defer txn.Finish()
	// return txn.ExecuteScatter(query)
	return qr, nil
}

// ExecuteOnThisBackend used to executye query on the backend whitout planner.
func (p *Proxy) ExecuteOnThisBackend(backend string, query string) (*sqltypes.Result, error) {
	log := p.log
	log.Info("proxy.ExecuteOnThisBackend")
	var qr *sqltypes.Result
	// scatter := spanner.scatter
	// txn, err := scatter.CreateTransaction()
	// if err != nil {
	// 	log.Error("spanner.execute.on.this.backend..txn.create.error:[%v]", err)
	// 	return nil, err
	// }
	// defer txn.Finish()
	// return txn.ExecuteOnThisBackend(backend, query)
	return qr, nil
}
