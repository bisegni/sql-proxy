package proxy

import (
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleExplain used to handle the EXPLAIN command.
func (p *Proxy) handleExplain(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := p.log
	log.Printf("proxy.handleExplain")
	// database := session.Schema()
	// router := spanner.router
	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "EXPLAIN", Type: querypb.Type_VARCHAR},
	}
	return nil, sqldb.NewSQLError(sqldb.ER_SYNTAX_ERROR, "explain not supported")
	// pat := `(?i)explain`
	// reg := regexp.MustCompile(pat)
	// idx := reg.FindStringIndex(query)
	// if len(idx) != 2 {
	// 	return nil, errors.Errorf("explain.query[%s].syntax.error", query)
	// }
	// cutQuery := query[idx[1]:]
	// subNode, err := sqlparser.Parse(cutQuery)
	// if err != nil {
	// 	msg := fmt.Sprintf("query[%s].parser.error: %v", cutQuery, err)
	// 	row := []sqltypes.Value{
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(msg)),
	// 	}
	// 	qr.Rows = append(qr.Rows, row)
	// 	return qr, nil
	// }

	// // privilegePlug := spanner.plugins.PlugPrivilege()
	// // if err := privilegePlug.Check(database, session.User(), subNode); err != nil {
	// // 	return nil, err
	// // }

	// // Explain only supports DML.
	// // see https://dev.mysql.com/doc/refman/5.7/en/explain.html
	// switch subNode.(type) {
	// case *sqlparser.Union:
	// case *sqlparser.Select:
	// case *sqlparser.Delete:
	// case *sqlparser.Insert:
	// 	// autoincPlug := spanner.plugins.PlugAutoIncrement()
	// 	// if err := autoincPlug.Process(database, subNode.(*sqlparser.Insert)); err != nil {
	// 	// 	return nil, err
	// 	// }
	// case *sqlparser.Update:
	// case *sqlparser.Checksum:
	// default:
	// 	return nil, sqldb.NewSQLError(sqldb.ER_SYNTAX_ERROR, "explain only supports SELECT/DELETE/INSERT/UNION")
	// }

	// simOptimizer := optimizer.NewSimpleOptimizer(log, database, cutQuery, subNode, router)
	// planTree, err := simOptimizer.BuildPlanTree()
	// if err != nil {
	// 	log.Error("proxy.explain.error:%+v", err)
	// 	msg := fmt.Sprintf("unsupported: cannot.explain.the.query:%s", cutQuery)
	// 	row := []sqltypes.Value{
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(msg)),
	// 	}
	// 	qr.Rows = append(qr.Rows, row)
	// 	return qr, nil
	// }

	// if len(planTree.Plans()) > 0 {
	// 	msg := planTree.Plans()[0].JSON()
	// 	row := []sqltypes.Value{
	// 		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(msg)),
	// 	}
	// 	qr.Rows = append(qr.Rows, row)
	// 	return qr, nil
	// }
	return qr, nil
}
