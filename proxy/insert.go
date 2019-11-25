package proxy

import (
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleInsert used to handle the insert command.
func (p *Proxy) handleInsert(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	database := session.Schema()
	// autoincPlug := spanner.plugins.PlugAutoIncrement()

	// AutoIncrement plugin process.
	// if err := autoincPlug.Process(database, node.(*sqlparser.Insert)); err != nil {
	// 	return nil, err
	// }
	return p.ExecuteDML(session, database, query, node)
}
