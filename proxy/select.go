package proxy

import (
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleSelect used to handle the select command.
func (p *Proxy) handleSelect(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	database := session.Schema()
	return p.ExecuteDML(session, database, query, node)
}

func (p *Proxy) handleSelectStream(session *driver.Session, query string, node sqlparser.Statement, callback func(qr *sqltypes.Result) error) error {
	database := session.Schema()
	return p.ExecuteStreamFetch(session, database, query, node, callback)
}
