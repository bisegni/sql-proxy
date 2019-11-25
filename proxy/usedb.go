package proxy

import (
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleUseDB used to handle the UseDB command.
// Here, we will send a fake query 'SELECT 1' to the backend and check the 'USE DB'.
func (p *Proxy) handleUseDB(session *driver.Session, query string, node *sqlparser.Use) (*sqltypes.Result, error) {
	usedb := node
	db := usedb.DBName.String()
	// router := p.router
	// // Check the database ACL.
	// if err := router.DatabaseACL(db); err != nil {
	// 	return nil, err
	// }

	if _, err := p.ExecuteSingle(query); err != nil {
		return nil, err
	}
	session.SetSchema(db)
	return &sqltypes.Result{}, nil
}