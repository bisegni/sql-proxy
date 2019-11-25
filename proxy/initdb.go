package proxy

import (
	"fmt"

	"github.com/xelabs/go-mysqlstack/driver"
)

// ComInitDB impl.
// Here, we will send a fake query 'SELECT 1' to the backend and check the 'USE DB'.
func (p *Proxy) ComInitDB(session *driver.Session, database string) error {
	// router := spanner.router

	// Check the database ACL.
	// if err := router.DatabaseACL(database); err != nil {
	// 	return err
	// }

	// privilegePlug := spanner.plugins.PlugPrivilege()
	fmt.Println("inidb", session.User())
	// isSet := privilegePlug.CheckUserPrivilegeIsSet(session.User())
	// if !isSet {
	// 	isSuper := privilegePlug.IsSuperPriv(session.User())
	// 	if !isSuper {
	// 		if isExist := privilegePlug.CheckDBinUserPrivilege(session.User(), database); !isExist {
	// 			error := sqldb.NewSQLErrorf(sqldb.ER_DBACCESS_DENIED_ERROR, "Access denied for user '%v'@'%%' to database '%v'",
	// 				session.User(), database)
	// 			return error
	// 		}
	// 	}
	// }

	query := fmt.Sprintf("use %s", database)
	if _, err := p.ExecuteSingle(query); err != nil {
		return err
	}
	session.SetSchema(database)
	return nil
}
