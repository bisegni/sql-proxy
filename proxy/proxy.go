package proxy

import (
	"sync"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Proxy tuple.
type Proxy struct {
	log           *xlog.Log
	mu            sync.RWMutex
	serverVersion string
}

// NewProxy creates a new spanner.
func NewProxy(log *xlog.Log, serverVersion string) *Proxy {
	return &Proxy{
		log:           log,
		serverVersion: serverVersion,
	}
}

// ServerVersion() string
// 	NewSession(session *Session)
// 	SessionInc(session *Session)
// 	SessionDec(session *Session)
// 	SessionClosed(session *Session)
// 	SessionCheck(session *Session) error
// 	AuthCheck(session *Session) error
// 	ComInitDB(session *Session, database string) error
// 	ComQuery(session *Session, query string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error

// Init used to init the async worker.
func (p *Proxy) Init() error {
	p.log.Info("NewProxy.Init...")
	return nil
}

// Close used to close NewProxy.
func (p *Proxy) Close() error {
	p.log.Info("Proxy.closed...")
	return nil
}

// NewSession impl.
func (p *Proxy) NewSession(s *driver.Session) {
	// spanner.sessions.Add(s)
}

// SessionClosed impl.
func (p *Proxy) SessionClosed(s *driver.Session) {
	// spanner.sessions.Remove(s)
}

// SessionInc increase client connection metrics, it need the user is assigned
func (p *Proxy) SessionInc(s *driver.Session) {
	// monitor.ClientConnectionInc(s.User())
}

// SessionDec decrease client connection metrics.
func (p *Proxy) SessionDec(s *driver.Session) {
	// monitor.ClientConnectionDec(s.User())
}

// ServerVersion impl -- returns server version of Radon when greeting.
func (p *Proxy) ServerVersion() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	// if package.serverVersion == "" {
	p.serverVersion = "test proxy"
	// }
	return p.serverVersion
}
