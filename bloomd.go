package bloomd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/krakend/krakend-jose/v2"
	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"

	"github.com/geetarista/go-bloomd/bloomd"
)

// namespace
const Namespace = "github_com/openrm/krakend-bloomd"
const prefix = "bloomd:"

// consts
const (
	delimiter       = "."
	keepAlivePeriod = 20 * time.Second

	// Bound on dialing, on one request/response round trip and on waiting for a free connection.
	defaultTimeout = 1 * time.Second
	// Connections to bloomd per gateway process. The go-bloomd connection is not safe for concurrent
	// use, so each request takes one for itself.
	defaultPoolSize = 4
)

// errors
var (
	errNoConfig      = errors.New("no config for bloomd")
	errInvalidConfig = errors.New("invalid config for bloomd")
	errNoFilterName  = errors.New("filter name is required")
	errFieldNotExist = errors.New("token missing required field")
	errInvalidField  = errors.New("token contains invalid field")
	errNoConnection  = errors.New("no free connection to bloomd")
	errBadResponse   = errors.New("unexpected response from bloomd")
)

// config map
type Config struct {
	Name      string   `json:"name"`
	Address   string   `json:"server_addr"`
	TokenKeys []string `json:"token_keys"`
	// PoolSize is the number of connections to bloomd (default 4).
	PoolSize int `json:"pool_size"`
}

func Register(scfg config.ServiceConfig, logger logging.Logger) (jose.Rejecter, error) {
	data, ok := scfg.ExtraConfig[Namespace]

	if !ok {
		logger.Debug(prefix, errNoConfig.Error())
		return nopRejecter{}, errNoConfig
	}

	raw, err := json.Marshal(data)

	if err != nil {
		logger.Debug(prefix, errInvalidConfig.Error())
		return nopRejecter{}, errInvalidConfig
	}

	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		logger.Debug(prefix, err.Error(), string(raw))
		return nopRejecter{}, errInvalidConfig
	}

	if cfg.Name == "" {
		return nopRejecter{}, errNoFilterName
	}

	size := cfg.PoolSize
	if size <= 0 {
		size = defaultPoolSize
	}

	r := newRejecter(cfg.Address, cfg.Name, cfg.TokenKeys, size, defaultTimeout, logger)

	// Connect one slot now so that a wrong address shows up in the log at start. Not fatal: the
	// connections are made again on demand, so a bloomd that starts after the gateway (or restarts)
	// is picked up without restarting the gateway.
	if err := r.warmUp(); err != nil {
		logger.Error(prefix, "error connecting to bloomd:", err)
	}

	return r, nil
}

// rejecter impl.
//
// A request takes a connection from the pool, uses it alone for one round trip and puts it back. A
// connection is never shared between goroutines: the go-bloomd connection has one socket and one
// buffered reader, so concurrent use mixes up replies, and a reconnect by one caller closes the
// socket under the others.
type rejecter struct {
	addr      string
	name      string
	tokenKeys []string
	timeout   time.Duration
	logger    logging.Logger
	pool      chan *slot
}

// slot is a pooled connection; filter is nil while disconnected.
type slot struct {
	filter *bloomd.Filter
}

func newRejecter(addr, name string, tokenKeys []string, size int, timeout time.Duration, logger logging.Logger) *rejecter {
	r := &rejecter{
		addr:      addr,
		name:      name,
		tokenKeys: tokenKeys,
		timeout:   timeout,
		logger:    logger,
		pool:      make(chan *slot, size),
	}
	for i := 0; i < size; i++ {
		r.pool <- &slot{}
	}
	return r
}

func (r *rejecter) warmUp() error {
	s, err := r.acquire()
	if err != nil {
		return err
	}
	defer r.release(s)

	if err := r.connect(s); err != nil {
		return err
	}
	info, err := r.roundTrip(s, func(f *bloomd.Filter) (interface{}, error) { return f.Info() })
	if err != nil {
		return err
	}
	r.logger.Info(prefix, "connected to bloomd:", info)
	return nil
}

func (r *rejecter) Reject(claims map[string]interface{}) bool {
	i, keys := 0, make([]string, len(r.tokenKeys))

	for _, k := range r.tokenKeys {

		v, ok := claims[k]

		if !ok {
			continue
		}

		switch v := v.(type) {
		case int:
			keys[i] = fmt.Sprintf("%s-%d", k, v)
		case int64:
			keys[i] = fmt.Sprintf("%s-%d", k, v)
		case string:
			keys[i] = k + "-" + v
		}

		i++

	}

	keys = keys[:i]

	// Nothing to look up; bloomd would answer an empty check with an error.
	if len(keys) == 0 {
		return false
	}

	responses, err := r.check(keys)

	if err != nil {
		// Fail open: an unreachable or misbehaving bloomd does not reject.
		r.logger.Error(prefix, err.Error())
		return false
	}

	for i, v := range responses {
		if v {
			r.logger.Info(prefix, "rejecting by key:", keys[i])
			return true
		}
	}

	return false
}

// check asks bloomd whether any of the keys is in the filter, one answer per key.
func (r *rejecter) check(keys []string) ([]bool, error) {
	s, err := r.acquire()
	if err != nil {
		return nil, err
	}
	defer r.release(s)

	if err := r.connect(s); err != nil {
		return nil, err
	}

	res, err := r.roundTrip(s, func(f *bloomd.Filter) (interface{}, error) { return f.Multi(keys) })
	if err != nil {
		return nil, err
	}

	responses := res.([]bool)
	// go-bloomd returns no error and no answers when bloomd replies with an error line (for example
	// when the filter does not exist); a shorter reply also means the stream is out of step.
	if len(responses) != len(keys) {
		s.drop()
		return nil, fmt.Errorf("%w: %d answers for %d keys", errBadResponse, len(responses), len(keys))
	}
	return responses, nil
}

// acquire takes a connection from the pool, waiting at most the timeout.
func (r *rejecter) acquire() (*slot, error) {
	select {
	case s := <-r.pool:
		return s, nil
	default:
	}

	t := time.NewTimer(r.timeout)
	defer t.Stop()

	select {
	case s := <-r.pool:
		return s, nil
	case <-t.C:
		return nil, errNoConnection
	}
}

func (r *rejecter) release(s *slot) {
	r.pool <- s
}

// connect dials bloomd for a disconnected slot.
func (r *rejecter) connect(s *slot) error {
	if s.filter != nil {
		return nil
	}

	raw, err := net.DialTimeout("tcp", r.addr, r.timeout)
	if err != nil {
		return err
	}
	sock := raw.(*net.TCPConn)

	if err := sock.SetKeepAlive(true); err != nil {
		_ = sock.Close()
		return err
	}
	if err := sock.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
		_ = sock.Close()
		return err
	}

	// The socket is handed to go-bloomd ready made, so that it never dials on its own (with no timeout).
	s.filter = &bloomd.Filter{
		Name: r.name,
		Conn: &bloomd.Connection{
			Server:   r.addr,
			Socket:   sock,
			Reader:   bufio.NewReader(sock),
			Attempts: 1,
		},
	}
	return nil
}

// roundTrip runs one command on the slot's connection under a deadline. After an error or a panic
// the connection is dropped: a late reply to a timed-out command would otherwise be read as the
// answer to the next one.
func (r *rejecter) roundTrip(s *slot, cmd func(*bloomd.Filter) (interface{}, error)) (res interface{}, err error) {
	defer func() {
		if p := recover(); p != nil {
			s.drop()
			res, err = nil, fmt.Errorf("%w: %v", errBadResponse, p)
		}
	}()

	deadline := time.Now().Add(r.timeout)
	if err := s.filter.Conn.Socket.SetDeadline(deadline); err != nil {
		s.drop()
		return nil, err
	}

	res, err = cmd(s.filter)
	if err != nil {
		s.drop()
		return nil, err
	}
	return res, nil
}

// drop closes the connection; the next use dials again.
func (s *slot) drop() {
	if s.filter != nil && s.filter.Conn != nil && s.filter.Conn.Socket != nil {
		_ = s.filter.Conn.Socket.Close()
	}
	s.filter = nil
}

type nopRejecter struct{}

func (nr nopRejecter) Reject(map[string]interface{}) bool { return false }
