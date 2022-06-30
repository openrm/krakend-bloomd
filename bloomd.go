package bloomd

import (
	"fmt"
	"time"
	"errors"
	"encoding/json"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/krakendio/krakend-jose/v2"

	"github.com/geetarista/go-bloomd/bloomd"
)

// namespace
const Namespace = "github_com/openrm/krakend-bloomd"
const prefix = "bloomd:"

// consts
const (
	delimiter = "."
	keepAlivePeriod = 20 * time.Second
)

// errors
var (
	errNoConfig = errors.New("no config for bloomd")
	errInvalidConfig = errors.New("invalid config for bloomd")
	errNoFilterName = errors.New("filter name is required")
	errFieldNotExist = errors.New("token missing required field")
	errInvalidField = errors.New("token contains invalid field")
)

// config map
type Config struct {
	Name string `json:"name"`
	Address string `json:"server_addr"`
	TokenKeys []string `json:"token_keys"`
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

	filter, err := createFilter(cfg.Address, cfg.Name, logger)
	if err != nil {
		logger.Error(prefix, "error connecting to bloomd:", err)
		return nopRejecter{}, errInvalidConfig
	}

	return rejecter{
		filter: filter,
		logger: logger,
		tokenKeys: cfg.TokenKeys,
	}, nil
}

// rejecter impl.
type rejecter struct {
	filter *bloomd.Filter
	logger logging.Logger
	tokenKeys []string
}

func (r rejecter) handlePanic() {
	if err := recover(); err != nil {
		if err, ok := err.(error); ok {
			r.logger.Error(prefix, err.Error())
		}
		if err, ok := err.(string); ok {
			r.logger.Error(prefix, err)
		}
	}
}

func (r rejecter) Reject(claims map[string]interface{}) bool {
	defer r.handlePanic()

	if r.filter == nil || r.filter.Conn == nil {
		return false
	}

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

	// XXX needs testing
	if r.filter.Conn.Socket != nil {
		t := time.Now().Add(1 * time.Second)
		r.filter.Conn.Socket.SetReadDeadline(t)
		r.filter.Conn.Socket.SetWriteDeadline(t)
	}

	responses, err := r.filter.Multi(keys[:i])

	if err != nil {
		r.logger.Error(prefix, err.Error())
		_ = setupConn(r.filter, r.logger)
	}

	for i, v := range responses {
		if v {
			r.logger.Info(prefix, "rejecting by key:", keys[i])
			return true
		}
	}

	return false
}

type nopRejecter struct {}
func (nr nopRejecter) Reject(map[string]interface{}) bool { return false }

func setupConn(filter *bloomd.Filter, logger logging.Logger) error {
	if filter.Conn.Socket != nil {
		_ = filter.Conn.Socket.Close()
		filter.Conn.Socket = nil
	}

	info, err := filter.Info()

	if err != nil {
		return err
	}

	logger.Info(prefix, "connected to bloomd:", info)

	// XXX needs testing
	if err := filter.Conn.Socket.SetKeepAlive(true); err != nil {
		return err
	}

	// XXX needs testing
	if err := filter.Conn.Socket.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
		return err
	}

	return nil
}

func createFilter(addr string, filterName string, logger logging.Logger) (*bloomd.Filter, error) {
	client := bloomd.NewClient(addr)
	filter := client.GetFilter(filterName)
	return filter, setupConn(filter, logger)
}
