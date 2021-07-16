package bloomd

import (
	"fmt"
	"time"
	"errors"
	"strings"
	"encoding/json"
	"crypto/sha256"

	"github.com/luraproject/lura/config"
	"github.com/luraproject/lura/logging"
	"github.com/devopsfaith/krakend-jose"
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

func (r rejecter) convTokens(claims map[string]interface{}) []string {
	tokens := make([]string, len(r.tokenKeys))

	for i, k := range r.tokenKeys {

		v, ok := claims[k]

		if !ok {
			// XXX
			// return tokens, errFieldNotExist
		}

		switch v := v.(type) {
		case int:
			tokens[i] = fmt.Sprintf("%d", v)
		case int64:
			tokens[i] = fmt.Sprintf("%d", v)
		case float64:
			tokens[i] = fmt.Sprintf("%d", int(v))
		case string:
			tokens[i] = v
		}

	}

	return tokens
}

func (r rejecter) calcHash(tokens []string) string {
	id := strings.Join(tokens, delimiter)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(id)))
}

func (r rejecter) recover() {
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
	defer r.recover()

	if r.filter == nil || r.filter.Conn == nil {
		return false
	}

	tokens := r.convTokens(claims)

	hashes := make([]string, len(r.tokenKeys) + 1)

	for i, key := range r.tokenKeys {
		hashes[i] = r.calcHash([]string{key, tokens[i]})
	}

	hashes[len(r.tokenKeys)] = r.calcHash(tokens)

	// XXX needs testing
	if r.filter.Conn.Socket != nil {
		t := time.Now().Add(1 * time.Second)
		r.filter.Conn.Socket.SetReadDeadline(t)
		r.filter.Conn.Socket.SetWriteDeadline(t)
	}

	matches, err := r.filter.Multi(hashes)

	if err != nil {
		r.logger.Error(prefix, err.Error())
		_ = setupConn(r.filter, r.logger)
	}

	for _, v := range matches {
		if v {
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

	logger.Info("connected to bloomd:", info)

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
