package bloomd

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
)

// fakeBloomd speaks the part of the bloomd text protocol the plugin uses ("info" and "m"), one reply
// per command in order, like the real server.
type fakeBloomd struct {
	t        *testing.T
	ln       net.Listener
	revoked  map[string]bool
	latency  time.Duration // added to every "m" reply
	missing  atomic.Bool   // answer "m" like a server without the filter
	stallFor time.Duration // how long the next stallNext "m" replies are held back
	stallNxt atomic.Int32

	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	open      int
	maxOpen   int
	checks    atomic.Int64
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func newFakeBloomd(t *testing.T, addr string, revoked ...string) *fakeBloomd {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeBloomd{t: t, ln: ln, revoked: map[string]bool{}, conns: map[net.Conn]struct{}{}}
	for _, k := range revoked {
		s.revoked[k] = true
	}
	s.wg.Add(1)
	go s.accept()
	t.Cleanup(s.close)
	return s
}

func (s *fakeBloomd) addr() string { return s.ln.Addr().String() }

func (s *fakeBloomd) accept() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.open++
		if s.open > s.maxOpen {
			s.maxOpen = s.open
		}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serve(c)
	}
}

func (s *fakeBloomd) serve(c net.Conn) {
	defer s.wg.Done()
	defer func() {
		_ = c.Close()
		s.mu.Lock()
		delete(s.conns, c)
		s.open--
		s.mu.Unlock()
	}()

	r := bufio.NewReader(c)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		f := strings.Fields(line)
		if len(f) == 0 {
			return
		}
		switch f[0] {
		case "info":
			_, _ = c.Write([]byte("START\ncapacity 100000\nsize 1\nEND\n"))
		case "m":
			s.checks.Add(1)
			if s.stallNxt.Add(-1) >= 0 {
				time.Sleep(s.stallFor)
			}
			time.Sleep(s.latency)
			if s.missing.Load() {
				_, _ = c.Write([]byte("Filter does not exist\n"))
				continue
			}
			answers := make([]string, 0, len(f)-2)
			for _, k := range f[2:] {
				if s.revoked[k] {
					answers = append(answers, "Yes")
				} else {
					answers = append(answers, "No")
				}
			}
			_, _ = c.Write([]byte(strings.Join(answers, " ") + "\n"))
		default:
			_, _ = c.Write([]byte("Client Error: Command not recognized\n"))
		}
	}
}

// dropConns closes every client connection, as a restarting bloomd does.
func (s *fakeBloomd) dropConns() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		_ = c.Close()
	}
}

func (s *fakeBloomd) close() {
	s.closeOnce.Do(func() {
		_ = s.ln.Close()
		s.dropConns()
		s.wg.Wait()
	})
}

func (s *fakeBloomd) maxConnections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxOpen
}

// testLogger counts what the plugin logs.
type testLogger struct {
	mu     sync.Mutex
	errors []string
	infos  []string
}

func (l *testLogger) Debug(v ...interface{})    {}
func (l *testLogger) Warning(v ...interface{})  {}
func (l *testLogger) Critical(v ...interface{}) {}
func (l *testLogger) Fatal(v ...interface{})    {}
func (l *testLogger) Info(v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, fmt.Sprint(v...))
}
func (l *testLogger) Error(v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, fmt.Sprint(v...))
}
func (l *testLogger) errorCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.errors)
}

func claims(jti string) map[string]interface{} {
	return map[string]interface{}{"jti": jti}
}

func newTestRejecter(addr string, size int, timeout time.Duration, l *testLogger) *rejecter {
	return newRejecter(addr, "token_rvk", []string{"jti"}, size, timeout, l)
}

// The failure seen in production: many requests at once on the one connection of a gateway. Every
// answer must be the one for its own token, with no error logged, and the connections stay within
// the pool. Run with -race.
func TestRejectConcurrent(t *testing.T) {
	srv := newFakeBloomd(t, "127.0.0.1:0", "jti-revoked")
	srv.latency = 200 * time.Microsecond
	l := &testLogger{}
	r := newTestRejecter(srv.addr(), 4, time.Second, l)

	const workers, rounds = 64, 100
	var wg sync.WaitGroup
	var wrong atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				revoked := (w+i)%3 == 0
				id := fmt.Sprintf("ok-%d-%d", w, i)
				if revoked {
					id = "revoked"
				}
				if got := r.Reject(claims(id)); got != revoked {
					wrong.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	if n := wrong.Load(); n != 0 {
		t.Errorf("%d of %d requests got the wrong answer", n, workers*rounds)
	}
	if n := l.errorCount(); n != 0 {
		t.Errorf("%d errors logged, first: %q", n, l.errors[0])
	}
	if n := srv.maxConnections(); n > 4 {
		t.Errorf("%d connections open at once, the pool is 4", n)
	}
}

// A reply that arrives after the deadline must not be taken for the answer to the next request on
// that connection: the request fails open and the connection is replaced.
func TestStalledReplyFailsOpenAndRecovers(t *testing.T) {
	srv := newFakeBloomd(t, "127.0.0.1:0", "jti-revoked")
	srv.stallFor = 300 * time.Millisecond
	srv.stallNxt.Store(1)
	l := &testLogger{}
	r := newTestRejecter(srv.addr(), 1, 100*time.Millisecond, l)

	start := time.Now()
	if r.Reject(claims("revoked")) {
		t.Fatal("a stalled bloomd must not reject (fail open)")
	}
	if d := time.Since(start); d > 250*time.Millisecond {
		t.Errorf("the stalled request took %v, the timeout is 100ms", d)
	}
	if l.errorCount() != 1 {
		t.Errorf("want 1 error logged, got %v", l.errors)
	}

	// Past the stall, so the late reply to the first request is sitting on the old connection.
	time.Sleep(400 * time.Millisecond)
	if !r.Reject(claims("revoked")) {
		t.Error("revoked token passed after the stall")
	}
	if r.Reject(claims("fine")) {
		t.Error("valid token rejected after the stall")
	}
}

// With every connection stalled the requests give up on their own timeouts, they do not queue up
// one behind the other.
func TestStalledServerDoesNotQueueRequests(t *testing.T) {
	srv := newFakeBloomd(t, "127.0.0.1:0", "jti-revoked")
	srv.stallFor = 2 * time.Second
	srv.stallNxt.Store(1 << 20)
	l := &testLogger{}
	r := newTestRejecter(srv.addr(), 1, 100*time.Millisecond, l)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r.Reject(claims("revoked")) {
				t.Error("a stalled bloomd must not reject (fail open)")
			}
		}()
	}
	wg.Wait()

	if d := time.Since(start); d > time.Second {
		t.Errorf("20 requests took %v against a stalled bloomd; they should each give up within ~200ms", d)
	}
}

// bloomd restarting closes the connections; the pool dials again.
func TestReconnectsAfterServerDropsConnections(t *testing.T) {
	srv := newFakeBloomd(t, "127.0.0.1:0", "jti-revoked")
	l := &testLogger{}
	r := newTestRejecter(srv.addr(), 2, time.Second, l)

	if !r.Reject(claims("revoked")) {
		t.Fatal("revoked token passed")
	}
	srv.dropConns()
	time.Sleep(50 * time.Millisecond)

	// Both slots may hold a dead connection: allow one failed (fail open) request per slot.
	for i := 0; i < 2; i++ {
		r.Reject(claims("revoked"))
	}
	if !r.Reject(claims("revoked")) {
		t.Error("revoked token passed after reconnecting")
	}
	if r.Reject(claims("fine")) {
		t.Error("valid token rejected after reconnecting")
	}
}

// bloomd down when the gateway starts: Register does not give up for good.
func TestRegisterWithBloomdDownThenUp(t *testing.T) {
	probe := newFakeBloomd(t, "127.0.0.1:0")
	addr := probe.addr()
	probe.close()

	l := &testLogger{}
	rj, err := Register(config.ServiceConfig{ExtraConfig: config.ExtraConfig{
		Namespace: map[string]interface{}{
			"name":        "token_rvk",
			"server_addr": addr,
			"token_keys":  []interface{}{"jti"},
		},
	}}, l)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if l.errorCount() == 0 {
		t.Error("the failed first connection was not logged")
	}
	if rj.Reject(claims("revoked")) {
		t.Error("must fail open while bloomd is down")
	}

	newFakeBloomd(t, addr, "jti-revoked")
	if !rj.Reject(claims("revoked")) {
		t.Error("revoked token passed after bloomd came up")
	}
}

// A reply that is not one answer per key (here: the filter is missing) is an error, never a pass
// taken from a half-read stream.
func TestUnexpectedReplyIsAnErrorAndFailsOpen(t *testing.T) {
	srv := newFakeBloomd(t, "127.0.0.1:0", "jti-revoked")
	srv.missing.Store(true)
	l := &testLogger{}
	r := newTestRejecter(srv.addr(), 1, time.Second, l)

	if r.Reject(claims("revoked")) {
		t.Error("must fail open")
	}
	if l.errorCount() != 1 {
		t.Errorf("want 1 error logged, got %v", l.errors)
	}

	srv.missing.Store(false)
	if !r.Reject(claims("revoked")) {
		t.Error("revoked token passed once the filter was back")
	}
}

// A token without the checked claims is not looked up at all.
func TestNoKeysNoRoundTrip(t *testing.T) {
	srv := newFakeBloomd(t, "127.0.0.1:0")
	l := &testLogger{}
	r := newTestRejecter(srv.addr(), 1, time.Second, l)

	if r.Reject(map[string]interface{}{"sub": "x"}) {
		t.Error("rejected a token with no claims to check")
	}
	if n := srv.checks.Load(); n != 0 {
		t.Errorf("%d checks sent for a token with nothing to check", n)
	}
}

func TestRegisterConfigErrors(t *testing.T) {
	l := &testLogger{}
	if _, err := Register(config.ServiceConfig{}, l); err != errNoConfig {
		t.Errorf("no config: got %v", err)
	}
	_, err := Register(config.ServiceConfig{ExtraConfig: config.ExtraConfig{
		Namespace: map[string]interface{}{"server_addr": "127.0.0.1:1"},
	}}, l)
	if err != errNoFilterName {
		t.Errorf("no filter name: got %v", err)
	}
}
