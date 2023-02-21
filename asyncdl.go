// Package adownload provides a simple download Manager for asynchronous
// download of a list of URLs.
package asyncdl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/rusq/dlog"
	"github.com/rusq/fsadapter"
)

const (
	// defNumWorkers is the default number of goroutines to spawn for
	// concurrent download.
	defNumWorkers = 12
)

// Download downloads files given the list of urls to the subdirectory within
// the filesystem wrapped by fs adapter fsa.  URLs must point to files,
// otherwise the behaviour is undefined.  It does not support the recursive
// download.
func Download(ctx context.Context, fsa fsadapter.FS, subdir string, urls []string, opts ...Option) error {
	return New(fsa, opts...).Download(ctx, subdir, urls)
}

// DownloadToPath downloads urls into a subdirectory subdir of a ZIP file or
// directory specified by zipOrDir.  See [Download] for more details.
func DownloadToPath(ctx context.Context, zipOrDir string, subdir string, urls []string, opts ...Option) error {
	m, err := NewWithPath(zipOrDir, opts...)
	if err != nil {
		return nil
	}
	defer m.Close()
	return m.Download(ctx, subdir, urls)
}

// Manager is the download manager.
type Manager struct {
	// numWorkers is the number of download workers.
	numWorkers int
	// fetchFn is the function that is called to download each of the provided
	// URLs.
	fetchFn fetchFunc
	// if ignoreHTTPerr is false, the Download will terminate on any HTTP GET
	// error.  If false, it will ignore the error and continue.
	ignoreHTTPerr bool

	// fsc is the base file system adapter, it points to a filesystem which
	// we are free to create files or directories in.
	fsc fsadapter.FSCloser
	// isClosed indicates, that the close was called on fs adapter.
	isClosed atomic.Bool
}

// Option is the download manager option.
type Option func(m *Manager)

// NumWorkers allows to set the number of goroutines that will download
// the files.
func NumWorkers(n int) Option {
	return func(m *Manager) {
		if n < 1 {
			n = defNumWorkers
		}
		m.numWorkers = n
	}
}

// IgnoreHTTPErrors allows to ignore HTTP errors (enabled by default)
func IgnoreHTTPErrors(isEnabled bool) Option {
	return func(m *Manager) {
		m.ignoreHTTPerr = isEnabled
	}
}

type fetchFunc func(ctx context.Context, fsa fsadapter.FS, dir string, name string, uri string) error

// New creates a new download Manager.
func New(fsa fsadapter.FS, opts ...Option) *Manager {
	return newMgr(&nopcloser{fsa}, opts...)
}

func newMgr(fsc fsadapter.FSCloser, opts ...Option) *Manager {
	m := &Manager{
		numWorkers:    defNumWorkers,
		fetchFn:       get,
		fsc:           fsc,
		ignoreHTTPerr: true,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// nopcloser is a wrapper around fsadapter.FS which implements the
// fsadapter.FSCloser interface, but does not close the underlying file system.
type nopcloser struct {
	fsadapter.FS
}

// Close is a noop.
func (n *nopcloser) Close() error { return nil }

// NewWithPath creates a download [Manager] which initialises a file system
// adapter over the ZIP file or Directory specified in zipOrDir.  It must be
// closed with [Close] to ensure that all buffers are flushed to disk.
func NewWithPath(zipOrDir string, opts ...Option) (*Manager, error) {
	fsa, err := fsadapter.New(zipOrDir)
	if err != nil {
		return nil, err
	}
	m := newMgr(fsa, opts...)
	m.isClosed.Store(false)
	return m, nil
}

// Close closes the underlying filesystem adapter, if necessary.  If the
// Manager wasn't initialised with [NewWithPath] then calling Close is noop.
func (m *Manager) Close() error {
	if _, ok := m.fsc.(*nopcloser); ok {
		return nil
	}

	if m.isClosed.Load() {
		return nil
	}
	m.isClosed.Store(true)
	return m.fsc.Close()
}

// request is the download request.
type request struct {
	filename string
	url      string
}

// Download downloads the files and saves them to the dir directory within the
// file system adapter fsa. It spawns numWorker goroutines for getting the
// files. It will call fetchFn for each url.
func (m *Manager) Download(ctx context.Context, dir string, urls []string) error {
	if m.isClosed.Load() {
		return errors.New("manager is closed")
	}
	if m.numWorkers == 0 {
		m.numWorkers = defNumWorkers
	}

	lg := dlog.FromContext(ctx)

	reqs, err := parseURLs(urls)
	if err != nil {
		return fmt.Errorf("error parsing urls: %w", err)
	}

	var (
		urlC    = make(chan request)
		resultC = make(chan result)
	)

	// Async download pipeline.

	// 1. generator, send urls into the emojiC channel.
	go func() {
		defer close(urlC)
		for _, req := range reqs {
			select {
			case <-ctx.Done():
				return
			case urlC <- req:
			}
		}
	}()

	// 2. Download workers, download the urls.
	var wg sync.WaitGroup
	for i := 0; i < m.numWorkers; i++ {
		wg.Add(1)
		go func() {
			m.worker(ctx, dir, urlC, resultC)
			wg.Done()
		}()
	}
	// 3. Sentinel, closes the result channel once all workers are finished.
	go func() {
		wg.Wait()
		close(resultC)
	}()

	// 4. Result processor, receives download results and logs any errors that
	//    may have occurred.
	var (
		total = len(urls)
		count = 0
	)
	for res := range resultC {
		if res.err != nil {
			if errors.Is(res.err, context.Canceled) {
				return res.err
			}
			if !m.ignoreHTTPerr {
				return fmt.Errorf("failed: %q: %w", res.filename, res.err)
			}
			lg.Printf("failed: %q: %s", res.filename, res.err)
		}
		count++
		lg.Printf("downloaded % 5d/%d %q", count, total, res.filename)
	}

	return nil
}

type result struct {
	filename string
	err      error
}

// worker is the function that runs in a separate goroutine and downloads
// files received from requestC.  The result of the operation is sent to
// resultC channel.
func (m *Manager) worker(ctx context.Context, dir string, requestC <-chan request, resultC chan<- result) {
	for {
		select {
		case <-ctx.Done():
			resultC <- result{err: ctx.Err()}
			return
		case req, more := <-requestC:
			if !more {
				return
			}
			err := m.fetchFn(ctx, m.fsc, dir, req.filename, req.url)
			resultC <- result{filename: req.filename, err: err}
		}
	}
}

// get downloads one file from the uri into the dir/filename within the
// filesystem wrapped with the fsa.
func get(ctx context.Context, fsa fsadapter.FS, dir string, filename, uri string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	fp := filepath.Join(dir, filename)
	wc, err := fsa.Create(fp)
	if err != nil {
		return fmt.Errorf("error creating the file at path %q: %w", fp, err)
	}
	defer wc.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid server status code: %d (%s)", resp.StatusCode, resp.Status)
	}

	if _, err := io.Copy(wc, resp.Body); err != nil {
		return err
	}

	return nil
}

var httpRe = regexp.MustCompile("^https?://.*[^/]$")

// basename returns the last element of the path in the URL.
func basename(uri string) (string, error) {
	if !httpRe.MatchString(uri) {
		return "", fmt.Errorf("invalid uri: %s", uri)
	}
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	bp := path.Base(u.Path)
	if len(bp) < 2 {
		// base path of "." or "/" is invalid.
		return "", fmt.Errorf("not a file: %s", bp)
	}
	return bp, nil
}

// parseURLs parses the provided URLs and returns a slice of requests.
func parseURLs(urls []string) ([]request, error) {
	reqs := make([]request, 0, len(urls))
	for _, uri := range urls {
		if len(uri) == 0 {
			continue
		}
		filename, err := basename(uri)
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, request{filename: filename, url: uri})
	}
	return reqs, nil
}
