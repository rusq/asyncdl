package asyncdl

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/rusq/fsadapter"
	"github.com/stretchr/testify/assert"
)

var mu sync.Mutex // globals mutex

var (
	emptyFetchFn = func(ctx context.Context, fsa fsadapter.FS, dir, name, uri string) error { return nil }
	errorFetchFn = func(ctx context.Context, fsa fsadapter.FS, dir, name, uri string) error {
		return errors.New("your shattered hopes")
	}
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func Test_get(t *testing.T) {
	type args struct {
		ctx  context.Context
		dir  string
		name string
	}
	type serverOptions struct {
		status int
		body   []byte
	}
	tests := []struct {
		name          string
		args          args
		opts          serverOptions
		wantErr       bool
		wantFileExist bool
		wantFileData  []byte
	}{
		{
			"ok",
			args{context.Background(), "test", "file"},
			serverOptions{status: http.StatusOK, body: []byte("test data")},
			false,
			true,
			[]byte("test data"),
		},
		{
			"404",
			args{context.Background(), "test", "file"},
			serverOptions{status: http.StatusNotFound, body: nil},
			true,
			true,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.opts.status)
				if _, err := w.Write(tt.opts.body); err != nil {
					panic(err)
				}
			}))
			defer server.Close()

			dir := t.TempDir()
			fsa, err := fsadapter.New(dir)
			if err != nil {
				t.Fatalf("failed to create test dir: %s", err)
			}

			if err := get(tt.args.ctx, http.DefaultClient, fsa, tt.args.dir, tt.args.name, server.URL); (err != nil) != tt.wantErr {
				t.Errorf("fetch() error = %v, wantErr %v", err, tt.wantErr)
			}

			testfile := filepath.Join(dir, tt.args.dir, tt.args.name)
			_, err = os.Stat(testfile)
			if notExist := os.IsNotExist(err); notExist != !tt.wantFileExist {
				t.Errorf("os.IsNotExist=%v tt.wantFileExist=%v", notExist, tt.wantFileExist)
			}
			if !tt.wantFileExist {
				return
			}
			got, err := os.ReadFile(testfile)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tt.wantFileData) {
				t.Errorf("file contents error: want=%v got=%v", tt.wantFileData, got)
			}
		})
	}
}

func testRequestC(requests []request, wantClosed bool) <-chan request {
	ch := make(chan request)
	go func() {
		for _, r := range requests {
			ch <- r
		}
		if wantClosed {
			close(ch)
		}
	}()
	return ch
}

func Test_worker(t *testing.T) {
	type args struct {
		ctx      context.Context
		dir      string
		requestC <-chan request
		// result is provided by test
	}
	tests := []struct {
		name       string
		args       args
		fetchFn    fetchFunc
		wantResult []result
	}{
		{
			"all ok",
			args{
				ctx:      context.Background(),
				dir:      "",
				requestC: testRequestC([]request{{"test", "passed"}}, true),
			},
			func(_ context.Context, _ *http.Client, _ fsadapter.FS, _ string, _ string, _ string) error {
				return nil
			},
			[]result{
				{filename: "test"},
			},
		},
		{
			"cancelled context",
			args{
				ctx:      func() context.Context { ctx, cancel := context.WithCancel(context.Background()); cancel(); return ctx }(),
				dir:      "",
				requestC: testRequestC([]request{}, false),
			},
			func(_ context.Context, _ *http.Client, _ fsadapter.FS, _ string, _ string, _ string) error {
				return nil
			},
			[]result{
				{filename: "", err: context.Canceled},
			},
		},
		{
			"fetch error",
			args{
				ctx:      context.Background(),
				requestC: testRequestC([]request{{"test", "passed"}}, true),
			},
			func(_ context.Context, _ *http.Client, _ fsadapter.FS, _ string, _ string, _ string) error {
				return io.EOF
			},
			[]result{
				{filename: "test", err: io.EOF},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsa, _ := fsadapter.New(t.TempDir())
			resultC := make(chan result)
			m := New(fsa)
			m.fetchFn = tt.fetchFn

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				m.worker(tt.args.ctx, tt.args.dir, tt.args.requestC, resultC)
				wg.Done()
			}()
			go func() {
				wg.Wait()
				close(resultC)
			}()
			var results []result
			for r := range resultC {
				results = append(results, r)
			}
			if !reflect.DeepEqual(results, tt.wantResult) {
				t.Errorf("results mismatch:\n\twant=%v\n\tgot =%v", tt.wantResult, results)
			}
		})
	}
}

func Test_fetch(t *testing.T) {
	t.Run("concurrent download of fake urls", func(t *testing.T) {
		urls := generateURLs(50)
		fsa, _ := fsadapter.New(t.TempDir())
		defer fsa.Close()

		got := make(map[string]string, len(urls))
		var gotMu sync.Mutex
		m := Manager{
			fsc: fsa,
			fetchFn: func(_ context.Context, _ *http.Client, _ fsadapter.FS, _ string, filename string, uri string) error {
				gotMu.Lock()
				got[filename] = uri
				gotMu.Unlock()
				return nil
			},
		}

		err := m.Download(context.Background(), "", urls)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		parsed, err := parseURLs(urls)
		if err != nil {
			t.Fatalf("error parsing generated URLs: %s", err)
		}
		want := make(map[string]string, len(urls))
		for _, r := range parsed {
			want[r.filename] = r.url
		}
		assert.Equal(t, want, got)

		if !reflect.DeepEqual(want, got) {
			t.Error("emojis!=got")
		}
	})
}

func generateURLs(n int) (ret []string) {
	ret = make([]string, n)
	for i := 0; i < n; i++ {
		filename := randString(10) + "." + randString(3)
		ret = append(ret, "https://sample.com/"+randString(10)+"/"+filename)
	}
	return
}

func randString(n int) string {
	chars := []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]rune, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func Test_basename(t *testing.T) {
	type args struct {
		uri string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"valid https file url",
			args{
				"https://foo.bar/baz.txt",
			},
			"baz.txt",
			false,
		},
		{
			"valid http file url",
			args{
				"http://localhost/foo.txt",
			},
			"foo.txt",
			false,
		},
		{
			"invalid url",
			args{
				"borked/lol",
			},
			"",
			true,
		},
		{
			"url with a directory",
			args{"http://localhost/foo/"},
			"",
			true,
		},
		{
			"empty URL",
			args{""},
			"",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := basename(tt.args.uri)
			if (err != nil) != tt.wantErr {
				t.Errorf("basename() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("basename() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseURLs(t *testing.T) {
	type args struct {
		urls []string
	}
	tests := []struct {
		name    string
		args    args
		want    []request
		wantErr bool
	}{
		{
			"valid URLs",
			args{[]string{
				"https://example.com/foo.txt",
				"https://localhost/bar.jpg",
			}},
			[]request{
				{filename: "foo.txt", url: "https://example.com/foo.txt"},
				{filename: "bar.jpg", url: "https://localhost/bar.jpg"},
			},
			false,
		},
		{
			"invalid url leads to an error",
			args{[]string{
				"https://example.com/foo.txt",
				"u fail",
			}},
			nil,
			true,
		},
		{
			"empty URL skipped",
			args{[]string{
				"",
				"https://localhost/bar.jpg",
			}},
			[]request{
				{filename: "bar.jpg", url: "https://localhost/bar.jpg"},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseURLs(tt.args.urls)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseURLs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseURLs() = %v, want %v", got, tt.want)
			}
		})
	}
}
