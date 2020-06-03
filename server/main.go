package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

func usage() {
	log.Printf("Usage: nats-fs [-s server] [-creds file] <directory>\n")
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		showUsageAndExit(1)
	}

	file := args[0]
	if stat, err := os.Stat(file); os.IsNotExist(err) {
		log.Fatalf("File %q does not exist", file)
	} else if stat.IsDir() {
		log.Fatalf("%q is a directory", file)
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS HTTP File Server")}

	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Connect to NATS
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	h := func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, file)
	}

	// Handle via NATS.
	natsHandleFunc(nc, "foo", h)

	// Handle via HTTP
	http.HandleFunc("/", h)

	log.Printf("Listening on HTTP localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// Our own response writer.
type nrw struct {
	sync.Mutex
	reply   string
	nc      *nats.Conn
	hdr     *nats.Msg
	inbox   string
	asub    *nats.Subscription
	acks    chan struct{}
	index   int
	pending int
}

func (w *nrw) Header() http.Header {
	if w.hdr == nil {
		w.hdr = nats.NewMsg(w.reply)
	}
	return w.hdr.Header
}

const defaultWindowSize = 32 * 1024 * 1024

func (w *nrw) processFlowAck(m *nats.Msg) {
	// Last token of the subject is chunk size.
	tokens := strings.Split(m.Subject, ".")
	if len(tokens) < 2 {
		log.Printf("Bad ack subject %q", m.Subject)
		return
	}
	chunkSize, err := strconv.Atoi(tokens[len(tokens)-1])
	if err != nil {
		log.Printf("Bad ack subject %q", m.Subject)
		return
	}
	w.Lock()
	w.pending -= chunkSize
	w.Unlock()
}

func (w *nrw) Write(data []byte) (int, error) {
	w.Lock()
	defer w.Unlock()

	if w.acks == nil {
		w.inbox = nats.NewInbox()
		w.asub, _ = w.nc.Subscribe(fmt.Sprintf("%s.*", w.inbox), w.processFlowAck)
		w.acks = make(chan struct{}, 1)
	}
	if w.pending > defaultWindowSize {
		// Unlock if we are held up.
		acks := w.acks
		w.Unlock()
		select {
		case <-acks:
		case <-time.After(time.Millisecond):
		}
		w.Lock()
	}
	ackReply := fmt.Sprintf("%s.%d", w.inbox, len(data))
	if err := w.nc.PublishRequest(w.reply, ackReply, data); err != nil {
		return 0, err
	}
	w.pending += len(data)
	return len(data), nil
}

func (w *nrw) WriteHeader(statusCode int) {
	w.Lock()
	w.hdr.Header.Add("Status", fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)))
	w.nc.PublishMsg(w.hdr)
	w.Unlock()
}

func natsHandleFunc(nc *nats.Conn, subject string, handler func(w http.ResponseWriter, r *http.Request)) {
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		// Determine if HTTP request format. For now assume its not and construct one.
		method := "GET"
		if hm := m.Header.Get("Method"); hm != "" {
			method = hm
		}
		path := m.Header.Get("URL")
		if path == "" {
			path = "/"
		}
		buf := bytes.NewBuffer(m.Data)
		req, err := http.NewRequest(method, path, buf)
		if err != nil {
			log.Printf("Error creating http request: %v", err)
		}
		req.Header = m.Header
		w := &nrw{nc: nc, reply: m.Reply}

		// Call into our handler.
		go func() {
			handler(w, req)
			w.Lock()
			w.asub.Unsubscribe()
			w.Unlock()
		}()
	})

	if err != nil {
		log.Fatalf("NATS Error subscribing to %q, %v", subject, err)
	}
}
