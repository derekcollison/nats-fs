package main

import (
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"
)

// NOTE: Can test with demo servers.
// nats-req -s demo.nats.io <subject> <msg>
// nats-req -s demo.nats.io:4443 <subject> <msg> (TLS version)

func usage() {
	log.Printf("Usage: nats-req [-s server] [-creds file] <subject> <msg>\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func main() {
	var (
		urls        = flag.String("s", nats.DefaultURL, "The NATS System")
		userCreds   = flag.String("creds", "", "Credentials")
		showHelp    = flag.Bool("h", false, "Show help message")
		showHeaders = flag.Bool("i", false, "Show message headers")
		output      = flag.String("output", "", "Output file")
	)

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) < 1 {
		showUsageAndExit(1)
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS HTTP Style Requestor")}

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

	subj := args[0]

	req := nats.NewMsg(subj)
	req.Header.Add("Accept", "*/*")
	req.Header.Add("User-Agent", "nats-fs-client/0.1")
	req.Header.Add("Method", "GET")
	if len(args) > 1 {
		req.Header.Add("URL", args[1])
	}
	req.Reply = nats.NewInbox()

	sub, _ := nc.SubscribeSync(req.Reply)
	nc.PublishMsg(req)

	// Grab first message.
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		if nc.LastError() != nil {
			log.Fatalf("%v for request", nc.LastError())
		}
		log.Fatalf("%v for request", err)
	}
	// Check Status
	if status := msg.Header.Get("Status"); !strings.HasPrefix(status, "200") {
		log.Fatalf("Error retrieving resource %q", status)
	}

	// Grab Content-Length
	cl, err := strconv.Atoi(msg.Header.Get("Content-Length"))
	if err != nil {
		log.Fatalf("Expected a Content-Length")
	}

	if *showHeaders {
		log.Printf("Received  [%v]\n", msg.Subject)
		for k, v := range msg.Header {
			log.Printf("\u001b[1m%s:\u001b[0m %s\n", k, strings.Join(v, ","))
		}
	}

	var fd *os.File
	if *output != "" {
		if fd, err = os.OpenFile(*output, os.O_CREATE|os.O_RDWR, 0644); err != nil {
			log.Fatalf("Error opening output file %q: %v", *output, err)
		}
	}

	for received, checked := 0, false; received < cl; received += len(msg.Data) {
		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil || len(msg.Data) == 0 {
			break
		}
		if !checked && fd == nil {
			// Check if the data is printable vs binary
			if !isPrintable(msg.Data) {
				log.Fatalf("Warning, data received is binary, consider using -output FILE")
			}
			checked = true
		}
		if fd != nil {
			fd.Write(msg.Data)
		} else {
			log.Printf("\n%s", msg.Data)
		}
		// ack flow control
		msg.Respond(nil)
	}
}

func isPrintable(data []byte) bool {
	const snippetSize = 32
	s := string(data)
	if len(s) > snippetSize {
		s = s[:snippetSize]
	}
	for _, r := range string(s) {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}
