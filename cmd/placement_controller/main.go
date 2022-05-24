// Binary placement_controller implements a node controller that applies
// placement policies.
package main

import (
	"flag"
	"log"

	"github.com/amscanne/placement-controller/pkg/options"
	"github.com/amscanne/placement-controller/pkg/server"
)

func main() {
	opts := new(options.Options)
	opts.AddFlags(flag.CommandLine)
	flag.Parse()

	// Ensure options are well-formed.
	if err := opts.Validate(); err != nil {
		log.Fatalf("invalid options: %v", err)
	}

	// Run the server.
	if err := server.Run(opts); err != nil {
		log.Fatalf("placement-controller failed to run: %v", err)
	}
}
