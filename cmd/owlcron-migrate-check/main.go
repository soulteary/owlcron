package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/soulteary/owlcron/migration"
)

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("owlcron-migrate-check", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	exportPath := flags.String("export", "", "Cronicle Data Export v1.0 file")
	configPath := flags.String("config", "", "optional Cronicle configuration file")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, err = fmt.Fprintln(out, "Usage: owlcron-migrate-check --export FILE [--config FILE]\nRead-only Cronicle migration preflight. No storage writes or plugin execution.")
			return err
		}
		return fmt.Errorf("usage: owlcron-migrate-check --export FILE [--config FILE]")
	}
	if *exportPath == "" || flags.NArg() != 0 {
		return fmt.Errorf("usage: owlcron-migrate-check --export FILE [--config FILE]")
	}
	file, err := os.Open(*exportPath)
	if err != nil {
		return fmt.Errorf("cannot open export")
	}
	defer file.Close()
	var config io.Reader
	if *configPath != "" {
		f, err := os.Open(*configPath)
		if err != nil {
			return fmt.Errorf("cannot open configuration")
		}
		defer f.Close()
		config = f
	}
	report, err := migration.Check(file, config)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "Migration check:", err)
		os.Exit(1)
	}
}
