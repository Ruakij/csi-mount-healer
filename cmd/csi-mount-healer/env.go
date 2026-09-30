package main

import (
	"flag"
	"fmt"
	"strings"
)

// envName is the environment variable for a flag: -kubelet-root is KUBELET_ROOT.
func envName(flagName string) string {
	return strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

// applyEnv sets every flag of fs from its environment variable, if present, and
// names that variable in the help. Called before parsing, so a command-line flag
// still wins over the environment, which wins over the default.
func applyEnv(fs *flag.FlagSet, lookup func(string) (string, bool)) error {
	var err error
	fs.VisitAll(func(f *flag.Flag) {
		name := envName(f.Name)
		f.Usage += " [$" + name + "]"
		if v, ok := lookup(name); ok && err == nil {
			if setErr := f.Value.Set(v); setErr != nil {
				err = fmt.Errorf("invalid $%s %q: %w", name, v, setErr)
			}
		}
	})
	return err
}
