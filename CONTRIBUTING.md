# Contributing to Conduit

Thanks for contributing. Keep the core transport-neutral: transport packages translate native messages to `conduit.Delivery`, while the core owns typed decoding, processor execution, and acknowledgement.

Before opening a pull request, format with `gofmt -w .` and run `go test ./...`. New adapters should include acknowledgement tests and a connection example.
