# Contributing to Torrin

Thanks for your interest in contributing! Here's how to get started.

## Project structure

- `internal/` -- open-source core (MIT licensed)
- `web/` -- React frontend (private, not in repo)
- `worker/` -- Cloudflare Worker (private, not in repo)
- `private/` -- proprietary API server (not in repo)

Contributions are welcome to anything in `internal/`.

## Getting started

1. Fork the repo
2. Clone your fork
3. Create a branch: `git checkout -b my-feature`
4. Make your changes
5. Run tests: `go test ./...`
6. Push and open a PR

## Code style

- Go: follow standard `gofmt` formatting
- Keep functions small and focused
- No unnecessary abstractions
- Error messages should be lowercase, no punctuation

## Reporting bugs

Open an issue with:
- What you expected
- What happened instead
- Steps to reproduce
- Version/environment info

## Feature requests

Open an issue describing the use case, not just the solution. We'll discuss the best approach together.

## Pull requests

- Keep PRs focused on one thing
- Write a clear description of what and why
- Reference any related issues
- Make sure `go build ./...` passes
