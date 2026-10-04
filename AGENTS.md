# Project instructions

## Communication

- Use simple technical English and short sentences, preferably under 20 words.
- Use active voice and consistent terms.
- Follow the Google developer documentation style guide for documentation.
- Prefer concise commands and examples over tutorials.
- Ask when ambiguity materially changes scope, behavior, or risk. Otherwise, state an assumption and proceed.

## Project layout

- `main.go`: CLI flags, orchestration, progress output, and result formatting.
- `api.go`: fast.com discovery, target decoding, and request URL construction.
- `measure.go`: transfer workers, throughput sampling, and latency probes.
- `README.md`: installation, usage, flags, and measurement behavior.
- `Makefile`: build, test, vet, format, and install commands.

## Implementation

- Keep this CLI simple and dependency-free. Use the Go version declared in `go.mod`.
- Make surgical edits. Preserve unrelated code, comments, and formatting.
- Match existing style. Avoid speculative features and unnecessary abstractions.
- Remove unused code introduced by your changes. Leave pre-existing unrelated issues unchanged.
- For nontrivial changes, consider whether a simpler readable solution exists.
- Keep machine-readable results on stdout and diagnostics on stderr.
- Bound network operations with context deadlines and propagate measurement failures.
- Keep documented flags and output formats consistent with implementation.

## Validation

- Run `gofmt` on changed Go files.
- Run `go test ./...` and `go vet ./...` after code changes.
- Add focused regression tests for behavior changes using local fixtures or mock transports.
- Keep tests independent of live fast.com endpoints.
- Run live speed tests only when requested; they consume network bandwidth.

## Safety

- Never read `.env`, `.envrc`, `*.auto.tfvars`, `credentials.json`, or other likely-secret files.
- Exclude likely-secret files from searches. Check existence only when necessary.
- Never send keys, passwords, tokens, or connection strings through cloud APIs. Reference them instead.
- Never print environment variables or credential-bearing output.
- Never perform destructive operations on existing user data or shared state. Give the user instructions instead.
- You may remove disposable files and branches created for the current task.
- Confirm before modifying shared or production state.
- Send or draft Gmail messages only to `yavosh@gmail.com` or addresses under `nar.cy` or `narity.com`.

## Git

- Choose a descriptive feature branch automatically. Stay on an existing open PR branch for the same concern.
- Never push to `main` or create a throwaway branch solely to hold a commit.
- Serialize Git operations that modify repository state.
- Never add AI attribution trailers or generated-by footers.
- Do not use `glab`; it is unavailable.
- For authorized GitLab MR creation, use these push options:

  ```sh
  git push -o merge_request.create -o merge_request.target=main -o merge_request.squash=true -o merge_request.remove_source_branch=true
  ```
