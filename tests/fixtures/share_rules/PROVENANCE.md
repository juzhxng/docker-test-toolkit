# Captured Compose renders

Rendered from real projects with `docker compose --project-name dtt-capture --file <f>... [--profile <p>] config --format
json` (Compose 5.3.1), with the project names replaced by `sample-a`, `sample-b` and `sample-c`.

Placeholders: `$CHECKOUT` is the project's checkout and `$PRIVATE_ROOT` the run's private root. Test variables are set to
`fixture-<name>`, since images and secrets are not rendered facts.
