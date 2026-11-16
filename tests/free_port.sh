# Sourced by the shell suites: free_ports N prints N distinct TCP ports
# that nothing holds, chosen by the kernel (FLAKE-13).
#
# Ports used to come from the pid, skipping any `ss` showed busy, so two
# suites on one host could still pick the same one between the check and
# the bind, and a daemon then failed to start or a test reached the other
# run's daemon. tests/tools/free_port.c asks the kernel instead.
#
# make builds the helper and names it in C_BACKUP_FREE_PORT. Run by hand,
# the suite uses build/bins/free-port, or compiles the helper into its own
# scratch directory if that is missing too.

free_ports() {  # free_ports <n> [scratch dir for a fallback build]
    local bin="${C_BACKUP_FREE_PORT:-}"
    local here
    here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    [ -n "$bin" ] && [ -x "$bin" ] || bin="$here/../build/bins/free-port"
    if [ ! -x "$bin" ]; then
        local dir="${2:-${TMPDIR:-/tmp}}"
        bin="$dir/free-port"
        if [ ! -x "$bin" ] && ! cc -O2 -o "$bin" "$here/tools/free_port.c"; then
            echo "free-port: cannot build $here/tools/free_port.c" >&2
            return 1
        fi
    fi
    "$bin" "$1"
}
