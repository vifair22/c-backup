#!/usr/bin/env bash
# Self-tests for the hygiene gates in ci/: each must fail when it should, not
# only pass on today's tree. A gate that cannot fail is decoration.
#
# Run by `make test` and by the CI lint job. The clang-tidy canary needs a
# real clang-tidy; it is skipped without one unless TIDY_REQUIRED=1 (the CI
# job sets it, so the canary always runs there).
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
PY=${PYTHON:-python3}
CLANG_TIDY=${CLANG_TIDY:-clang-tidy}
SCRATCH_BASE=${C_BACKUP_TEST_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/c-backup-test}
mkdir -p "$SCRATCH_BASE"
T=$(mktemp -d "$SCRATCH_BASE/hygiene.XXXXXX")
trap 'rm -rf "$T"' EXIT

pass=0
fail=0
ok()  { pass=$((pass + 1)); echo "  ok   $1"; }
bad() { fail=$((fail + 1)); echo "  FAIL $1"; }

# expect <exit code> <description> <command...>
expect() {
    local want=$1 what=$2; shift 2
    local out rc
    out=$("$@" 2>&1); rc=$?
    if [ "$rc" -eq "$want" ]; then ok "$what"; else
        bad "$what (exit $rc, wanted $want)"; printf '%s\n' "$out" | sed 's/^/       /'
    fi
    LAST_OUT=$out
}

# gen <file> <n>: a C file with exactly n code lines, padded with comments
# and blank lines that must not count.
gen() {
    local f=$1 n=$2 i
    mkdir -p "$(dirname "$f")"
    {
        echo "/* header comment"
        echo "   spanning lines */"
        echo ""
        for ((i = 0; i < n; i++)); do
            echo "int v$i; // trailing comment"
            echo "    // a comment-only line"
        done
    } > "$f"
}

echo "=== size_check ==="
expect 0 "line counter handles comments and literals" "$PY" -B - "$HERE" <<'EOF'
import sys
sys.path.insert(0, sys.argv[1])
from size_check import code_lines
cases = [
    ("", 0),
    ("\n\n   \n", 0),
    ("// only\n", 0),
    ("/* a\n b\n c */\n", 0),
    ("int a; /* x */\n", 1),
    ("/* x */ int a;\n", 1),
    ("/* x\n */ int a;\n", 1),
    ('char *s = "/* not a comment";\nint b;\n', 2),
    ('char *s = "// nor this";\n', 1),
    ("char c = '\"'; // q\n", 1),
    ("// continued \\\n still comment\nint x;\n", 1),
    ("#include <stdio.h>\n", 1),
    ('const char *e = "a\\"b /* x"; int y;\n', 1),
]
bad = [(src, want, code_lines(src)) for src, want in cases if code_lines(src) != want]
for src, want, got in bad:
    print(f"{src!r}: wanted {want}, got {got}")
sys.exit(1 if bad else 0)
EOF

S=$T/size
gen "$S/src/a/ok.c" 2000
gen "$S/tests/t.h" 10
expect 0 "a file at the ceiling passes" "$PY" "$HERE/size_check.py" --root "$S"
gen "$S/src/a/big.c" 2001
expect 1 "a file one line over the ceiling fails" "$PY" "$HERE/size_check.py" --root "$S"
expect 0 "--init records it" "$PY" "$HERE/size_check.py" --root "$S" --init
expect 0 "a recorded file at its baseline passes" "$PY" "$HERE/size_check.py" --root "$S"
gen "$S/src/a/big.c" 2002
expect 1 "a recorded file that grows fails" "$PY" "$HERE/size_check.py" --root "$S"
gen "$S/bench/new.c" 2001
gen "$S/src/a/big.c" 2001
expect 1 "a new file over the ceiling fails beside a baseline" "$PY" "$HERE/size_check.py" --root "$S"
rm "$S/bench/new.c"
mkdir -p "$S/build" && gen "$S/build/gen.c" 5000
expect 0 "build/ is out of scope" "$PY" "$HERE/size_check.py" --root "$S"
printf '2500 src/a/big.c\n' > "$S/ci/size-baseline.txt"
expect 0 "a file under its baseline passes" "$PY" "$HERE/size_check.py" --root "$S"
case $LAST_OUT in *"make size-ratchet"*) ok "and suggests the ratchet";; *) bad "no ratchet note";; esac
expect 0 "--update lowers the entry" "$PY" "$HERE/size_check.py" --root "$S" --update
if grep -qx '2001 src/a/big.c' "$S/ci/size-baseline.txt"; then ok "baseline now 2001"; else bad "baseline not lowered"; fi
gen "$S/src/a/big.c" 1500
expect 0 "--update drops a file back under the ceiling" "$PY" "$HERE/size_check.py" --root "$S" --update
if grep -q 'big.c' "$S/ci/size-baseline.txt"; then bad "entry kept"; else ok "entry dropped"; fi
gen "$S/src/a/big.c" 2001
expect 1 "and it cannot grow back past the ceiling" "$PY" "$HERE/size_check.py" --root "$S"
printf '1999 src/a/big.c\n' > "$S/ci/size-baseline.txt"
gen "$S/src/a/big.c" 2400
expect 1 "--update never raises an entry" "$PY" "$HERE/size_check.py" --root "$S" --update

echo "=== layout_check ==="
L=$T/layout
mkdir -p "$L/src/one" "$L/src/two/sub" "$L/docs"
touch "$L/src/one/a.c" "$L/src/two/sub/b.c"
printf '| Path | Purpose |\n|---|---|\n| `src/one/` | x |\n| `src/two/sub/` | y |\n' > "$L/docs/layout.md"
expect 0 "every directory listed passes" "$PY" "$HERE/layout_check.py" --root "$L"
mkdir -p "$L/src/three" && touch "$L/src/three/c.c"
expect 1 "an unlisted directory fails" "$PY" "$HERE/layout_check.py" --root "$L"
printf '| `src/three/` (qualified) | z |\n' >> "$L/docs/layout.md"
expect 0 "a row with a qualifier counts" "$PY" "$HERE/layout_check.py" --root "$L"
printf '| `src/gone/` | z |\n' >> "$L/docs/layout.md"
expect 1 "a row for a missing directory fails" "$PY" "$HERE/layout_check.py" --root "$L"
rm "$L/docs/layout.md"
expect 1 "a missing layout file fails" "$PY" "$HERE/layout_check.py" --root "$L"

echo "=== tidy_gate (baseline logic, fake clang-tidy) ==="
G=$T/tidy
mkdir -p "$G/src/a" "$G/bin"
touch "$G/src/a/x.c" "$G/src/a/y.c" "$G/src/a/h.h"
# The fake prints whatever $G/out.<basename> holds, as clang-tidy would.
cat > "$G/bin/clang-tidy" <<EOF
#!/usr/bin/env bash
[ "\$1" = "--version" ] && { echo "fake version 0"; exit 0; }
f=\$2
cat "$G/out.\$(basename "\$f")" 2>/dev/null
grep -q ': error:' "$G/out.\$(basename "\$f")" 2>/dev/null && exit 1
exit 0
EOF
chmod +x "$G/bin/clang-tidy"
FT="$G/bin/clang-tidy"
echo "src/a/x.c:3:5: warning: value 12 is odd [bugprone-x]" > "$G/out.x.c"
echo "src/a/h.h:1:1: warning: in a header [misc-y]" > "$G/out.y.c"
echo "src/a/h.h:1:1: warning: in a header [misc-y]" >> "$G/out.x.c"
expect 0 "--update records a baseline" "$PY" "$HERE/tidy_gate.py" --root "$G" --tidy "$FT" --update
if [ "$(grep -vc '^#' "$G/ci/tidy-baseline.txt")" -eq 2 ]; then ok "two keys, header warning deduplicated"
else bad "baseline has the wrong keys"; cat "$G/ci/tidy-baseline.txt"; fi
expect 0 "an unchanged tree passes" "$PY" "$HERE/tidy_gate.py" --root "$G" --tidy "$FT"
echo "src/a/x.c:90:5: warning: value 7 is odd [bugprone-x]" > "$G/out.x.c"
expect 0 "a moved warning with other digits is the same key" "$PY" "$HERE/tidy_gate.py" --root "$G" --tidy "$FT"
echo "src/a/x.c:91:5: warning: value 8 is odd [bugprone-x]" >> "$G/out.x.c"
expect 1 "a second instance of a key fails" "$PY" "$HERE/tidy_gate.py" --root "$G" --tidy "$FT"
echo "src/a/x.c:90:5: warning: something else [bugprone-z]" > "$G/out.x.c"
expect 1 "a new check fails" "$PY" "$HERE/tidy_gate.py" --root "$G" --tidy "$FT"
echo "/usr/include/stdio.h:1:1: warning: system header [bugprone-z]" > "$G/out.x.c"
expect 0 "warnings outside src/ are ignored" "$PY" "$HERE/tidy_gate.py" --root "$G" --tidy "$FT"
echo "src/a/x.c:1:10: error: 'nope.h' file not found [clang-diagnostic-error]" > "$G/out.x.c"
expect 1 "a file that does not parse fails" "$PY" "$HERE/tidy_gate.py" --root "$G" --tidy "$FT"
: > "$G/out.x.c"

# Changed-files mode against a git history.
(
    cd "$G" && git init -q && git add -A &&
    git -c user.name=t -c user.email=t@t -c commit.gpgsign=false -c core.hooksPath=/dev/null commit -qm base
) >/dev/null 2>&1
BASE=$(git -C "$G" rev-parse HEAD)
commit() { git -C "$G" add -A && git -C "$G" -c user.name=t -c user.email=t@t -c commit.gpgsign=false -c core.hooksPath=/dev/null commit -qm c >/dev/null; }
echo "src/a/y.c:4:1: warning: new in y [bugprone-q]" > "$G/out.y.c"
echo "int z;" >> "$G/src/a/x.c"; commit
expect 0 "changed mode skips an untouched file" "$PY" "$HERE/tidy_gate.py" --root "$G" --tidy "$FT" --changed-from "$BASE"
echo "int z;" >> "$G/src/a/y.c"; commit
expect 1 "changed mode checks a changed file" "$PY" "$HERE/tidy_gate.py" --root "$G" --tidy "$FT" --changed-from "$BASE"
git -C "$G" reset -q --hard HEAD~1
echo "int h;" >> "$G/src/a/h.h"; commit
expect 1 "a changed header runs in full" "$PY" "$HERE/tidy_gate.py" --root "$G" --tidy "$FT" --changed-from "$BASE"
case $LAST_OUT in *"running in full"*) ok "and says so";; *) bad "no full-run note";; esac

echo "=== tidy_gate canary (real clang-tidy, the shipped .clang-tidy) ==="
if command -v "$CLANG_TIDY" >/dev/null 2>&1; then
    C=$T/canary
    mkdir -p "$C/src"
    cp "$ROOT/.clang-tidy" "$C/.clang-tidy"
    # One defect per family the config claims to enable; if a config edit
    # quietly switches a family off, its line here stops firing.
    cat > "$C/src/canary.c" <<'EOF'
#include <stdlib.h>
#include <string.h>
int canary_sizeof(const char *s);
int canary_atoi(const char *s);
int canary_leak(void);
int canary_sizeof(const char *s) { return (int)sizeof(sizeof(s)); }
int canary_atoi(const char *s) { return atoi(s); }
int canary_leak(void) { char *p = malloc(8); if (!p) return 0; p[0] = 1; return p[0]; }
static int canary_unused_static;
EOF
    : > "$C/empty-baseline"
    expect 1 "the canary trips the gate" "$PY" "$HERE/tidy_gate.py" --root "$C" --tidy "$CLANG_TIDY" \
        --baseline "$C/empty-baseline" --flags=-std=c11
    for chk in bugprone-sizeof-expression cert-err34-c clang-analyzer-unix.Malloc; do
        case $LAST_OUT in *"$chk"*) ok "$chk fires";; *) bad "$chk did not fire";; esac
    done
elif [ "${TIDY_REQUIRED:-0}" = 1 ]; then
    bad "clang-tidy ($CLANG_TIDY) not found and TIDY_REQUIRED=1"
else
    echo "  skip clang-tidy canary ($CLANG_TIDY not found)"
fi

echo "hygiene self-tests: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
