# MCP Schema offline dependency recipe

This is a private adapter dependency recipe, not a protocol/Schema algorithm change.
Supported package lock: native Linux/x86_64 (Ubuntu `amd64`), jsoncons 1.10.0
commit `9e57e421285422d7aeeb5aa7dc839f6a19121ee6` plus the exact three-header
dynamic-scope overlay, ICU upstream 78.2 / Ubuntu `78.2-2ubuntu1`.

## Obtain the locked inputs separately

`lock.json` records the exact archive/package names, SHA256 values, package
metadata, three header preimages/postimages, full jsoncons include-tree and
ICU Unicode-header-tree fingerprints, and required ICU file hashes.
The jsoncons upstream repository is https://github.com/danielaparker/jsoncons;
the ICU package metadata gives https://icu.unicode.org/ as its homepage.
These are upstream references, not asserted download URLs for the locked bytes.
Obtain the fixed archive and both `.deb` files separately and provide their
explicit local paths. No downloader, package installation, host fallback or
network access is performed. The archive hash identifies the approved local
bytes; this recipe does not claim a fresh upstream provenance audit.

Required local tools: Python 3.11.8+ (standard library only), `git`, `dpkg-deb`;
CMake and the project's existing C++ toolchain are needed to build the adapter.
The dev package declares additional distribution dependencies for its installed
tools. This recipe does not install or execute those tools; it extracts only for
header/library consumption. Host libc/C++ runtime remain toolchain prerequisites,
not bundled or isolated runtime dependencies.

## Prepare a new private root

Use an absolute output path whose parent already exists. It must not already
exist, even if empty. Do not point it at an existing dependency tree.

```sh
python3 cmake/mcp-schema/deps.py prepare \
  --jsoncons-archive /absolute/packages/jsoncons-9e57e421.tar.gz \
  --icu-dev /absolute/packages/libicu-dev_78.2-2ubuntu1_amd64.deb \
  --icu-runtime /absolute/packages/libicu78_78.2-2ubuntu1_amd64.deb \
  --output /absolute/private/mcp-schema-deps
```

All input hashes and `.deb` Package/Version/Architecture fields are checked
before creating the output. Then the fixed top-level archive layout and full
header preimage are checked; `git apply --check` / `git apply` replay the relative
patch with no fuzz/skip options, and full postimage fingerprints are checked.
No caller source or input package is changed. Failure is nonzero; extraction
failure may leave a partial **new** output directory, which is never reused or
automatically deleted. Choose another new output after fixing the cause.

The output contains `jsoncons-<commit>/` and `icu/`. ICU package control files
are retained under `icu/.mcp-controls/` and checked with the binaries/headers;
copying only libraries into another root is not sufficient. `libicudata` is in
`libicu78`; no extra third data package is required.

## Check and build

```sh
JSONCONS_ROOT=/absolute/private/mcp-schema-deps/jsoncons-9e57e421285422d7aeeb5aa7dc839f6a19121ee6
ICU_ROOT=/absolute/private/mcp-schema-deps/icu
python3 cmake/mcp-schema/deps.py check \
  --jsoncons-root "$JSONCONS_ROOT" --icu-root "$ICU_ROOT"
cmake -S . -B build-schema \
  -DBBT_INFRA_MCP=ON -DBBT_INFRA_MCP_SCHEMA=ON -DBUILD_TESTING=ON \
  -DBBT_JSONCONS_PREFIX="$JSONCONS_ROOT" -DBBT_ICU_PREFIX="$ICU_ROOT" \
  -DBBT_COROUTINE_SOURCE_DIR=/absolute/existing/bbtools-coroutine
cmake --build build-schema --target Test_mcp_schema --parallel 2
```

Only schema-ON configuration requires Python/these roots. Schema-OFF retains
its existing dependency-free schema behavior. ON checks fail closed for wrong
pins, missing/modified overlay, wrong ICU version/architecture, escaped library
symlinks or missing resources. The checker clears both normal and cache ICU
`find_library` bindings before the existing `NO_DEFAULT_PATH` lookup, so changing
roots cannot retain old libraries. It validates at configure time, not continuously
while a build runs. No default-root discovery or user-root repair is performed.
The core does not link ICU; only the private schema target does. There is no new
CI, SDK test, generic platform abstraction or install/export contract here.

## Runtime isolation is explicit

Selecting a private link root does **not** ensure transitive runtime isolation:
ELF `DT_RUNPATH` need not propagate to `libicudata`. For every test/consumer
process (and any SDK parent launching it), set the complete environment explicitly:

```sh
LD_LIBRARY_PATH="$ICU_ROOT/usr/lib/x86_64-linux-gnu" \
  ./build-schema/tests/Test_mcp_schema
LD_LIBRARY_PATH="$ICU_ROOT/usr/lib/x86_64-linux-gnu" \
  ldd ./build-schema/tests/Test_mcp_schema
```

Confirm all three resolved `libicui18n.so.78`, `libicuuc.so.78` and
`libicudata.so.78` paths are beneath the selected private root. Do not infer this
from configure success or globally modify the system loader environment.
No RPATH propagation or public runtime semantics are changed by this recipe.

## Attribution and licenses

`dynamic-scope.patch` is the unified, relative-path representation of the
previously approved local overlay, not an upstream release patch. It modifies
only `eval_context.hpp`, `keyword_validator.hpp`, and
`schema_validator_factory_base.hpp` under `include/jsoncons_ext/jsonschema/common/`.
Patched jsoncons headers retain their original Daniel Parker copyright notices;
the jsoncons-derived patch/context is distributed under BSL-1.0, including local
derivative additions under the same terms. `LICENSE-jsoncons.txt` reproduces the
archive's original license text and copyright notice; retain it with the patch.
This does not relicense ICU or third-party source/binaries. ICU's supplied
copyright/license notices remain in the extracted
`usr/share/doc/libicu-dev/copyright` and `usr/share/doc/libicu78/copyright`; the
lock checks them. Consult these originals before redistributing ICU binaries.
No third-party source tree, `.deb`, binary, private local path or raw verification
log is part of the repository candidate.
