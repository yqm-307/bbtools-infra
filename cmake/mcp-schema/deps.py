#!/usr/bin/env python3
"""Prepare or check the locked Linux/amd64 MCP schema dependencies, offline."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import re
import subprocess
import sys
import tarfile

ASSETS = Path(__file__).resolve().parent
LOCK = json.loads((ASSETS / 'lock.json').read_text())


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def fingerprint(root):
    require(root.is_dir(), 'missing directory: ' + str(root))
    digest = hashlib.sha256()
    for path in sorted(root.rglob('*')):
        require(not path.is_symlink(), 'unexpected header symlink: ' + str(path))
        if path.is_file():
            digest.update((path.relative_to(root).as_posix() + '\0' + sha(path) + '\n').encode())
    return digest.hexdigest()


def hashes(root, expected):
    for relative, expected_sha in expected.items():
        path = root / relative
        require(path.resolve().is_relative_to(root.resolve()), 'path escapes root: ' + relative)
        require(path.is_file() and sha(path) == expected_sha, 'SHA256 mismatch: ' + relative)


def check_platform():
    require(platform.system() == LOCK['platform']['system'] and
            platform.machine() == LOCK['platform']['machine'],
            'only the locked Linux/x86_64 (amd64) platform is supported')


def check(jsoncons, icu):
    check_platform()
    require(jsoncons.is_absolute() and icu.is_absolute(), 'dependency roots must be absolute')
    pinned = LOCK['jsoncons']
    hashes(jsoncons, pinned['headers_after'])
    require(fingerprint(jsoncons / 'include') == pinned['include_after_sha256'],
            'jsoncons include tree does not match pinned archive plus overlay')
    version = (icu / 'usr/include/unicode/uvernum.h').read_text()
    require(re.search(r'^#define U_ICU_VERSION "78\.2"$', version, re.M), 'ICU version must be 78.2')
    for package in LOCK['icu']['packages']:
        control = (icu / '.mcp-controls' / package['Package'] / 'control').read_text()
        for field in ('Package', 'Version', 'Architecture'):
            require(re.search(r'^' + field + ': ' + re.escape(package[field]) + '$', control, re.M),
                    'ICU package ' + field + ' mismatch')
    hashes(icu, LOCK['icu']['controls'])
    hashes(icu, LOCK['icu']['resources'])
    require(fingerprint(icu / 'usr/include/unicode') == LOCK['icu']['unicode_tree_sha256'],
            'ICU header tree mismatch')
    for name in LOCK['icu']['libraries']:
        library = icu / LOCK['icu']['libdir'] / ('lib' + name + '.so')
        require(library.resolve() == (library.parent / ('lib' + name + '.so.78.2')).resolve()
                and library.resolve().is_relative_to(icu.resolve()), 'ICU library link escapes locked root: ' + name)


def prepare(args):
    check_platform()
    pinned = LOCK['jsoncons']
    archive = args.jsoncons_archive.resolve(strict=True)
    dev = args.icu_dev.resolve(strict=True)
    runtime = args.icu_runtime.resolve(strict=True)
    require(sha(archive) == pinned['archive_sha256'], 'jsoncons archive SHA256 mismatch')
    patch = ASSETS / LOCK['overlay']['file']
    require(sha(patch) == LOCK['overlay']['sha256'], 'overlay SHA256 mismatch')
    for path, package in zip((dev, runtime), LOCK['icu']['packages']):
        require(sha(path) == package['sha256'], 'ICU package SHA256 mismatch: ' + package['Package'])
        for field in ('Package', 'Version', 'Architecture'):
            actual = subprocess.check_output(['dpkg-deb', '-f', str(path), field], text=True).strip()
            require(actual == package[field], 'ICU package ' + field + ' mismatch')
    output = args.output
    require(output.is_absolute(), 'output must be an absolute new private directory')
    require(not output.exists() and not output.is_symlink(), 'output already exists; never reuse or overwrite')
    # ponytail: Linux/amd64 debs only; approve a new lock before supporting another platform.
    output.mkdir(parents=False)
    with tarfile.open(archive) as tar:
        require(all(member.name == pinned['directory'] or
                    member.name.startswith(pinned['directory'] + '/') for member in tar.getmembers()),
                'unexpected archive top-level layout')
        tar.extractall(output, filter='data')
    jsoncons = output / pinned['directory']
    hashes(jsoncons, pinned['headers_before'])
    require(fingerprint(jsoncons / 'include') == pinned['include_before_sha256'], 'jsoncons preimage tree mismatch')
    subprocess.run(['git', 'apply', '--check', str(patch)], cwd=jsoncons, check=True)
    subprocess.run(['git', 'apply', str(patch)], cwd=jsoncons, check=True)
    icu = output / 'icu'
    icu.mkdir()
    for path, package in zip((dev, runtime), LOCK['icu']['packages']):
        subprocess.run(['dpkg-deb', '-x', str(path), str(icu)], check=True)
        control = icu / '.mcp-controls' / package['Package']
        control.parent.mkdir(exist_ok=True)
        subprocess.run(['dpkg-deb', '-e', str(path), str(control)], check=True)
    check(jsoncons, icu)
    print(json.dumps({'status': 'SUCCESS', 'MCP_JSONCONS_ROOT': str(jsoncons),
                      'MCP_ICU_ROOT': str(icu), 'runtime_environment':
                      'LD_LIBRARY_PATH=' + str(icu / LOCK['icu']['libdir'])}, indent=2))


def main():
    require(sys.version_info >= (3, 11, 8), 'Python 3.11.8+ is required')
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    prep = commands.add_parser('prepare')
    for name in ('jsoncons-archive', 'icu-dev', 'icu-runtime', 'output'):
        prep.add_argument('--' + name, type=Path, required=True)
    verify = commands.add_parser('check')
    verify.add_argument('--jsoncons-root', type=Path, required=True)
    verify.add_argument('--icu-root', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare(args)
    else:
        check(args.jsoncons_root, args.icu_root)
        print('SUCCESS: pinned jsoncons overlay and ICU roots verified')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, tarfile.TarError, subprocess.CalledProcessError) as error:
        print('ERROR: ' + str(error), file=sys.stderr)
        sys.exit(1)
