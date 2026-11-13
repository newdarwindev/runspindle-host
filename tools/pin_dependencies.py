#!/usr/bin/env python3
"""Record exact cached crate, bundled SQLite, notice and toolchain bytes before tests."""
import hashlib
import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

metadata = json.loads(subprocess.check_output(['cargo','metadata','--locked','--offline','--format-version','1'],cwd=ROOT,text=True))
rows = []
notices = ROOT/'licenses/dependencies'
notices.mkdir(parents=True, exist_ok=True)
for package in metadata['packages']:
    if package['name'] == 'runspindle-host':
        continue
    base = pathlib.Path(package['manifest_path']).parent
    archives = list((pathlib.Path.home()/'.cargo/registry/cache').glob('*/'+package['name']+'-'+package['version']+'.crate'))
    assert len(archives) == 1
    checksum = sha(archives[0])
    assert 'checksum = \"'+checksum+'\"' in (ROOT/'Cargo.lock').read_text()
    license_files = {}
    for p in base.rglob('*'):
        if p.is_file() and p.name.upper().startswith(('LICENSE','NOTICE','COPYING')):
            dest = notices/(package['name']+'-'+package['version']+'-'+str(p.relative_to(base)).replace('/','_'))
            dest.write_bytes(p.read_bytes())
            license_files[str(dest.relative_to(ROOT))] = sha(dest)
    row = {'name':package['name'],'version':package['version'],'license':package['license'],'crate_sha256':checksum,'notices':license_files}
    if package['name'] == 'libsqlite3-sys':
        row['bundled_sources'] = {str(p.relative_to(base)):sha(p) for p in sorted(base.glob('sqlite3/*')) if p.is_file()}
    rows.append(row)
result = {'schema_version':1,'cargo_lock_sha256':sha(ROOT/'Cargo.lock'),'dependencies':rows,
          'toolchain':{'rustc':subprocess.check_output(['rustc','--version'],text=True).strip(),'cargo':subprocess.check_output(['cargo','--version'],text=True).strip()},
          'qualification':'cached pinned dependencies; tests offline; native protocol and live acceptance blocked'}
(ROOT/'docs/durable-dependencies.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
