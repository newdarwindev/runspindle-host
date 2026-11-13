#!/usr/bin/env python3
"""Behavioral mutations in disposable source copies; compile failures never count."""
import json
import pathlib
import shutil
import subprocess
import tempfile
ROOT=pathlib.Path(__file__).resolve().parents[1]
source=(ROOT/'src/lifecycle.rs').read_text()
mutations={
 'duplicate_dispatch': ('            return Ok(existing.clone());', '            let mut duplicate = existing.clone(); self.launch(&mut duplicate, now, peer)?; return self.put(duplicate);'),
 'session_rebinding': ('            .is_some_and(|old| old != &observation.native_session_id)', '            .is_some_and(|_old| false)'),
}
results=[]
for name,(old,new) in mutations.items():
    assert source.count(old)==1, name
    with tempfile.TemporaryDirectory(prefix='host-mutation-') as directory:
        root=pathlib.Path(directory)
        for path in ('Cargo.toml','Cargo.lock','src','tests'):
            src=ROOT/path;dest=root/path
            shutil.copytree(src,dest) if src.is_dir() else shutil.copy2(src,dest)
        (root/'src/lifecycle.rs').write_text(source.replace(old,new))
        run=subprocess.run(['cargo','test','--locked','--offline','--test','durable','--target-dir',str(ROOT/'target/mutations')],cwd=root,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=180)
        assert run.returncode!=0 and 'test result: FAILED' in run.stdout and 'could not compile' not in run.stdout, run.stdout
        results.append({'mutation':name,'behavioral_failure_observed':True})
print(json.dumps({'evidence_class':'component_mutation','results':results}))
