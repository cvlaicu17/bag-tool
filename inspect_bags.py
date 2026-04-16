from rosbags.rosbag2 import Reader
from pathlib import Path

for p in [
    Path('/tmp/current_result/current_result_0.mcap_aligned'),
    Path.home() / 'data/bea276eb8b0c91f69e638c5994519cba/Day3.3_0.mcap',
]:
    try:
        with Reader(p) as r:
            print('===', p.name)
            for c in r.connections:
                d = c.msgdef.data if isinstance(c.msgdef.data, str) else c.msgdef.data.decode()
                print(' ', c.topic, c.msgdef.format.name, repr(c.digest))
                print('  first 300:', repr(d[:300]))
    except Exception as e:
        print('ERROR', p, e)
