"""
Enforce build retention policy in WEB_ROOT:
  - always keep the 10 most recent builds
  - beyond that, keep the newest build per calendar month
  - delete everything else (full directory tree)
"""
import os
import re
import shutil
from collections import defaultdict

KEEP_RECENT = 10

web_root = '/home/web/fuzzwork/htdocs/dump'

entries = []
for name in os.listdir(web_root):
    full = os.path.join(web_root, name)
    if not os.path.isdir(full) or os.path.islink(full):
        continue
    m = re.match(r'^\d+_(\d{8})_(\d{6})$', name)
    if m:
        sort_key = m.group(1) + m.group(2)   # YYYYMMDDHHMMSS — sorts correctly
        entries.append((sort_key, name, full))

entries.sort(reverse=True)  # newest first

keep = set()

for _, name, _ in entries[:KEEP_RECENT]:
    keep.add(name)

monthly = defaultdict(list)
for sort_key, name, full in entries[KEEP_RECENT:]:
    month = sort_key[:6]  # YYYYMM
    monthly[month].append((sort_key, name))

for items in monthly.values():
    keep.add(items[0][1])  # items[0] is already the newest (list is sorted desc)

for _, name, full in entries:
    if name not in keep:
        assert os.path.dirname(os.path.realpath(full)) == os.path.realpath(web_root), \
            f"Safety check failed: {full!r} is not a direct child of {web_root!r}"
        assert re.match(r'^\d+_(\d{8})_(\d{6})$', name), \
            f"Safety check failed: {name!r} does not match build-directory pattern"
        print(f"Removing old build: {name}")
        shutil.rmtree(full)

print(f"Retention complete: {len(keep)} builds kept, "
      f"{sum(1 for _, n, _ in entries if n not in keep)} removed")
