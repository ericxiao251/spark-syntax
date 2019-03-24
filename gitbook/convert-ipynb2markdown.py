import glob
import os
import os.path as op
import sys
from shutil import copyfile

try:
    assert sys.version_info.major == 3
    assert sys.version_info.minor > 5
except AssertionError:
    raise RuntimeError('converter requires Python 3.6+!')

basedir = op.abspath(op.dirname(__file__))

markdown_dir = op.join(basedir, 'src')
ipynb_dir = op.abspath(op.join(basedir, os.pardir, 'src'))

os.system(f'rm -rf {markdown_dir}/*/')  # delete chapter folders only

# convert ipynb to md
files_ipynb = glob.glob(f'{ipynb_dir}/**/*.ipynb', recursive=True)
for file_ipynb in files_ipynb:
    file_ipynb = op.abspath(file_ipynb)
    if 'Random' in file_ipynb:
        continue

    file_md = file_ipynb.replace('src', 'gitbook/src') \
        .replace('.ipynb', '.md') \
        .replace('(', '<').replace(')', '>').replace('?', '')
    os.makedirs(op.dirname(file_md), exist_ok=True)
    cmd = f'jupyter nbconvert --to markdown "{file_ipynb}" --output "{file_md}"'
    os.system(cmd)

# copy md to md
files_md = glob.glob(f'{ipynb_dir}/**/*.md', recursive=True)
for file_md in files_md:
    file_md = op.abspath(file_md)
    cp_file_md = file_md.replace('src', 'gitbook/src')
    os.makedirs(op.dirname(cp_file_md), exist_ok=True)
    copyfile(file_md, cp_file_md)

style = """\
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
"""

# cleanup
files = glob.glob(f'{markdown_dir}/**/*.md', recursive=True)
for file in files:
    with open(file, 'r') as f:
        content = f.read()
    content_new = content.replace(style, '')
    with open(file, 'w') as f:
        f.write(content_new)
