import os


def line_prepender(filename, line):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + '=' * len(line) + '\n\n' + content)


for rst_file in os.scandir('source'):
    if rst_file.is_file() and 'index.rst' not in rst_file.name and 'conf.py' not in rst_file.name:
        with open(rst_file, 'rt') as f:
            found = False
            for line in f:
                if '====' in line:
                    found = True
            if not found:
                line_prepender(rst_file, rst_file.name)
