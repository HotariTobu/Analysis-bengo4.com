import sys
from pathlib import Path
from subprocess import run, PIPE

input_path = Path(input('input path: '))
output_path = Path(input('output path: '))

if input_path.is_dir():
    input_files = input_path.iterdir()
else:
    input_files = [input_path]

if not output_path.is_dir():
    if input('Make output dirs? (Y/n): ') == 'Y':
        output_path.mkdir(parents=True)
    else:
        sys.exit()
        
for path in input_files:
    if path.suffix != '.svg':
        continue

    if not path.is_file():
        continue

    input_file = str(path)
    output_file = str(output_path / path.with_suffix('.png').name)

    args = [
        'magick',
        'convert',
        '-density',
        '160',
        input_file,
        output_file,
    ]

    run(args, stdout=PIPE, stderr=PIPE)