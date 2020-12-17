import os
import constants

def strip_trailing_whitespace_from_files():
    """ Remove trailing whitespace from all files.
    """
    for path, dirs, files in os.walk(constants.IB_PATH):
        for f in files:
            file_name, file_extension = os.path.splitext(f)
            if file_extension == '.py':
                path_name = os.path.join(path, f)
                with open(path_name, 'r') as fh:
                    new = [line.rstrip() for line in fh]
                with open(path_name, 'w') as fh:
                    [fh.write('%s\n' % line) for line in new]
