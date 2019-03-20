# -*- coding: utf-8 -*-
# Author Frank Hu
# GitBook auto summary
# summary all .md files in a GitBook folder

import os
import re


def atoi(text):
    return int(text) if text.isdigit() else text


def output_markdown(dire, base_dir, output_file, append, iter_depth=0):
    """Main iterator for get information from every file/folder

    i: directory, base directory(to calulate relative path), 
       output file name, iter depth.
    p: Judge is directory or is file, then process .md/.markdown files.
    o: write .md information (with identation) to output_file.
    """

    dir_list = os.listdir(dire)

    # sort by numbers in directories
    dir_list.sort(key=lambda x: [atoi(c) for c in re.split('(\d+)', x)])

    for filename in sort_dir_file(dir_list, base_dir):
        # add list and sort
        print('Processing ', filename)  # output log
        file_or_path = os.path.join(dire, filename)
        if os.path.isdir(file_or_path):  # is dir
            if mdfile_in_dir(file_or_path):
                # if there is .md files in the folder, output folder name
                output_file.write('  ' * iter_depth + '- ' + filename + '\n')
                output_markdown(file_or_path, base_dir, output_file, append,
                                iter_depth + 1)  # iteration
        else:  # is file
            if is_markdown_file(filename):
                # re to find target markdown files, $ for matching end of filename
                if (filename not in ['SUMMARY.md',
                                     'SUMMARY-GitBook-auto-summary.md']
                        or iter_depth != 0):  # escape SUMMARY.md at base directory
                    output_file.write('  ' * iter_depth +
                                      '- [{}]({})\n'.format(write_md_filename(filename,
                                                                              append),
                                                            os.path.join(os.path.relpath(dire, base_dir),
                                                                         filename)))
                    # iter depth for indent, relpath and join to write link.


def mdfile_in_dir(dire):
    """Judge if there is .md file in the directory

    i: input directory
    o: return Ture if there is .md file; False if not.
    """
    for root, dirs, files in os.walk(dire):
        for filename in files:
            if re.search('.md$|.markdown$', filename):
                return True
    return False


def is_markdown_file(filename):
    """ Judge if the filename is a markdown filename

    i: filename
    o: filename without '.md' or '.markdown'
    """
    match = re.search('.md$|.markdown$', filename)
    if not match:
        return False
    elif len(match.group()) is len('.md'):
        return filename[:-3]
    elif len(match.group()) is len('.markdown'):
        return filename[:-9]


def sort_dir_file(listdir, dire):
    # sort dirs and files, first files a-z, then dirs a-z
    list_of_file = []
    list_of_dir = []
    for filename in listdir:
        if os.path.isdir(os.path.join(dire, filename)):
            list_of_dir.append(filename)
        else:
            list_of_file.append(filename)
    for dire in list_of_dir:
        list_of_file.append(dire)
    return list_of_file


def write_md_filename(filename, append):
    """ write markdown filename

    i: filename and append
    p: if append: find former list name and return
       else: write filename
    """
    if append:
        for line in former_summary_list:
            if re.search(filename, line):
                s = re.search('\[.*\]\(', line)
                return s.group()[1:-2]
        else:
            return is_markdown_file(filename)
    else:
        return is_markdown_file(filename)


def main():
    overwrite = True
    append = False
    dir_input = 'src'

    # print information
    print('GitBook auto summary:', dir_input, end=' ')

    if append and os.path.exists(os.path.join(dir_input, 'SUMMARY.md')):
        # append: read former SUMMARY.md
        print('--append', end=' ')
        global former_summary_list
        with open(os.path.join(dir_input, 'SUMMARY.md')) as f:
            former_summary_list = f.readlines()
            f.close()
    print()
    # output to flie
    if (overwrite == False and
            os.path.exists(os.path.join(dir_input, 'SUMMARY.md'))):
        # overwrite logic
        filename = 'SUMMARY-GitBook-auto-summary.md'
    else:
        filename = 'SUMMARY.md'
    output = open(os.path.join(dir_input, filename), 'w')
    output.write('# Summary\n\n')
    output_markdown(dir_input, dir_input, output, append)

    print('GitBook auto summary finished:) ')
    return 0


if __name__ == '__main__':
    main()
