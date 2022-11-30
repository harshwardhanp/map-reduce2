# from https://stackoverflow.com/questions/55187374/cleaning-text-with-python-and-re

def filter_text(dirty_file):
    data_file = open(dirty_file, 'r', encoding='utf-8')
    dirty_text = data_file.read()

    killpunctuation = str.maketrans('', '', r"()\"“”’#/@;:<>{}[]*-=~|.?,0123456789")

    clean_text = dirty_text.translate(killpunctuation)

    clean_file_name = 'clean_'+dirty_file
    fp = open(clean_file_name, 'w+', encoding='utf-8')
    fp.write(clean_text.lower())
    fp.close()
    return clean_file_name


# input_file = filter_text('6527-0.txt')