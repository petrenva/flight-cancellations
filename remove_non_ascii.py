import re
import sys

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: {} filename".format(sys.argv[0]))
        sys.exit(1)
    fname = sys.argv[1]
    print("Reading {}".format(fname))
    text = open(fname, 'rb').read()
    text = str(text)
    text = re.sub(r'\\x[0-9a-z]{2}','', text)[2:-1].replace('\\n', '\n')
    f = open('test.csv', 'w')
    f.write(text)
    f.close()
