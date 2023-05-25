import sys

with open(sys.argv[1], 'rb') as file1:
    data1 = file1.read()

with open(sys.argv[2], 'rb') as file2:
    data2 = file2.read()

print('ok') if data1 == data2 else print('nok')