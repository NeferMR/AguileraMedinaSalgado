import sys
from compresorp import compress

path = sys.argv[1]
destino = 'comprimidop.elmejorprofesor'
compressTask = compress(path, destino)