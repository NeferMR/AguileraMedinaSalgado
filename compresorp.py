import heapq
import os
import sys
from mpi4py import MPI
import time

class HuffmanCoding:

  def __init__(self, path):
    self.path = path
    self.heap = []
    self.codes = {}
    self.reverse_mapping = {}

  class HeapNode:

    def __init__(self, char, freq):
      self.char = char
      self.freq = freq
      self.left = None
      self.right = None

    def __lt__(self, other):
      return self.freq < other.freq

    def __eq__(self, other):
      if (other == None):
        return False
      if (not isinstance(other, HeapNode)):
        return False
      return self.freq == other.freq

  def make_frequency_dict(self, text):
    frequency = {}
    for character in text:
      if not character in frequency:
        frequency[character] = 0
      frequency[character] += 1
    return frequency

  def make_heap(self, frequency):
    for key in frequency:
      node = self.HeapNode(key, frequency[key])
      heapq.heappush(self.heap, node)

  def merge_nodes(self):
    while (len(self.heap) > 1):
      node1 = heapq.heappop(self.heap)
      node2 = heapq.heappop(self.heap)

      merged = self.HeapNode(None, node1.freq + node2.freq)
      merged.left = node1
      merged.right = node2

      heapq.heappush(self.heap, merged)

  def make_codes_helper(self, root, current_code):
    if (root == None):
      return
    if (root.char != None):
      self.codes[root.char] = current_code
      self.reverse_mapping[current_code] = root.char
      return
    self.make_codes_helper(root.left, current_code + "0")
    self.make_codes_helper(root.right, current_code + "1")

  def make_codes(self):
    root = heapq.heappop(self.heap)
    current_code = ""
    self.make_codes_helper(root, current_code)

  def get_encoded_text(self, text):
    encoded_text = ""
    for character in text:
      encoded_text += self.codes[character]
    return encoded_text

  def pad_encoded_text(self, encoded_text):
    extra_padding = 8 - len(encoded_text) % 8
    for i in range(extra_padding):
      encoded_text += "0"
    padded_info = "{0:08b}".format(extra_padding)
    encoded_text = padded_info + encoded_text
    return encoded_text

  def get_byte_array(self, padded_encoded_text):
    if (len(padded_encoded_text) % 8 != 0):
      print("Encoded text not padded properly")
      exit(0)
    b = bytearray()
    for i in range(0, len(padded_encoded_text), 8):
      byte = padded_encoded_text[i:i + 8]
      b.append(int(byte, 2))
    return b

def compress(input, output):
    start_time = time.time()

    #obtenemos los datos del mpi
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        # Proceso maestro
        compressed_files = []

        #se lee el archivo
        with open(input, 'rb') as file:
            text = file.read().rstrip()
            file_size = len(text)

        #se divide en partes iguales el archivo
        chunk_size = file_size // size
        extra_size = file_size % size

        # Envío de datos a los procesos
        for i in range(1, size):
            #se da un inicio y un fin a leer el archivo
            start = i * chunk_size
            end = start + chunk_size

            #al ultimo se le envia adicional lo que sobró
            if i == size - 1:
                end += extra_size

            #se particiona el texto
            data = text[start:end]

            #se envia el texto particionado
            comm.send(data, dest=i)

        # Procesamiento del primer fragmento por el proceso maestro
        start = 0
        end = start + chunk_size
        if size == 1:
            end += extra_size
        data = text[start:end]

        #Se hace el proceso de compresion de huffman mediante creacion de arboles
        frequency = huffman.make_frequency_dict(data)
        huffman.make_heap(frequency)
        huffman.merge_nodes()
        huffman.make_codes()

        #se obtienen los datos codificados
        encoded_text = huffman.get_encoded_text(data)
        padded_encoded_text = huffman.pad_encoded_text(encoded_text)
        byte_array = huffman.get_byte_array(padded_encoded_text)
        
        #Se agrega a la variable el trabajo parcial
        compressed_files.append(bytes(byte_array))

        # Recepción y unión de los resultados de los procesos
        for i in range(1, size):
            compressed_files.append(comm.recv(source=i))

        # Creación del archivo comprimido
        with open(output, 'wb') as output:
            for data in compressed_files:
                output.write(data)

        print(time.time() - start_time)

    else:
        # Procesos trabajadores
        data = comm.recv(source=0)
        huffman = HuffmanCoding(None)  # Inicializar una instancia vacía de HuffmanCoding

        #Se hace el proceso de compresion de huffman mediante creacion de arboles
        frequency = huffman.make_frequency_dict(data)
        huffman.make_heap(frequency)
        huffman.merge_nodes()
        huffman.make_codes()

        #se obtienen los datos codificados
        encoded_text = huffman.get_encoded_text(data)
        padded_encoded_text = huffman.pad_encoded_text(encoded_text)
        byte_array = huffman.get_byte_array(padded_encoded_text)

        #Se envia el trabajo parcial hecho al rank 0
        comm.send(bytes(byte_array), dest=0)
