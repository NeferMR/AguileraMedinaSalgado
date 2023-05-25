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

  def get_padded_encoded_text(self, byte_array):
        padded_info = byte_array[0]
        return ''.join(format(byte, '08b') for byte in byte_array[1:])[:-padded_info]

  def get_codes_from_encoded_text(self):
        current_code = ""
        for bit in self.padded_encoded_text:
            current_code += bit
            if current_code in self.reverse_mapping:
                character = self.reverse_mapping[current_code]
                self.codes[current_code] = character
                current_code = ""

  def decode_text(self):
        current_code = ""
        decoded_text = bytearray()
        for bit in self.padded_encoded_text:
            current_code += bit
            if current_code in self.codes:
                character = self.codes[current_code]
                decoded_text.append(character)
                current_code = ""
        return decoded_text


def decompress (input, output):
    start_time = time.time()

    #obtenemos los datos del MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        # Proceso maestro
        huffman = HuffmanCoding(None)  # Inicializar una instancia vacía de HuffmanCoding
        compressed_files = []

        #Se abre el archivo original
        with open(input, 'rb') as file:
            byte_array = list(file.read())

        #se obtienen los limites de las particiones del archivo
        chunk_size = len(byte_array) // size
        extra_size = len(byte_array) % size

        # Envío de datos a los procesos
        for i in range(1, size):
            #se crean los limites inicio y fin del archivo a trabajar
            start = i * chunk_size
            end = start + chunk_size

            #si es el ultimo se agrega lo que sobró
            if i == size - 1:
                end += extra_size

            #se crea el texto particionado que le toca a cada proceso
            data = byte_array[start:end]

            #se envia el texto particionado
            comm.send(data, dest=i)

        # Procesamiento del primer fragmento por el proceso maestro
        start = 0
        end = start + chunk_size
        if size == 1:
            end += extra_size
        data = byte_array[start:end]


        #Se hace la decodificacion del archivo parcial
        huffman.padded_encoded_text = huffman.get_padded_encoded_text(data)
        huffman.get_codes_from_encoded_text()
        decoded_text = huffman.decode_text()

        #se añade el resultado del trabajo parcial
        compressed_files.append(decoded_text)

        # Recepción y unión de los resultados de los procesos
        for i in range(1, size):
            compressed_files.append(comm.recv(source=i))

        # Creación del archivo descomprimido
        with open(output, 'wb') as output_file:
            for data in compressed_files:
                output_file.write(data)

        print(time.time() - start_time)

    else:
        # Procesos trabajadores
        data = comm.recv(source=0)
        huffman = HuffmanCoding(None)  # Inicializar una instancia vacía de HuffmanCoding

        #Se hace la decodificacion del archivo parcial
        huffman.padded_encoded_text = huffman.get_padded_encoded_text(data)
        huffman.get_codes_from_encoded_text()
        decoded_text = huffman.decode_text()

        #Se envia el resultado del proceso parcial
        comm.send(decoded_text, dest=0)
