from io import TextIOWrapper

class IterStream(TextIOWrapper):
    """
    File-like streaming iterator.
    """
    def __init__(self, generator):
        self.generator = generator
        self.iterator = iter(generator)
        self.leftover = ''

    def __iter__(self):
        return self.iterator

    def next(self):
        return self.iterator.__next__()

    def __next__(self):
        return self.iterator.__next__()

    def read(self, size):
        data = self.leftover
        count = len(self.leftover)
        try:
            while count < size:
                chunk = self.__next__()
                data += chunk
                count += len(chunk)
        except StopIteration:
            self.leftover = ''
            return data

        return data[:size]

    def readline(self, size):
        return self.read(size)