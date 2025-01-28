
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.data.length < 4096:
            return True
        return False
        

    def write(self, value):
        self.num_records += 1
        # append values until page is full
        if self.has_capacity() is True:
            self.data.append(value)
        else:
            print("error: page is full")
        return

