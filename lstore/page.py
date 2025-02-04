ARRAY_SIZE = 4096
VALUE_SIZE = 8
PAGE_RANGE_SIZE = 16

# PHYSICAL PAGE CLASS
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(ARRAY_SIZE)

    def has_capacity(self):
        if self.num_records < ARRAY_SIZE/VALUE_SIZE:
            return True
        return False

    # TODO: we need to keep track of offset (num_records * VALUE_SIZE)
    def write(self, value):
        # append values until page is full
        if self.has_capacity() is True:
            self.data[self.num_records] = value
            self.num_records += 1
        else:
            print("error: page is full")  

    def read(self, index):
        if index>=(ARRAY_SIZE/VALUE_SIZE):
            print("error: invalid index")  
            return
        return self.data[index]

# BASE AND TAIL PAGE CLASS
class PageGroup():
    def __init__(self):
        self.pages = []
        pass
    # contains an array of Pages

    def has_capacity(self):
        for page in self.pages:
            if page.has_capacity() is True:
                return True
        return False

    # TODO: need to write to all the pages in the group
    def write(self, *columns):
        for page in self.pages:
            if page.has_capacity() is True:
                page.write(columns)
                return
        print("error: no page has capacity")

# PAGE RANGE CLASS
class pageRange():
    # TODO: pageRange should only hold 16 base/tail pages total. How ar we going to implement this
    def __init__(self):
        self.base_pages = []
        self.tail_pages = []
        pass

    # contains an array of PageGroup (base pages)
    # contains an array of PageGroup (tail pages)
