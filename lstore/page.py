
# PHYSICAL PAGE CLASS
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records < 4096:
            return True
        return False
        

    def write(self, value):
        # append values until page is full
        if self.has_capacity() is True:
            self.data[self.num_records] = value
            self.num_records += 1
        else:
            print("error: page is full")  

    def read(self, index):
        if index>=4096:
            print("error: invalid index")  
            return
        return self.data[index]

# BASE AND TAIL PAGE CLASS
class PageGroup(Page):
    pass
    # contains an array of Pages

# PAGE RANGE CLASS
class pageRange(Page):
    pass
    # contains an array of PageGroup (base pages)
    # contains an array of PageGroup (tail pages)
